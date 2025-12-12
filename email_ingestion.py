import hashlib
import imaplib
import json
import logging
import os
import shutil
import sqlite3
import time
from dataclasses import dataclass
from email import message_from_bytes
from email.message import Message
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import shlex
import subprocess

import pdf2image
import pytesseract
import qrcode
import requests
from PIL import Image
from pypdf import PdfReader, PdfWriter, Transformation
from pyzbar.pyzbar import decode as decode_qr


DEFAULT_WORKER_ENDPOINT = os.environ.get("VLSC_WORKER_ENDPOINT", "https://example.workers.dev")
BASE_DATA_DIR = Path(os.environ.get("VLSC_DATA_DIR", Path(__file__).parent / "data"))
INTAKE_QUEUE = BASE_DATA_DIR / "intake_queue"
STAMPED_QUEUE = BASE_DATA_DIR / "stamped"
PROCESSED_DIRS = {
    "AP": BASE_DATA_DIR / "processed" / "ap",
    "AR": BASE_DATA_DIR / "processed" / "ar",
    "CLIENT": BASE_DATA_DIR / "processed" / "client",
    "ADMIN": BASE_DATA_DIR / "processed" / "admin",
    "ARCHIVE": BASE_DATA_DIR / "processed" / "archive",
}
DB_PATH = BASE_DATA_DIR / "document_routes.db"


@dataclass
class EmailIngestionConfig:
    imap_host: str
    username: str
    password: str
    mailbox: str = "INBOX"
    allowed_extensions: Tuple[str, ...] = (".pdf", ".png", ".jpg", ".jpeg")
    endpoint: str = DEFAULT_WORKER_ENDPOINT
    # Optional command to invoke CaptureOnTouch (or other vendor OCR). The command
    # should accept the image/pdf path as the last argument and emit plain text
    # to stdout. Example: "C:\\Program Files\\Canon\\CaptureOnTouch\\cotocr.exe --ocr"
    capture_cmd: Optional[str] = None
    max_retries: int = 3
    backoff_seconds: int = 3


@dataclass
class AttachmentRecord:
    source_email: str
    subject: str
    filename: str
    local_path: Path


@dataclass
class WorkerResponse:
    final_filename: str
    route: str
    naming_convention: Optional[str]
    metadata: Dict


class DatabaseClient:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS document_routes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    original_filename TEXT NOT NULL,
                    final_filename TEXT NOT NULL,
                    route TEXT NOT NULL,
                    location TEXT NOT NULL,
                    metadata_hash TEXT,
                    qr_payload TEXT,
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

    def record(self, *, original_filename: str, final_filename: str, route: str, location: str, metadata_hash: str, qr_payload: str) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                INSERT INTO document_routes (
                    original_filename, final_filename, route, location, metadata_hash, qr_payload
                ) VALUES (?, ?, ?, ?, ?, ?);
                """,
                (
                    original_filename,
                    final_filename,
                    route,
                    location,
                    metadata_hash,
                    qr_payload,
                ),
            )


class EmailIngestor:
    def __init__(self, config: EmailIngestionConfig):
        self.config = config
        INTAKE_QUEUE.mkdir(parents=True, exist_ok=True)

    def fetch_attachments(self) -> List[AttachmentRecord]:
        logging.info("Connecting to IMAP server for %s", self.config.username)
        attachments: List[AttachmentRecord] = []
        with imaplib.IMAP4_SSL(self.config.imap_host) as mail:
            mail.login(self.config.username, self.config.password)
            mail.select(self.config.mailbox)
            status, data = mail.search(None, "UNSEEN")
            if status != "OK":
                logging.warning("No messages retrieved from mailbox")
                return attachments

            for msg_id in data[0].split():
                status, msg_data = mail.fetch(msg_id, "(RFC822)")
                if status != "OK":
                    continue
                message = message_from_bytes(msg_data[0][1])
                attachments.extend(self._save_attachments_from_message(message))
                mail.store(msg_id, "+FLAGS", "(\\Seen)")
        return attachments

    def _save_attachments_from_message(self, message: Message) -> List[AttachmentRecord]:
        saved: List[AttachmentRecord] = []
        subject = message.get("Subject", "")
        from_email = message.get("From", "")
        for part in message.walk():
            if part.get_content_disposition() != "attachment":
                continue
            filename = part.get_filename()
            if not filename:
                continue
            ext = Path(filename).suffix.lower()
            if ext not in self.config.allowed_extensions:
                logging.info("Skipping attachment %s due to extension", filename)
                continue
            payload = part.get_payload(decode=True)
            if not payload:
                continue
            safe_name = f"email_{int(time.time())}_{filename}"
            dest_path = INTAKE_QUEUE / safe_name
            with open(dest_path, "wb") as f:
                f.write(payload)
            saved.append(
                AttachmentRecord(
                    source_email=from_email, subject=subject, filename=filename, local_path=dest_path
                )
            )
            logging.info("Saved attachment %s to intake queue", dest_path)
        return saved


def extract_text(path: Path, capture_cmd: Optional[str] = None) -> str:
    # Try vendor-supplied OCR command first (CaptureOnTouch CLI or similar).
    if capture_cmd:
        try:
            cmd_parts = shlex.split(capture_cmd) + [str(path)]
            proc = subprocess.run(cmd_parts, capture_output=True, text=True, timeout=60)
            if proc.returncode == 0 and proc.stdout.strip():
                return proc.stdout
            logging.warning("Capture OCR command failed (%s): %s", proc.returncode, proc.stderr)
        except Exception as exc:
            logging.warning("Capture OCR command exception for %s: %s", path, exc)

    # Fall back to pytesseract for OCR if vendor command not provided or failed.
    try:
        if path.suffix.lower() == ".pdf":
            images = pdf2image.convert_from_path(path, dpi=200, first_page=1, last_page=1)
            if images:
                return pytesseract.image_to_string(images[0])
        else:
            with Image.open(path) as img:
                return pytesseract.image_to_string(img)
    except Exception as exc:
        logging.warning("OCR extraction failed for %s: %s", path, exc)
    return ""


def build_metadata_hash(text: str, path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(text.encode("utf-8", errors="ignore"))
    with open(path, "rb") as f:
        digest.update(f.read())
    return digest.hexdigest()


def determine_route_hint(text: str) -> str:
    lower = text.lower()
    if any(k in lower for k in ["invoice", "bill", "payable"]):
        return "AP"
    if any(k in lower for k in ["receipt", "payment", "credit"]):
        return "AR"
    if any(k in lower for k in ["contract", "client", "engagement"]):
        return "CLIENT"
    if any(k in lower for k in ["policy", "admin", "hr"]):
        return "ADMIN"
    return "ARCHIVE"


def _qr_payload(doc_type: str, metadata_hash: str, route_hint: str) -> str:
    payload = {"doc_type": doc_type, "metadata_hash": metadata_hash, "route_hint": route_hint}
    return json.dumps(payload, sort_keys=True)


def _qr_image(data: str) -> Image.Image:
    qr = qrcode.QRCode(version=1, box_size=6, border=2)
    qr.add_data(data)
    qr.make(fit=True)
    return qr.make_image(fill_color="black", back_color="white")


def _stamp_pdf(source: Path, qr_img: Image.Image, output: Path) -> None:
    qr_pdf_path = output.parent / f"{output.stem}_qr.pdf"
    qr_img.save(qr_pdf_path, "PDF")
    qr_reader = PdfReader(str(qr_pdf_path))
    qr_page = qr_reader.pages[0]
    reader = PdfReader(str(source))
    writer = PdfWriter()
    for i, page in enumerate(reader.pages):
        if i == 0:
            page_width = float(page.mediabox.width)
            qr_width = float(qr_page.mediabox.width)
            transform = Transformation().translate(tx=page_width - qr_width - 36, ty=36)
            page.merge_transformed_page(qr_page, transform)
        writer.add_page(page)
    with open(output, "wb") as f:
        writer.write(f)
    qr_pdf_path.unlink(missing_ok=True)


def _stamp_image(source: Path, qr_img: Image.Image, output: Path) -> None:
    with Image.open(source) as img:
        img = img.convert("RGB")
        qr_resized = qr_img.resize((int(img.width * 0.2), int(img.width * 0.2)))
        position = (img.width - qr_resized.width - 10, img.height - qr_resized.height - 10)
        img.paste(qr_resized, position)
        img.save(output)


def stamp_with_qr(path: Path, payload: str) -> Path:
    stamped_dir = STAMPED_QUEUE
    stamped_dir.mkdir(parents=True, exist_ok=True)
    qr_img = _qr_image(payload)
    output = stamped_dir / f"{path.stem}_stamped{path.suffix}"
    if path.suffix.lower() == ".pdf":
        _stamp_pdf(path, qr_img, output)
    else:
        _stamp_image(path, qr_img, output)
    logging.info("Stamped QR onto %s", output)
    return output


def send_to_worker(stamped_path: Path, metadata: Dict, endpoint: str, *, max_retries: int = 3, backoff_seconds: int = 3) -> Dict:
    for attempt in range(1, max_retries + 1):
        try:
            with open(stamped_path, "rb") as f:
                files = {"file": (stamped_path.name, f, "application/octet-stream")}
                response = requests.post(endpoint, data={"metadata": json.dumps(metadata)}, files=files, timeout=30)
            if response.ok:
                logging.info("Worker accepted file %s", stamped_path)
                return response.json()
            logging.warning("Worker responded with %s on attempt %s", response.status_code, attempt)
        except Exception as exc:
            logging.warning("Error sending to worker on attempt %s: %s", attempt, exc)
        time.sleep(backoff_seconds * attempt)
    raise RuntimeError("Failed to send file to Cloudflare worker after retries")


def decode_qr_payload_from_path(path: Path) -> Optional[str]:
    try:
        if path.suffix.lower() == ".pdf":
            images = pdf2image.convert_from_path(path, dpi=200, first_page=1, last_page=1)
            if not images:
                return None
            codes = decode_qr(images[0])
        else:
            with Image.open(path) as img:
                codes = decode_qr(img)
        if codes:
            return codes[0].data.decode("utf-8")
    except Exception as exc:
        logging.warning("Failed to decode QR from %s: %s", path, exc)
    return None


def _apply_naming_convention(response: Dict, fallback_name: str) -> str:
    if "final_filename" in response and response["final_filename"]:
        return response["final_filename"]
    if "naming_convention" in response:
        try:
            template = response["naming_convention"]
            return template.format(**response.get("metadata", {}))
        except Exception as exc:
            logging.warning("Failed to apply naming convention: %s", exc)
    return fallback_name


def route_file(
    stamped_path: Path,
    response: Dict,
    payload: str,
    metadata_hash: str,
    db: DatabaseClient,
    original_name: str,
) -> Path:
    route = response.get("route", "ARCHIVE").upper()
    target_dir = PROCESSED_DIRS.get(route, PROCESSED_DIRS["ARCHIVE"])
    target_dir.mkdir(parents=True, exist_ok=True)

    final_name = _apply_naming_convention(response, stamped_path.name)
    destination = target_dir / final_name
    if destination.exists():
        destination = target_dir / f"{destination.stem}_{int(time.time())}{destination.suffix}"

    shutil.move(stamped_path, destination)
    logging.info("Routed file to %s", destination)

    qr_value = decode_qr_payload_from_path(destination)
    if qr_value and qr_value != payload:
        logging.warning("QR payload mismatch for %s", destination)

    db.record(
        original_filename=original_name,
        final_filename=destination.name,
        route=route,
        location=str(destination.parent),
        metadata_hash=metadata_hash,
        qr_payload=payload,
    )
    return destination


def process_attachment(attachment: AttachmentRecord, config: EmailIngestionConfig, db: DatabaseClient) -> Optional[Path]:
    text = extract_text(attachment.local_path, config.capture_cmd)
    metadata_hash = build_metadata_hash(text, attachment.local_path)
    route_hint = determine_route_hint(text)
    doc_type = attachment.subject or "email-attachment"
    payload = _qr_payload(doc_type, metadata_hash, route_hint)

    stamped_path = stamp_with_qr(attachment.local_path, payload)
    metadata = {
        "source": attachment.source_email,
        "subject": attachment.subject,
        "original_filename": attachment.filename,
        "route_hint": route_hint,
        "metadata_hash": metadata_hash,
    }

    try:
        response = send_to_worker(
            stamped_path,
            metadata,
            endpoint=config.endpoint,
            max_retries=config.max_retries,
            backoff_seconds=config.backoff_seconds,
        )
    except Exception as exc:
        logging.error("Failed to deliver %s to worker: %s", attachment.filename, exc)
        return None

    final_path = route_file(
        stamped_path, response, payload, metadata_hash, db, original_name=attachment.filename
    )
    try:
        attachment.local_path.unlink(missing_ok=True)
        logging.info("Removed intake copy %s", attachment.local_path)
    except Exception as exc:
        logging.warning("Failed to remove intake file %s: %s", attachment.local_path, exc)
    return final_path


def run_email_ingestion():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    config = EmailIngestionConfig(
        imap_host=os.environ.get("IMAP_HOST", "imap.gmail.com"),
        username=os.environ.get("IMAP_USERNAME", "books@thevlsc.com"),
        password=os.environ.get("IMAP_PASSWORD", ""),
        endpoint=os.environ.get("VLSC_WORKER_ENDPOINT", DEFAULT_WORKER_ENDPOINT),
        capture_cmd=os.environ.get("CAPTURE_TOUCH_OCR_CMD"),
    )

    ingestor = EmailIngestor(config)
    db = DatabaseClient()

    attachments = ingestor.fetch_attachments()
    for attachment in attachments:
        process_attachment(attachment, config, db)


if __name__ == "__main__":
    run_email_ingestion()
