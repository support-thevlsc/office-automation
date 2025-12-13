import os
import re
import csv
import time
import json
import logging
import shutil
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Any

from pdf2image import convert_from_path
from PIL import Image
import pytesseract

BASE_DIR = Path(__file__).parent
CONFIG_PATH = BASE_DIR / "Rules" / "worker_config.json"

DEFAULT_CONFIG = {
    "paths": {
        "raw_scans_dir": str(BASE_DIR / "data" / "incoming"),
        "needs_review_dir": str(BASE_DIR / "data" / "needs_review"),
        "duplicate_hold_dir": str(BASE_DIR / "data" / "duplicate_hold"),
        "ocr_text_dir": str(BASE_DIR / "data" / "ocr_text"),
        "staging_dir": str(BASE_DIR / "data" / "staging"),
        "temp_dir": str(BASE_DIR / "data" / "temp"),
        "logs_dir": str(BASE_DIR / "logs"),
        "rules_file": str(BASE_DIR / "Rules" / "routing_rules.json"),
        "personal_storage_dir": str(BASE_DIR / "data" / "personal" / "secure"),
    },
    "logging": {
        "csv_log": str(BASE_DIR / "logs" / "worker_log.csv"),
        "level": "INFO",
        "metadata_store": str(BASE_DIR / "logs" / "documents.db"),
    },
    "classification": {"min_confidence": 0.5},
    "polling": {
        "interval_seconds": 10,
        "stability_check_seconds": 2,
        "allowed_extensions": [".pdf", ".png", ".jpg", ".jpeg", ".tif", ".tiff"],
    },
}


def load_config():
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    logging.warning("Config file missing, using defaults: %s", CONFIG_PATH)
    return DEFAULT_CONFIG


CONFIG = load_config()

# Paths
PATHS = CONFIG["paths"]
RAW_SCANS_DIR = Path(PATHS["raw_scans_dir"])
NEEDS_REVIEW_DIR = Path(PATHS["needs_review_dir"])
DUPLICATE_HOLD_DIR = Path(PATHS["duplicate_hold_dir"])
OCR_TEXT_DIR = Path(PATHS["ocr_text_dir"])
STAGING_DIR = Path(PATHS["staging_dir"])
TEMP_DIR = Path(PATHS["temp_dir"])
LOGS_DIR = Path(PATHS["logs_dir"])
RULES_FILE = Path(PATHS["rules_file"])
PERSONAL_STORAGE_DIR = Path(PATHS["personal_storage_dir"])

LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Logging
LOG_FILE = Path(CONFIG["logging"]["csv_log"])
logging.basicConfig(
    level=getattr(logging, CONFIG["logging"]["level"], logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)


class FlexibleStore:
    """SQLite-backed store that grows columns as new metadata fields appear."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.ensure_base_table()

    def ensure_base_table(self):
        with self.conn:
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    original_path TEXT,
                    final_path TEXT,
                    route_tag TEXT,
                    priority TEXT,
                    status TEXT,
                    error TEXT,
                    classification_confidence REAL,
                    ocr_confidence REAL
                )
                """
            )

    def existing_columns(self) -> set[str]:
        cur = self.conn.execute("PRAGMA table_info(documents)")
        return {row[1] for row in cur.fetchall()}

    def add_columns(self, record_keys: set[str]):
        current = self.existing_columns()
        new_cols = record_keys - current
        for col in new_cols:
            with self.conn:
                self.conn.execute(f"ALTER TABLE documents ADD COLUMN {col} TEXT")

    def record(self, record: dict):
        self.add_columns(set(record.keys()))
        columns = ", ".join(record.keys())
        placeholders = ":" + ", :".join(record.keys())
        with self.conn:
            self.conn.execute(
                f"INSERT INTO documents ({columns}) VALUES ({placeholders})", record
            )


METADATA_STORE_PATH = Path(CONFIG.get("logging", {}).get("metadata_store", LOGS_DIR / "documents.db"))
STORE = FlexibleStore(METADATA_STORE_PATH)


# Utility


def ensure_directory(path: Path, mode: int = 0o755) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(path, mode)
    except PermissionError:
        logging.warning("Unable to set permissions for %s", path)
    return path


def ensure_personal_directory(base_dir: Path) -> Path:
    personal_root = ensure_directory(base_dir, mode=0o700)
    dated_dir = ensure_directory(
        personal_root / datetime.utcnow().strftime("%Y%m%d"), mode=0o700
    )
    return dated_dir


def detect_file_format(path: Path) -> str:
    suffix = path.suffix.lower().lstrip(".")
    return suffix or "unknown"


def is_personal_file(path: Path, metadata: dict) -> bool:
    name_lower = path.name.lower()
    personal_keywords = ["owner", "personal"]
    if any(keyword in name_lower for keyword in personal_keywords):
        return True
    doc_type = metadata.get("document_type", "")
    if isinstance(doc_type, str) and "personal" in doc_type.lower():
        return True
    return False

def crc32_for_file(path: Path) -> str:
    import zlib

    buf_size = 65536
    crc = 0
    with open(path, "rb") as f:
        while True:
            data = f.read(buf_size)
            if not data:
                break
            crc = zlib.crc32(data, crc)
    return format(crc & 0xFFFFFFFF, "08x")


def is_file_stable(path: Path, wait_seconds: int) -> bool:
    """Check if file size is unchanged after wait_seconds."""
    size1 = path.stat().st_size
    time.sleep(wait_seconds)
    size2 = path.stat().st_size
    return size1 == size2


def load_routing_rules():
    if RULES_FILE.exists():
        with open(RULES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    logging.warning("Routing rules missing, using defaults: %s", RULES_FILE)
    return {
        "fallback_tag": "Misc",
        "priorities": {"P1": ["urgent", "overdue"], "P2": ["invoice", "statement"]},
        "keyword_routes": [
            {"tag": "AP", "keywords": ["invoice", "purchase order", "vendor"]},
            {"tag": "AR", "keywords": ["receipt", "payment"]},
        ],
        "document_type_routes": {"invoice": "AP", "receipt": "AR"},
    }


ROUTING_RULES = load_routing_rules()


# OCR and extraction

def extract_text_from_image(img: Image.Image) -> tuple[str, float]:
    """Run Tesseract OCR on a PIL image and return text with average confidence."""
    ocr_data = pytesseract.image_to_data(img, output_type=pytesseract.Output.DICT)
    texts = []
    confidences = []
    for text, conf in zip(ocr_data.get("text", []), ocr_data.get("conf", [])):
        if text and text.strip():
            texts.append(text)
        try:
            conf_val = float(conf)
            if conf_val >= 0:
                confidences.append(conf_val)
        except (TypeError, ValueError):
            continue
    text_out = " ".join(texts)
    avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
    return text_out, avg_conf


def perform_ocr(input_path: Path, output_txt_path: Path) -> tuple[str, float]:
    """Perform OCR using Tesseract/pdf2image for PDFs and images."""
    texts: list[str] = []
    confidences: list[float] = []

    try:
        if input_path.suffix.lower() == ".pdf":
            pages = convert_from_path(str(input_path))
            for page in pages:
                text, conf = extract_text_from_image(page)
                texts.append(text)
                if conf:
                    confidences.append(conf)
        else:
            img = Image.open(input_path)
            text, conf = extract_text_from_image(img)
            texts.append(text)
            if conf:
                confidences.append(conf)
    except Exception as exc:  # pragma: no cover - defensive logging
        logging.exception("OCR failed for %s: %s", input_path, exc)
        raise

    full_text = "\n".join(texts).strip()
    avg_conf = sum(confidences) / len(confidences) if confidences else 0.0
    output_txt_path.write_text(full_text, encoding="utf-8")
    return full_text, avg_conf


def parse_dates(text: str) -> list[str]:
    date_patterns = [
        r"\b\d{4}-\d{2}-\d{2}\b",  # YYYY-MM-DD
        r"\b\d{2}/\d{2}/\d{4}\b",  # MM/DD/YYYY
        r"\b\d{1,2}\s+[A-Za-z]{3,}\s+\d{4}\b",  # 1 Jan 2023
    ]
    dates: set[str] = set()
    for pattern in date_patterns:
        for match in re.findall(pattern, text):
            dates.add(match)
    return sorted(dates)


def parse_amount(text: str) -> tuple[str, float]:
    amount_match = re.findall(r"\$?(-?\d{1,3}(?:,\d{3})*(?:\.\d{2})?)", text)
    if not amount_match:
        return "0.00", 0.2
    cleaned = [amt.replace(",", "").lstrip("$") for amt in amount_match]
    try:
        numeric = [float(a) for a in cleaned]
    except ValueError:
        return "0.00", 0.2
    best = max(numeric)
    return f"{best:.2f}", 0.9


def parse_vendor(text: str) -> tuple[str, float]:
    vendor_lines = []
    for line in text.splitlines():
        if any(keyword in line.lower() for keyword in ["invoice", "bill", "statement"]):
            continue
        if len(line.strip()) > 4 and re.search(r"[A-Za-z]{2,}", line):
            vendor_lines.append(line.strip())
    vendor = vendor_lines[0] if vendor_lines else ""
    return vendor, 0.6 if vendor else 0.2


def parse_identifiers(text: str) -> tuple[list[str], float]:
    patterns = [
        r"\bINV[- ]?\d{4,}\b",
        r"\bPO[- ]?\d{4,}\b",
        r"\b\d{8,}\b",
    ]
    ids: set[str] = set()
    for pattern in patterns:
        ids.update(re.findall(pattern, text, flags=re.IGNORECASE))
    confidence = 0.7 if ids else 0.2
    return sorted(ids), confidence


def parse_document_type(text: str) -> tuple[str, float]:
    lower = text.lower()
    mapping = {
        "invoice": ["invoice", "inv#"],
        "receipt": ["receipt", "thanks for your purchase"],
        "purchase_order": ["purchase order", "po #", "po#"],
        "statement": ["statement", "balance forward"],
    }
    for doc_type, keywords in mapping.items():
        for keyword in keywords:
            if keyword in lower:
                return doc_type, 0.85
    return "document", 0.3


def parse_line_items(text: str) -> list[dict]:
    line_items = []
    for line in text.splitlines():
        if re.search(r"\bqty\b", line.lower()) and re.search(r"\d", line):
            parts = line.split()
            qty = next((p for p in parts if p.replace(".", "", 1).isdigit()), "")
            amount = next((p for p in parts if re.match(r"\$?\d", p)), "")
            description = " ".join(p for p in parts if p not in {qty, amount})
            if description:
                line_items.append(
                    {
                        "description": description,
                        "quantity": qty,
                        "amount": amount,
                    }
                )
    return line_items


def extract_document_metadata(text: str) -> dict:
    doc_type, type_conf = parse_document_type(text)
    vendor, vendor_conf = parse_vendor(text)
    total, total_conf = parse_amount(text)
    identifiers, id_conf = parse_identifiers(text)
    dates = parse_dates(text)
    line_items = parse_line_items(text)
    return {
        "document_type": doc_type,
        "document_type_confidence": type_conf,
        "vendor": vendor,
        "vendor_confidence": vendor_conf,
        "total": total,
        "total_confidence": total_conf,
        "identifiers": identifiers,
        "identifier_confidence": id_conf,
        "dates": dates,
        "line_items": line_items,
    }


# Classification

def classify_text(text: str, metadata: dict | None = None):
    """
    Rule-based classifier using routing_rules.json and extracted metadata.
    Returns (route_tag, priority, confidence).
    """
    lower = text.lower()
    route_tag = ROUTING_RULES.get("fallback_tag", "Misc")
    priority = "P3"
    confidence = 0.2

    # metadata driven
    if metadata:
        doc_type = metadata.get("document_type")
        doc_type_routes = ROUTING_RULES.get("document_type_routes", {})
        if doc_type and doc_type in doc_type_routes:
            route_tag = doc_type_routes[doc_type]
            confidence = max(confidence, metadata.get("document_type_confidence", 0.0))

    # priority detection
    for p, keywords in ROUTING_RULES.get("priorities", {}).items():
        for kw in keywords:
            if kw.lower() in lower:
                priority = p
                confidence = max(confidence, 0.6)
                break

    # route detection
    for rule in ROUTING_RULES.get("keyword_routes", []):
        tag = rule.get("tag")
        kws = rule.get("keywords", [])
        for kw in kws:
            if kw.lower() in lower:
                route_tag = tag
                confidence = max(confidence, 0.8)
                break

    return route_tag, priority, confidence


# File handling helpers

def build_filename(
    route_tag: str,
    priority: str,
    amount: str = "0.00",
    client_code: str = "GEN",
    entity: str = "MAIN",
    doc_type: str = "DOC",
    doc_id: str = "0001",
    ext: str = ".pdf",
) -> str:
    today = datetime.today().strftime("%Y-%m-%d")
    safe_doc_type = doc_type.upper().replace(" ", "_")
    return f"{today}__{client_code}__{entity}__{safe_doc_type}-{doc_id}__{route_tag}__{priority}__{amount}{ext}"


def sanitize_record_values(record: dict[str, Any]) -> dict[str, Any]:
    sanitized = {}
    for key, value in record.items():
        if isinstance(value, (dict, list)):
            sanitized[key] = json.dumps(value)
        else:
            sanitized[key] = value
    return sanitized


def log_to_csv(record: dict):
    record = sanitize_record_values(record)
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    rows = []
    header: list[str] = []

    if LOG_FILE.exists():
        with open(LOG_FILE, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            header = reader.fieldnames or []
            rows = list(reader)

    for key in record.keys():
        if key not in header:
            header.append(key)

    rows.append(record)

    with open(LOG_FILE, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow({h: row.get(h, "") for h in header})


# Main worker

def handle_file(path: Path):
    logging.info(f"Handling file: {path}")
    try:
        if not is_file_stable(path, CONFIG["polling"]["stability_check_seconds"]):
            logging.info(f"File not stable yet, skipping: {path}")
            return

        allowed_exts = set(
            ext.lower() for ext in CONFIG["polling"].get("allowed_extensions", [])
        )
        file_format = detect_file_format(path)
        if allowed_exts and path.suffix.lower() not in allowed_exts:
            logging.warning(
                "Unrecognized file format %s for %s, moving to NeedsReview", file_format, path
            )
            NEEDS_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
            review_dest = NEEDS_REVIEW_DIR / path.name
            shutil.move(str(path), review_dest)
            record = {
                "timestamp": datetime.utcnow().isoformat(),
                "original_path": str(path),
                "final_path": str(review_dest),
                "route_tag": "",
                "priority": "",
                "status": "UNRECOGNIZED_FORMAT",
                "error": "",
                "file_format": file_format,
                "classification_confidence": 0.0,
                "ocr_confidence": 0.0,
            }
            log_to_csv(record)
            STORE.record(record)
            return

        # Move to TEMP
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        temp_path = TEMP_DIR / path.name
        shutil.move(str(path), temp_path)
        logging.info(f"Moved to temp: {temp_path}")

        # OCR
        OCR_TEXT_DIR.mkdir(parents=True, exist_ok=True)
        ocr_txt = OCR_TEXT_DIR / (temp_path.stem + ".txt")
        text, ocr_conf = perform_ocr(temp_path, ocr_txt)

        # Extraction
        metadata = extract_document_metadata(text)
        metadata["file_format"] = file_format

        # Classification
        route_tag, priority, class_confidence = classify_text(text, metadata)
        max_class_conf = max(class_confidence, metadata.get("document_type_confidence", 0.0))
        ocr_conf_cap = ocr_conf if ocr_conf is not None else 1.0
        combined_confidence = min(max_class_conf, ocr_conf_cap)
        min_conf = CONFIG["classification"]["min_confidence"]
        if combined_confidence < min_conf:
            logging.warning(
                f"Low confidence ({combined_confidence:.2f}) for {temp_path}, moving to NeedsReview."
            )
            NEEDS_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
            final_review_path = NEEDS_REVIEW_DIR / temp_path.name
            shutil.move(str(temp_path), final_review_path)
            record = {
                "timestamp": datetime.utcnow().isoformat(),
                "original_path": str(path),
                "final_path": str(final_review_path),
                "route_tag": route_tag,
                "priority": priority,
                "status": "NEEDS_REVIEW",
                "error": f"Low confidence {combined_confidence:.2f}",
                "classification_confidence": combined_confidence,
                "ocr_confidence": ocr_conf,
                **metadata,
            }
            log_to_csv(record)
            STORE.record(record)
            return

        # Build filename
        crc = crc32_for_file(temp_path)
        ext = temp_path.suffix.lower()
        doc_id = metadata.get("identifiers", [crc[:6]])
        doc_id_str = doc_id[0] if isinstance(doc_id, list) and doc_id else crc[:6]
        new_name = build_filename(
            route_tag=route_tag,
            priority=priority,
            amount=metadata.get("total", "0.00"),
            doc_id=doc_id_str,
            doc_type=metadata.get("document_type", "DOC"),
            ext=ext,
        )

        is_personal = is_personal_file(temp_path, metadata)
        metadata["is_personal"] = is_personal

        target_dir = (
            ensure_personal_directory(PERSONAL_STORAGE_DIR) if is_personal else STAGING_DIR
        )
        target_dir.mkdir(parents=True, exist_ok=True)
        final_path = target_dir / new_name

        # Duplicate detection
        if final_path.exists():
            logging.warning(
                f"Duplicate detected for {final_path}, moving to DuplicateHold."
            )
            DUPLICATE_HOLD_DIR.mkdir(parents=True, exist_ok=True)
            dup_path = DUPLICATE_HOLD_DIR / temp_path.name
            shutil.move(str(temp_path), dup_path)
            record = {
                "timestamp": datetime.utcnow().isoformat(),
                "original_path": str(path),
                "final_path": str(dup_path),
                "route_tag": route_tag,
                "priority": priority,
                "status": "DUPLICATE",
                "error": "Duplicate file name",
                "classification_confidence": combined_confidence,
                "ocr_confidence": ocr_conf,
                **metadata,
            }
            log_to_csv(record)
            STORE.record(record)
            return

        shutil.move(str(temp_path), final_path)
        destination_label = "personal storage" if is_personal else "staging"
        logging.info(f"Moved to {destination_label}: {final_path}")

        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "original_path": str(path),
            "final_path": str(final_path),
            "route_tag": route_tag,
            "priority": priority,
            "status": "PERSONAL_STORED" if is_personal else "OK",
            "error": "",
            "classification_confidence": combined_confidence,
            "ocr_confidence": ocr_conf,
            **metadata,
        }
        log_to_csv(record)
        STORE.record(record)

    except Exception as e:  # pragma: no cover - defensive
        logging.exception(f"Error handling file {path}: {e}")
        try:
            NEEDS_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
            error_dest = NEEDS_REVIEW_DIR / path.name
            if path.exists():
                shutil.move(str(path), error_dest)
        except Exception:
            pass
        record = {
            "timestamp": datetime.utcnow().isoformat(),
            "original_path": str(path),
            "final_path": "",
            "route_tag": "",
            "priority": "",
            "status": "ERROR",
            "error": str(e),
        }
        log_to_csv(record)
        STORE.record(record)


def main_loop():
    logging.info("Starting VLSC worker main loop...")
    interval = CONFIG["polling"]["interval_seconds"]
    allowed_exts = set(e.lower() for e in CONFIG["polling"]["allowed_extensions"])

    RAW_SCANS_DIR.mkdir(parents=True, exist_ok=True)

    while True:
        try:
            for entry in RAW_SCANS_DIR.iterdir():
                if not entry.is_file():
                    continue
                if entry.suffix.lower() not in allowed_exts:
                    continue
                handle_file(entry)
        except Exception as e:  # pragma: no cover - defensive loop guard
            logging.exception(f"Error in polling loop: {e}")
        time.sleep(interval)


if __name__ == "__main__":
    main_loop()
