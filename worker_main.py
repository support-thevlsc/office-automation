import os
import time
import json
import logging
import shutil
from pathlib import Path
from datetime import datetime

from power_automate_local import forward_file_to_books

CONFIG_PATH = Path(__file__).parent / "Rules" / "worker_config.json"


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


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

POWER_AUTOMATE = CONFIG.get("power_automate_forwarding", {})
EMAIL_FORWARD_ENABLED = POWER_AUTOMATE.get("enabled", False)
BOOKS_EMAIL = POWER_AUTOMATE.get("books_email", "")
SENDER_EMAIL = POWER_AUTOMATE.get("sender_email", "documenthub@local")
OUTBOX_DIR = Path(POWER_AUTOMATE.get("outbox_dir", LOGS_DIR / "Outbox"))

LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Logging
LOG_FILE = Path(CONFIG["logging"]["csv_log"])
logging.basicConfig(
    level=getattr(logging, CONFIG["logging"]["level"], logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
)


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
    with open(RULES_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


ROUTING_RULES = load_routing_rules()


def perform_ocr_stub(input_path: Path, output_txt_path: Path) -> str:
    """
    Stub OCR: just writes a placeholder. Replace with real Tesseract/pdf2image logic later.
    """
    text = f"OCR stub for {input_path.name}"
    output_txt_path.write_text(text, encoding="utf-8")
    return text


def classify_text(text: str):
    """
    Simple rule-based classifier using routing_rules.json.
    Returns (route_tag, priority, confidence).
    """
    lower = text.lower()
    route_tag = ROUTING_RULES.get("fallback_tag", "Misc")
    priority = "P3"
    confidence = 0.2

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
    return f"{today}__{client_code}__{entity}__{doc_type}-{doc_id}__{route_tag}__{priority}__{amount}{ext}"


def log_to_csv(record: dict):
    header = [
        "timestamp",
        "original_path",
        "final_path",
        "route_tag",
        "priority",
        "status",
        "error",
    ]
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    file_exists = LOG_FILE.exists()
    with open(LOG_FILE, "a", encoding="utf-8", newline="") as f:
        import csv

        writer = csv.DictWriter(f, fieldnames=header)
        if not file_exists:
            writer.writeheader()
        writer.writerow(record)


def handle_file(path: Path):
    logging.info(f"Handling file: {path}")
    try:
        if not is_file_stable(path, CONFIG["polling"]["stability_check_seconds"]):
            logging.info(f"File not stable yet, skipping: {path}")
            return

        # Move to TEMP
        TEMP_DIR.mkdir(parents=True, exist_ok=True)
        temp_path = TEMP_DIR / path.name
        shutil.move(str(path), temp_path)
        logging.info(f"Moved to temp: {temp_path}")

        # OCR
        OCR_TEXT_DIR.mkdir(parents=True, exist_ok=True)
        ocr_txt = OCR_TEXT_DIR / (temp_path.stem + ".txt")
        text = perform_ocr_stub(temp_path, ocr_txt)

        # Classification
        route_tag, priority, confidence = classify_text(text)
        min_conf = CONFIG["classification"]["min_confidence"]
        if confidence < min_conf:
            logging.warning(
                f"Low confidence ({confidence:.2f}) for {temp_path}, moving to NeedsReview."
            )
            NEEDS_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
            final_review_path = NEEDS_REVIEW_DIR / temp_path.name
            shutil.move(str(temp_path), final_review_path)
            log_to_csv(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "original_path": str(path),
                    "final_path": str(final_review_path),
                    "route_tag": route_tag,
                    "priority": priority,
                    "status": "NEEDS_REVIEW",
                    "error": f"Low confidence {confidence:.2f}",
                }
            )
            return

        # Build filename
        crc = crc32_for_file(temp_path)
        ext = temp_path.suffix.lower()
        new_name = build_filename(
            route_tag=route_tag, priority=priority, doc_id=crc[:6], ext=ext
        )

        STAGING_DIR.mkdir(parents=True, exist_ok=True)
        final_path = STAGING_DIR / new_name

        # Duplicate detection
        if final_path.exists():
            logging.warning(
                f"Duplicate detected for {final_path}, moving to DuplicateHold."
            )
            DUPLICATE_HOLD_DIR.mkdir(parents=True, exist_ok=True)
            dup_path = DUPLICATE_HOLD_DIR / temp_path.name
            shutil.move(str(temp_path), dup_path)
            log_to_csv(
                {
                    "timestamp": datetime.utcnow().isoformat(),
                    "original_path": str(path),
                    "final_path": str(dup_path),
                    "route_tag": route_tag,
                    "priority": priority,
                    "status": "DUPLICATE",
                    "error": "Duplicate file name",
                }
            )
            return

        shutil.move(str(temp_path), final_path)
        logging.info(f"Moved to staging: {final_path}")

        log_to_csv(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "original_path": str(path),
                "final_path": str(final_path),
                "route_tag": route_tag,
                "priority": priority,
                "status": "OK",
                "error": "",
            }
        )

        if EMAIL_FORWARD_ENABLED and BOOKS_EMAIL:
            try:
                eml_path = forward_file_to_books(
                    final_path,
                    books_email=BOOKS_EMAIL,
                    sender_email=SENDER_EMAIL,
                    outbox_dir=OUTBOX_DIR,
                    subject_prefix="DocumentHub Forward",
                    body_intro=(
                        "Automated forwarding prepared locally for the Books account."
                    ),
                )
                logging.info(
                    f"Prepared Books forwarding email at {eml_path} for {final_path}"
                )
            except Exception as forward_error:
                logging.exception(
                    f"Failed to prepare Books forwarding email for {final_path}: {forward_error}"
                )
                log_to_csv(
                    {
                        "timestamp": datetime.utcnow().isoformat(),
                        "original_path": str(path),
                        "final_path": str(final_path),
                        "route_tag": route_tag,
                        "priority": priority,
                        "status": "FORWARD_FAILED",
                        "error": str(forward_error),
                    }
                )

    except Exception as e:
        logging.exception(f"Error handling file {path}: {e}")
        try:
            NEEDS_REVIEW_DIR.mkdir(parents=True, exist_ok=True)
            error_dest = NEEDS_REVIEW_DIR / path.name
            if path.exists():
                shutil.move(str(path), error_dest)
        except Exception:
            pass
        log_to_csv(
            {
                "timestamp": datetime.utcnow().isoformat(),
                "original_path": str(path),
                "final_path": "",
                "route_tag": "",
                "priority": "",
                "status": "ERROR",
                "error": str(e),
            }
        )


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
        except Exception as e:
            logging.exception(f"Error in polling loop: {e}")
        time.sleep(interval)


if __name__ == "__main__":
    main_loop()
