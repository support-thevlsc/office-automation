# Setup_VLSC_DocumentHub.ps1
# Creates folder structure, config files, worker, venv, dependencies, and auto-start.

$ErrorActionPreference = "Stop"

# -------- SETTINGS --------
$BasePath   = "D:\OneDrive - The VLSC\The_VLSC_DocumentHub"
$PythonCmd  = "python"
# --------------------------

Write-Host "Base path: $BasePath"

# 1. Create directory tree
$dirs = @(
    "00_Inbox\RawScans",
    "00_Inbox\EmailIntake",
    "00_Inbox\Imports",
    "00_Inbox\NeedsReview",
    "00_Inbox\DuplicateHold",

    "01_Processing\OCR_Text",
    "01_Processing\Staging",
    "01_Processing\Rules",
    "01_Processing\Logs",
    "01_Processing\Temp",

    "02_Processed\Accounting",
    "02_Processed\Admin",
    "02_Processed\Legal",
    "02_Processed\Operations",
    "02_Processed\HR",
    "02_Processed\Misc",

    "03_Clients\_ClientTemplate",
    "04_Vendors\_VendorTemplate",

    "05_Reports_and_Exports\Monthly",
    "05_Reports_and_Exports\Quarterly",
    "05_Reports_and_Exports\YearEnd",
    "05_Reports_and_Exports\Custom",

    "06_Archive\ByYear",
    "06_Archive\ClosedClients"
)

foreach ($d in $dirs) {
    $full = Join-Path $BasePath $d
    if (-not (Test-Path $full)) {
        New-Item -ItemType Directory -Path $full | Out-Null
        Write-Host "Created: $full"
    } else {
        Write-Host "Exists:  $full"
    }
}

# README
$readmePath = Join-Path $BasePath "README_SystemMap.md"
if (-not (Test-Path $readmePath)) {
    @"
# The VLSC Document Hub

Base path: $BasePath

This hub is processed by a local Python worker and routed by Power Automate.
Only **01_Processing\Staging** is watched by Power Automate.
"@ | Set-Content -Path $readmePath -Encoding UTF8
}

# Shortcuts
$processingDir = Join-Path $BasePath "01_Processing"
$rulesDir      = Join-Path $processingDir "Rules"
$logsDir       = Join-Path $processingDir "Logs"
$tempDir       = Join-Path $processingDir "Temp"
$ocrTextDir    = Join-Path $processingDir "OCR_Text"
$stagingDir    = Join-Path $processingDir "Staging"
$rawScansDir   = Join-Path $BasePath "00_Inbox\RawScans"
$needsReviewDir = Join-Path $BasePath "00_Inbox\NeedsReview"
$duplicateHoldDir = Join-Path $BasePath "00_Inbox\DuplicateHold"

# 2. Create worker_config.json
$workerConfig = @{
    paths = @{
        base_dir          = $BasePath
        raw_scans_dir     = $rawScansDir
        needs_review_dir  = $needsReviewDir
        duplicate_hold_dir= $duplicateHoldDir
        ocr_text_dir      = $ocrTextDir
        staging_dir       = $stagingDir
        temp_dir          = $tempDir
        logs_dir          = $logsDir
        rules_file        = (Join-Path $rulesDir "routing_rules.json")
    }
    ocr = @{
        engine    = "stub"   # change to "tesseract" when you hook in real OCR
        language  = "eng"
    }
    classification = @{
        use_rules_only  = $true
        use_openai      = $false  # set true when you add real OpenAI keys/logic
        min_confidence  = 0.60
    }
    logging = @{
        level   = "INFO"
        csv_log = (Join-Path $logsDir "worker_log.csv")
    }
    filename_schema = @{
        pattern   = "YYYY-MM-DD__ClientCode__Entity__DocType-DocID__RouteTag__Priority__Amount"
        extension = ".pdf"
    }
    polling = @{
        interval_seconds           = 10
        stability_check_seconds    = 3
        allowed_extensions         = @(".pdf", ".jpg", ".jpeg", ".png", ".tif", ".tiff")
    }
}

$workerConfigPath = Join-Path $rulesDir "worker_config.json"
$workerConfig | ConvertTo-Json -Depth 6 | Set-Content -Path $workerConfigPath -Encoding UTF8
Write-Host "Wrote: $workerConfigPath"

# 3. Create routing_rules.json
$routingRules = @{
    route_tags = @(
        "AP", "AR", "Banking", "Payroll", "Tax", "GL", "FixedAssets",
        "Admin", "Legal", "Operations", "HR", "Misc"
    )
    fallback_tag = "Misc"
    priorities = @{
        P1 = @("past due", "final notice", "urgent", "collections")
        P2 = @("invoice", "statement", "payroll", "tax", "bank")
        P3 = @("newsletter", "promo", "advertisement")
    }
    keyword_routes = @(
        @{ tag = "AP"; keywords = @("invoice", "bill to", "accounts payable", "pay this amount") },
        @{ tag = "AR"; keywords = @("remittance", "payment advice", "accounts receivable") },
        @{ tag = "Banking"; keywords = @("bank statement", "deposit detail", "check image") },
        @{ tag = "Payroll"; keywords = @("pay stub", "payroll summary", "w-2", "w2") },
        @{ tag = "Tax"; keywords = @("irs", "tax return", "1040", "1120", "k-1", "k1") },
        @{ tag = "Legal"; keywords = @("contract", "agreement", "nda", "settlement") },
        @{ tag = "HR"; keywords = @("offer letter", "termination", "performance review") },
        @{ tag = "Admin"; keywords = @("utility bill", "lease", "insurance") }
    )
}

$routingRulesPath = Join-Path $rulesDir "routing_rules.json"
$routingRules | ConvertTo-Json -Depth 6 | Set-Content -Path $routingRulesPath -Encoding UTF8
Write-Host "Wrote: $routingRulesPath"

# 4. Create requirements.txt
$requirementsPath = Join-Path $processingDir "requirements.txt"
@"
pytesseract
Pillow
pdf2image
watchdog
openai
"@ | Set-Content -Path $requirementsPath -Encoding UTF8
Write-Host "Wrote: $requirementsPath"

# 5. Create worker_main.py (full skeleton)
$workerPyPath = Join-Path $processingDir "worker_main.py"
@'
import os
import time
import json
import logging
import shutil
from pathlib import Path
from datetime import datetime

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
'@ | Set-Content -Path $workerPyPath -Encoding UTF8
Write-Host "Wrote: $workerPyPath"

# 6. Create virtual environment & install dependencies
Write-Host "Creating virtual environment in: $processingDir\.venv"
Push-Location $processingDir
try {
    & $PythonCmd -m venv ".venv"
    $venvPython = Join-Path $processingDir ".venv\Scripts\python.exe"

    Write-Host "Upgrading pip..."
    & $venvPython -m pip install --upgrade pip

    Write-Host "Installing requirements..."
    & $venvPython -m pip install -r $requirementsPath
} catch {
    Write-Warning "Could not create venv or install packages. Error: $_"
}
Pop-Location

# 7. Auto-run batch file in Startup folder
$startupDir = [Environment]::GetFolderPath("Startup")
$batPath = Join-Path $startupDir "Start_VLSC_Worker.bat"
$batContent = @"
@echo off
cd /d "$processingDir"
".venv\Scripts\python.exe" "worker_main.py"
"@
$batContent | Set-Content -Path $batPath -Encoding ASCII
Write-Host "Created Startup batch: $batPath"

Write-Host "Setup complete."
