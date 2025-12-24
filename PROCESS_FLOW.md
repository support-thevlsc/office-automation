# Processing Flow — books@thevlsc.com

Diagram: [Mermaid flowchart](docs/flowchart.mmd)

This document describes the end-to-end file processing path for files received at `books@thevlsc.com` in this repository.

1. Receive email
   - IMAP account: `books@thevlsc.com` (configured via `IMAP_HOST`, `IMAP_USERNAME`, `IMAP_PASSWORD` — see `.env` / `enviroment_config.py`).
   - Code: `email_ingestion.py` connects to IMAP and saves attachments to the intake queue directory: `data/intake_queue` (variable `INTAKE_QUEUE`).

2. Save attachment to intake queue
   - Attachments with allowed extensions (`.pdf`, `.png`, `.jpg`, `.jpeg`) are written to `INTAKE_QUEUE` with a safe name.
   - Function: `EmailIngestor._save_attachments_from_message` in `email_ingestion.py`.

3. OCR / Text extraction
   - Primary: vendor OCR command (Canon CaptureOnTouch) via `CAPTURE_TOUCH_OCR_CMD` or `capture_ocr_wrapper.py` (set via environment); placeholder support `{path}` is supported.
   - Fallback: `pytesseract` (inside `extract_text()` in `email_ingestion.py`).
   - For PDFs, first page is rasterized using `pdf2image` before OCR.

4. Build metadata and determine route
   - `build_metadata_hash()` computes a SHA256 hash of extracted text + file bytes.
   - `determine_route_hint()` inspects extracted text for keywords (invoice, receipt, contract, policy) to suggest `AP`, `AR`, `CLIENT`, `ADMIN`, or `ARCHIVE`.
   - If a metadata hash already exists in `document_routes.db`, the attachment is treated as a duplicate and archived with a QR stamp without calling the worker.

5. Generate QR payload & stamp file
   - QR payload created by `_qr_payload()` and rendered via `_qr_image()` (qrcode lib).
   - File stamping:
     - PDFs: `_stamp_pdf()` merges the QR page into the PDF first page.
     - Images: `_stamp_image()` pastes the QR into the corner.
   - Stamped outputs are saved to `data/stamped` (variable `STAMPED_QUEUE`).

6. Send to worker
   - Stamped file + metadata are POSTed to the worker endpoint defined by `VLSC_WORKER_ENDPOINT` (env / `wrangler.toml`).
   - Function: `send_to_worker()` in `email_ingestion.py`.

7. Worker processing and response
   - Worker: `cloudflare_email_worker.js` (deployed via `wrangler` / `wrangler.toml`).
   - Expected response: JSON including `route`, optional `final_filename` or `naming_convention`, and `metadata` used for naming.

8. Route and persist
   - `route_file()` moves the stamped file into one of the `processed` subdirectories based on route:
     - `data/processed/ap`, `data/processed/ar`, `data/processed/client`, `data/processed/admin`, `data/processed/archive` (mapping `PROCESSED_DIRS`).
   - It decodes the QR again to validate payload and then records the route in the SQLite DB: `data/document_routes.db` via `DatabaseClient.record()`.

9. Additional pieces
   - `worker_main.py`: (orchestrator/launcher — see file for runtime behavior).
   - `capture_ocr_wrapper.py`: wrapper to run vendor OCR CLIs; supports `{path}` substitution.
   - `enviroment_config.py`: scans repo for env placeholders, prompts, writes `.env`, and replaces `<NAME>` tokens.

10. How to run locally (quick)
   - Create venv and install deps:
     ```bash
     python3 -m venv .venv
     source .venv/bin/activate
     pip install -r requirements.txt
     ```
   - Configure env (example):
     ```bash
     export IMAP_HOST="imap.example.com"
     export IMAP_USERNAME="books@thevlsc.com"
     export IMAP_PASSWORD="<your_password>"
     export VLSC_WORKER_ENDPOINT="https://example.workers.dev"
     export CAPTURE_TOUCH_OCR_CMD="/path/to/capture_ocr_wrapper.py --cmd 'tesseract {path} stdout -l eng'"
     ```
   - Start ingestion (one-shot):
     ```bash
     python email_ingestion.py
     ```

If you want a diagram (SVG/PNG) or more detailed sequence diagrams, I can generate that next.
