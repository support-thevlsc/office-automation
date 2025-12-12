#!/usr/bin/env python3
"""
capture_ocr_wrapper.py

Simple helper to run a vendor OCR CLI (e.g., Canon CaptureOnTouch) and print
the extracted text to stdout. This is intended as a small, cross-platform
wrapper that can be referenced by the `CAPTURE_TOUCH_OCR_CMD` environment
variable used by `email_ingestion.py`.

Usage:
  capture_ocr_wrapper.py --cmd "/path/to/cotocr --ocr" /path/to/file.pdf
  # or with env var
  export CAPTURE_TOUCH_OCR_CMD="/path/to/capture_ocr_wrapper.py --cmd \"/path/to/cotocr --ocr\""
  capture_ocr_wrapper.py /path/to/file.pdf

You can also include a `{path}` placeholder inside the command; it will be
replaced with the quoted file path instead of being appended.
"""
import argparse
import os
import shlex
import subprocess
import sys


def main() -> int:
    parser = argparse.ArgumentParser(description="Run vendor OCR command and emit text")
    parser.add_argument("path", help="Path to the image/pdf to OCR")
    parser.add_argument("--cmd", help="Command template to run (the file path will be appended)")
    args = parser.parse_args()

    cmd = args.cmd or os.environ.get("CAPTURE_TOUCH_OCR_CMD")
    if not cmd:
        print("Error: no OCR command provided (use --cmd or set CAPTURE_TOUCH_OCR_CMD)", file=sys.stderr)
        return 2

    # Build command and run
    try:
        if "{path}" in cmd:
            interpolated = cmd.replace("{path}", shlex.quote(args.path))
            parts = shlex.split(interpolated)
        else:
            parts = shlex.split(cmd) + [args.path]
        proc = subprocess.run(parts, capture_output=True, text=True, timeout=120)
    except Exception as exc:
        print(f"OCR wrapper exception: {exc}", file=sys.stderr)
        return 3

    # Print stdout (OCR text) and any stderr to stderr
    if proc.stdout:
        sys.stdout.write(proc.stdout)
    if proc.stderr:
        print(proc.stderr, file=sys.stderr)

    return proc.returncode or 0


if __name__ == "__main__":
    raise SystemExit(main())
