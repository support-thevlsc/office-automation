"""Local Power Automate emulation helpers.

This module prepares a basic .eml file for the Books account so the local
worker can simulate what a Power Automate flow would send. The worker is
responsible for writing the file to an outbox directory that the Books
account can monitor or forward.
"""

from __future__ import annotations

from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from typing import Optional


def build_forwarding_email(
    attachment_path: Path,
    *,
    books_email: str,
    sender_email: str,
    subject_prefix: str = "DocumentHub Forward",
    body_intro: Optional[str] = None,
) -> EmailMessage:
    """Create an EmailMessage with the provided attachment.

    The attachment is read from disk to avoid depending on the main worker's
    open file handles.
    """

    if body_intro is None:
        body_intro = (
            "Automated forwarding from the local worker. Please review and "
            "route as needed."
        )

    msg = EmailMessage()
    msg["To"] = books_email
    msg["From"] = sender_email
    msg["Subject"] = f"{subject_prefix}: {attachment_path.name}"
    msg.set_content(body_intro)

    file_bytes = attachment_path.read_bytes()
    msg.add_attachment(
        file_bytes,
        maintype="application",
        subtype="octet-stream",
        filename=attachment_path.name,
    )
    return msg


def save_email_to_outbox(msg: EmailMessage, outbox_dir: Path) -> Path:
    """Persist the email to an .eml file for a local pickup folder."""
    outbox_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    subject_part = msg["Subject"] or "Document"
    safe_subject = "".join(ch for ch in subject_part if ch.isalnum() or ch in ("_", "-", " "))
    eml_name = f"{timestamp}__{safe_subject.replace(' ', '_')}.eml"
    eml_path = outbox_dir / eml_name
    eml_path.write_bytes(bytes(msg))
    return eml_path


def forward_file_to_books(
    attachment_path: Path,
    *,
    books_email: str,
    sender_email: str,
    outbox_dir: Path,
    subject_prefix: str = "DocumentHub Forward",
    body_intro: Optional[str] = None,
) -> Path:
    """Build and save an email for the Books account."""
    msg = build_forwarding_email(
        attachment_path,
        books_email=books_email,
        sender_email=sender_email,
        subject_prefix=subject_prefix,
        body_intro=body_intro,
    )
    return save_email_to_outbox(msg, outbox_dir)

