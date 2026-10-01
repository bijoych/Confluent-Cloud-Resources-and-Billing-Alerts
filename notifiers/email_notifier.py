"""Sends alerts via SMTP email."""
from __future__ import annotations

import logging
import smtplib
from dataclasses import dataclass
from email.message import EmailMessage

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SmtpConfig:
    host: str
    port: int
    username: str
    password: str
    from_addr: str
    to_addrs: list[str]


def format_email(payload: dict) -> tuple[str, str]:
    subject = f"[{payload['severity']}] {payload['alert_name']} — {payload.get('resource_id') or payload['organization_id']}"
    body_lines = [
        payload.get("message", ""),
        "",
        f"Category: {payload['category']}",
        f"Current value: {payload['current_value']}  (threshold: {payload['threshold']})",
        f"Environment: {payload.get('environment_id') or 'n/a'}",
        f"Observed at: {payload['observed_at']}",
        f"Dashboard: {payload['dashboard_url']}",
    ]
    if payload.get("data_freshness") == "accrued_estimate":
        body_lines.append("")
        body_lines.append(
            "Note: this reflects accrued usage/cost, not a final reconciled invoice."
        )
    return subject, "\n".join(body_lines)


def send_email_alert(config: SmtpConfig, payload: dict, timeout_seconds: int = 15) -> bool:
    if not config.host or not config.to_addrs:
        logger.warning("SMTP not configured; skipping email send for %s", payload.get("alert_name"))
        return False

    subject, body = format_email(payload)
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = config.from_addr
    msg["To"] = ", ".join(config.to_addrs)
    msg.set_content(body)

    try:
        with smtplib.SMTP(config.host, config.port, timeout=timeout_seconds) as server:
            server.starttls()
            if config.username:
                server.login(config.username, config.password)
            server.send_message(msg)
        return True
    except (smtplib.SMTPException, OSError):
        logger.exception("Failed to send email alert for %s", payload.get("alert_name"))
        return False
