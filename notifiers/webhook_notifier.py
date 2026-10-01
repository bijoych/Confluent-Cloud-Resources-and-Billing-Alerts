"""Sends the raw alert payload as JSON to a generic webhook URL — for
PagerDuty, Opsgenie, a custom internal endpoint, etc.
"""
from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)


def send_webhook_alert(webhook_url: str, payload: dict, timeout_seconds: int = 10) -> bool:
    if not webhook_url:
        logger.warning("Generic webhook URL not configured; skipping send for %s", payload.get("alert_name"))
        return False
    try:
        resp = requests.post(webhook_url, json=payload, timeout=timeout_seconds)
        resp.raise_for_status()
        return True
    except requests.RequestException:
        logger.exception("Failed to send webhook alert for %s", payload.get("alert_name"))
        return False
