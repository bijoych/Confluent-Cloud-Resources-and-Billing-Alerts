"""Sends alerts to a Slack Incoming Webhook.

Uses a plain `requests.post` to the webhook URL rather than the Slack Web
API / bot token flow, since Confluent Cloud's own native notifications
also support Slack via incoming webhook and this keeps the POC's auth
surface minimal (one URL, no OAuth). Swap for `slack_sdk` if you need
richer interactivity later.
"""
from __future__ import annotations

import logging

import requests

logger = logging.getLogger(__name__)

SEVERITY_EMOJI = {"INFO": "ℹ️", "WARNING": "⚠️", "CRITICAL": "🚨"}


def format_slack_message(payload: dict) -> dict:
    emoji = SEVERITY_EMOJI.get(payload["severity"], "")
    lines = [
        f"{emoji} *{payload['severity']}* — {payload['alert_name']} ({payload['category']})",
        payload["message"] if "message" in payload else "",
    ]
    if payload.get("resource_id"):
        lines.append(f"Resource: `{payload['resource_id']}`")
    if payload.get("environment_id"):
        lines.append(f"Environment: `{payload['environment_id']}`")
    if payload.get("data_freshness") == "accrued_estimate":
        lines.append("_Note: accrued usage estimate, not a final invoice value._")
    lines.append(f"<{payload['dashboard_url']}|View dashboard>")
    return {"text": "\n".join(line for line in lines if line)}


def send_slack_alert(webhook_url: str, payload: dict, timeout_seconds: int = 10) -> bool:
    if not webhook_url:
        logger.warning("Slack webhook URL not configured; skipping send for %s", payload.get("alert_name"))
        return False
    body = format_slack_message(payload)
    try:
        resp = requests.post(webhook_url, json=body, timeout=timeout_seconds)
        resp.raise_for_status()
        return True
    except requests.RequestException:
        logger.exception("Failed to send Slack alert for %s", payload.get("alert_name"))
        return False
