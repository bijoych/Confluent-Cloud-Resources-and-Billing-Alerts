"""Audit log collector — consumes Confluent Cloud's managed audit log Kafka
cluster for security/access events (API key creation, RBAC/privilege
changes, etc.).

This is deliberately a working *skeleton*, not a fully wired collector:
audit log ingestion needs your org's specific audit-log cluster bootstrap
servers and topic name (both org-specific), plus a Cluster API key scoped
to that audit-log cluster. Fill in `config/config.yaml: audit_log` and set
`CONFLUENT_AUDIT_LOG_API_KEY` / `_SECRET` in secrets.env, then flip
`audit_log.enabled: true`.

I'm not certain of the exact current audit-log event schema/topic naming
for every org — verify against
https://docs.confluent.io/cloud/current/monitoring/audit-logging/cloud-audit-log-concepts.html
before relying on field names here.
"""
from __future__ import annotations

import json
import logging
from typing import Callable, Iterator

logger = logging.getLogger(__name__)

# Event categories worth surfacing as "security_and_audit" alerts, per the
# design doc. Confirm exact `methodName`/`eventType` values against your
# audit log payloads before trusting this filter.
INTERESTING_EVENT_KEYWORDS = (
    "ApiKey",
    "RoleBinding",
    "IamPolicy",
    "ServiceAccount",
)


def is_interesting_event(event: dict) -> bool:
    method = event.get("methodName", "") or event.get("method_name", "")
    return any(keyword in method for keyword in INTERESTING_EVENT_KEYWORDS)


def consume_audit_log_events(
    bootstrap_servers: str,
    topic: str,
    api_key: str,
    api_secret: str,
    group_id: str = "confluent-billing-alerts-audit-consumer",
    poll_timeout_seconds: float = 5.0,
    max_messages: int = 500,
) -> Iterator[dict]:
    """Yields decoded audit log event dicts. Requires `confluent-kafka`.

    This function intentionally does a bounded poll loop (`max_messages`)
    rather than running forever, so the scheduler can call it once per
    polling interval like the other collectors.
    """
    try:
        from confluent_kafka import Consumer
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "confluent-kafka is required for audit log collection; "
            "pip install confluent-kafka"
        ) from exc

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
        "auto.offset.reset": "latest",
    }
    consumer = Consumer(conf)
    consumer.subscribe([topic])
    seen = 0
    try:
        while seen < max_messages:
            msg = consumer.poll(poll_timeout_seconds)
            if msg is None:
                break
            if msg.error():
                logger.warning("Audit log consumer error: %s", msg.error())
                continue
            try:
                event = json.loads(msg.value())
            except (ValueError, TypeError):
                logger.warning("Skipping non-JSON audit log message")
                continue
            seen += 1
            yield event
    finally:
        consumer.close()


def collect_security_events(
    bootstrap_servers: str,
    topic: str,
    api_key: str,
    api_secret: str,
    on_event: Callable[[dict], None] | None = None,
) -> list[dict]:
    events = []
    for event in consume_audit_log_events(bootstrap_servers, topic, api_key, api_secret):
        if is_interesting_event(event):
            events.append(event)
            if on_event:
                on_event(event)
    return events
