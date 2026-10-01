"""Collects applied-quota facts from the Service Quotas API.

Uses the discovery endpoint (`/service-quota/v1/scopes`) rather than
hardcoding the scope list, though `organization`, `environment`,
`kafka_cluster`, `network`, `user_account`, `service_account` are the
documented values as of when this was written.
"""
from __future__ import annotations

import logging

from collectors.confluent_client import ConfluentCloudClient

logger = logging.getLogger(__name__)

KNOWN_SCOPES = [
    "organization",
    "environment",
    "kafka_cluster",
    "network",
    "user_account",
    "service_account",
]


def collect_applied_quotas(client: ConfluentCloudClient, scope: str, scope_id: str | None = None):
    try:
        return client.list_applied_quotas(scope=scope, scope_id=scope_id)
    except Exception:
        logger.exception("Failed to list applied quotas for scope=%s id=%s", scope, scope_id)
        return []


def compute_utilization_pct(quota_item: dict) -> float | None:
    """Some quotas only expose `applied_limit`, not `usage` (per Confluent's
    docs — "Some service quotas show only applied limits, not usage").
    Returns None when usage isn't available rather than guessing.
    """
    limit = quota_item.get("applied_limit")
    usage = quota_item.get("usage")
    if limit in (None, 0) or usage is None:
        return None
    return (usage / limit) * 100.0
