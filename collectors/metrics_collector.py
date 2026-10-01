"""Collects usage/utilization facts from the Metrics API.

Resolves metric names dynamically from `/descriptors/metrics` (scoped to the
kafka resource type) rather than trusting hardcoded names, so it degrades
gracefully (skips a metric with a warning) instead of silently querying a
stale/renamed metric.

Two things that were previously wrong here:
  - /descriptors/metrics is paginated; pass resource_type to scope it to a
    resource type's metrics (reading only page one looked like missing data).
  - The Kafka resource label in query filters/group_by is `resource.kafka.id`
    (NOT `resource.kafka_cluster.id`).
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import TYPE_CHECKING

from collectors.metrics_query_builder import LIKELY_METRIC_NAMES, build_query

if TYPE_CHECKING:
    from collectors.confluent_client import ConfluentCloudClient

logger = logging.getLogger(__name__)

# The resource-type string and its id label for Kafka clusters in the
# Metrics API (confirmed against Confluent's query examples).
KAFKA_RESOURCE_TYPE = "kafka"
KAFKA_ID_LABEL = "resource.kafka.id"


def resolve_available_metric_names(
    client: "ConfluentCloudClient", resource_type: str = KAFKA_RESOURCE_TYPE
) -> set[str]:
    try:
        descriptors = client.describe_metrics(resource_type=resource_type)
    except Exception:
        logger.exception("Failed to fetch metric descriptors; falling back to static list")
        return set(LIKELY_METRIC_NAMES.values())
    return {m.get("name") for m in descriptors.get("data") or [] if m.get("name")}


def _resolve_metric(available: set[str] | None, preferred: str) -> str | None:
    """Return the concrete metric name to query.

    Prefer an exact match; otherwise match by suffix (the part after the last
    '/') so a changed namespace prefix still resolves. Returns None if nothing
    in the account's descriptors matches, so the caller can skip gracefully.
    """
    if available is None:
        return preferred  # no descriptor info; trust the preferred name
    if preferred in available:
        return preferred
    suffix = "/" + preferred.rsplit("/", 1)[-1]
    matches = [m for m in available if m.endswith(suffix)]
    return matches[0] if matches else None


def _points(result: dict) -> list[dict]:
    return result.get("data") or []


def _point_cluster_id(point: dict) -> str | None:
    return point.get(KAFKA_ID_LABEL) or point.get("resource_id")


def collect_cluster_utilization(
    client: "ConfluentCloudClient",
    cluster_ids: list[str],
    lookback_minutes: int = 30,
    available_metrics: set[str] | None = None,
) -> dict[str, list[dict]]:
    """Returns {cluster_id: [{metric, timestamp, value}, ...]} of cluster load
    percent, for the high-utilization rule. Skips gracefully if the account
    doesn't expose a load metric (e.g. some cluster types).
    """
    if not cluster_ids:
        return {}
    metric_name = _resolve_metric(available_metrics, LIKELY_METRIC_NAMES["cluster_load_percent"])
    if metric_name is None:
        logger.warning(
            "No cluster-load metric in this account's descriptors; skipping utilization "
            "collection (idle-throughput detection still runs)."
        )
        return {}

    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(minutes=lookback_minutes)
    query = build_query(
        metric_name=metric_name,
        resource_type=KAFKA_RESOURCE_TYPE,
        resource_ids=cluster_ids,
        start=start,
        end=end,
        granularity="PT1M",
        group_by=[KAFKA_ID_LABEL],
    )
    try:
        result = client.query_metrics(query)
    except Exception:
        logger.exception("Metrics query failed for cluster utilization")
        return {}

    by_cluster: dict[str, list[dict]] = {}
    for point in _points(result):
        cid = _point_cluster_id(point)
        if not cid:
            continue
        by_cluster.setdefault(cid, []).append(
            {"metric": metric_name, "timestamp": point.get("timestamp"), "value": point.get("value")}
        )
    return by_cluster


def collect_throughput_for_idle_check(
    client: "ConfluentCloudClient",
    cluster_ids: list[str],
    lookback_hours: int = 24,
    available_metrics: set[str] | None = None,
) -> dict[str, float]:
    """Sums received+sent bytes per cluster over the lookback window, for the
    "active resource, near-zero throughput" idle-detection rule. These byte
    metrics are GA for all Kafka cluster types.
    """
    if not cluster_ids:
        return {}
    preferred = [LIKELY_METRIC_NAMES["received_bytes"], LIKELY_METRIC_NAMES["sent_bytes"]]
    metric_names = [m for m in (_resolve_metric(available_metrics, p) for p in preferred) if m]
    if not metric_names:
        logger.warning("No throughput metrics available for idle-resource check; skipping")
        return {}

    end = dt.datetime.now(dt.timezone.utc)
    start = end - dt.timedelta(hours=lookback_hours)
    totals: dict[str, float] = {cid: 0.0 for cid in cluster_ids}
    for metric_name in metric_names:
        query = build_query(
            metric_name=metric_name,
            resource_type=KAFKA_RESOURCE_TYPE,
            resource_ids=cluster_ids,
            start=start,
            end=end,
            granularity="P1D",
            group_by=[KAFKA_ID_LABEL],
        )
        try:
            result = client.query_metrics(query)
        except Exception:
            logger.exception("Metrics query failed for metric %s", metric_name)
            continue
        for point in _points(result):
            cid = _point_cluster_id(point)
            value = point.get("value") or 0
            if cid in totals:
                totals[cid] += value
    return totals
