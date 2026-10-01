"""Builds Metrics API `/v2/metrics/cloud/query` request bodies.

Deliberately does not hardcode a canonical list of metric names: Confluent's
metric catalog evolves (metrics can be `PREVIEW` or `GA`, per the
`/descriptors/metrics` response), so callers should resolve metric names via
`ConfluentCloudClient.describe_metrics()` and pass them in explicitly. The
constants below are commonly-used metric names as of the Confluent Cloud
Metrics Reference at the time this was written — treat them as a
starting-point default, not a guarantee, and confirm against
`/descriptors/metrics` for your org before depending on them.
"""
from __future__ import annotations

import datetime as dt
from typing import Iterable, Optional

# Commonly referenced metric names in Confluent's public Metrics Reference.
# CONFIRM against /v2/metrics/cloud/descriptors/metrics for your account —
# names/labels can change between PREVIEW and GA.
LIKELY_METRIC_NAMES = {
    "received_bytes": "io.confluent.kafka.server/received_bytes",
    "sent_bytes": "io.confluent.kafka.server/sent_bytes",
    "retained_bytes": "io.confluent.kafka.server/retained_bytes",
    "request_count": "io.confluent.kafka.server/request_count",
    "active_connection_count": "io.confluent.kafka.server/active_connection_count",
    "cluster_load_percent": "io.confluent.kafka.server/cluster_load_percent",
    "hot_partition_ingress": "io.confluent.kafka.server/hot_partition_ingress",
    "hot_partition_egress": "io.confluent.kafka.server/hot_partition_egress",
}


def build_query(
    metric_name: str,
    resource_type: str,
    resource_ids: Iterable[str],
    start: dt.datetime,
    end: dt.datetime,
    granularity: str = "PT15M",
    group_by: Optional[list[str]] = None,
) -> dict:
    """Builds a single-metric query body.

    Mirrors the documented POST body shape: a list of `aggregations`, a
    `filter` (here: an OR-of-equals over the resource id label), `group_by`,
    `granularity`, and an ISO-8601 `intervals` range. NOTE the Kafka resource
    label is `resource.kafka.id` (pass resource_type="kafka"), not
    `resource.kafka_cluster.id`. Confirm field names against the live API
    reference for your account before trusting this in production.
    """
    resource_label = f"resource.{resource_type}.id"
    filters = [{"field": resource_label, "op": "EQ", "value": rid} for rid in resource_ids]
    body: dict = {
        "aggregations": [{"metric": metric_name}],
        "granularity": granularity,
        "intervals": [f"{start.isoformat()}/{end.isoformat()}"],
    }
    if filters:
        body["filter"] = (
            filters[0] if len(filters) == 1 else {"op": "OR", "filters": filters}
        )
    if group_by:
        body["group_by"] = group_by
    return body
