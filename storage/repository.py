"""Persistence helpers that give the dashboard something to read.

The scheduler collects fresh data every cycle. Costs and current resource
state are stored as a *snapshot* (delete-then-insert per organization) so
the dashboard always reflects the latest cycle without accumulating
duplicate month-to-date cost line items across polls. Alerts are stored
*append-only* — they're a log/feed, so history matters.

Kept separate from collection so the collectors stay pure and testable.
"""
from __future__ import annotations

import datetime as dt
import json

from sqlalchemy.orm import Session

from normalizer.normalize import normalize_cost_items
from storage.models import Alert, CostFact, ResourceFact


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def replace_cost_snapshot(
    session: Session, organization_id: str, cost_items: list[dict]
) -> int:
    """Delete this org's existing CostFacts and insert the fresh MTD set."""
    session.query(CostFact).filter(CostFact.organization_id == organization_id).delete()
    facts = normalize_cost_items(cost_items, organization_id)
    session.add_all(facts)
    session.commit()
    return len(facts)


def replace_resource_snapshot(
    session: Session,
    organization_id: str,
    clusters: list[dict],
    connectors: list[dict],
    quotas: list[dict],
    flink_pools: list[dict] | None = None,
    observed_at: dt.datetime | None = None,
) -> int:
    """Delete this org's existing ResourceFacts and insert fresh current state.

    Stores a flattened, dashboard-friendly dict in raw_json (not the deep
    Confluent API response) so the dashboard doesn't have to dig through
    nested spec/status objects.
    """
    observed_at = observed_at or utcnow()
    session.query(ResourceFact).filter(
        ResourceFact.organization_id == organization_id
    ).delete()

    rows: list[ResourceFact] = []

    for c in clusters:
        flat = {
            "resource_id": c.get("resource_id"),
            "display_name": c.get("display_name"),
            "cluster_kind": c.get("cluster_kind"),
            "cloud": c.get("cloud"),
            "region": c.get("region"),
            "cku": c.get("cku"),
            "phase": c.get("phase"),
            "environment_id": c.get("environment_id"),
        }
        rows.append(
            ResourceFact(
                organization_id=organization_id,
                environment_id=c.get("environment_id"),
                resource_id=c.get("resource_id"),
                resource_type="kafka_cluster",
                metric_name="status",
                value=1.0 if c.get("phase") == "PROVISIONED" else 0.0,
                unit="bool",
                source="cmk_v2",
                observed_at=observed_at,
                raw_json=json.dumps(flat),
            )
        )

    for conn in connectors:
        flat = {
            "resource_id": conn.get("resource_id"),
            "connector_name": conn.get("connector_name"),
            "state": conn.get("state"),
            "task_count": conn.get("task_count"),
            "failed_task_count": conn.get("failed_task_count"),
            "environment_id": conn.get("environment_id"),
            "cluster_id": conn.get("cluster_id"),
        }
        is_failed = 1.0 if (conn.get("state") == "FAILED" or conn.get("failed_task_count")) else 0.0
        rows.append(
            ResourceFact(
                organization_id=organization_id,
                environment_id=conn.get("environment_id"),
                resource_id=conn.get("resource_id"),
                resource_type="connector",
                metric_name="state",
                value=is_failed,
                unit="bool",
                source="connect_v1",
                observed_at=observed_at,
                raw_json=json.dumps(flat),
            )
        )

    for pool in flink_pools or []:
        flat = {
            "resource_id": pool.get("resource_id"),
            "display_name": pool.get("display_name"),
            "cloud": pool.get("cloud"),
            "region": pool.get("region"),
            "max_cfu": pool.get("max_cfu"),
            "current_cfu": pool.get("current_cfu"),
            "phase": pool.get("phase"),
            "environment_id": pool.get("environment_id"),
        }
        rows.append(
            ResourceFact(
                organization_id=organization_id,
                environment_id=pool.get("environment_id"),
                resource_id=pool.get("resource_id"),
                resource_type="flink_compute_pool",
                metric_name="status",
                value=1.0 if pool.get("phase") == "PROVISIONED" else 0.0,
                unit="bool",
                source="fcpm_v2",
                observed_at=observed_at,
                raw_json=json.dumps(flat),
            )
        )

    for q in quotas:
        pct = q.get("_utilization_pct")
        flat = {
            "id": q.get("id"),
            "display_name": q.get("display_name"),
            "applied_limit": q.get("applied_limit"),
            "usage": q.get("usage"),
            "scope": q.get("scope"),
        }
        rows.append(
            ResourceFact(
                organization_id=organization_id,
                environment_id=None,
                resource_id=q.get("id", "unknown-quota"),
                resource_type="quota",
                metric_name="utilization_pct",
                value=float(pct) if pct is not None else 0.0,
                unit="pct",
                source="service_quota_v1",
                observed_at=observed_at,
                raw_json=json.dumps(flat),
            )
        )

    session.add_all(rows)
    session.commit()
    return len(rows)


def record_alert(
    session: Session, candidate, payload: dict, delivery_status: str
) -> None:
    """Append one fired alert to the alerts log/feed."""
    session.add(
        Alert(
            alert_name=candidate.rule_name,
            category=candidate.category,
            severity=candidate.severity,
            organization_id=payload.get("organization_id", ""),
            environment_id=candidate.environment_id,
            resource_id=candidate.resource_id,
            current_value=candidate.current_value,
            threshold=candidate.threshold,
            unit=payload.get("unit"),
            message=candidate.message,
            observed_at=candidate.observed_at,
            sent_at=utcnow(),
            delivery_status=delivery_status,
            raw_payload_json=json.dumps(payload),
        )
    )
    session.commit()
