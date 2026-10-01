"""Read-side aggregation queries for the dashboard.

Deliberately imports no Flask — everything here is plain SQLAlchemy over
the storage models, so it can be unit-tested against an in-memory SQLite
DB without standing up the web app.

All cost figures returned here are accrued estimates (see the design doc's
note that the Costs API lags actual usage and is not a final invoice).
"""
from __future__ import annotations

import datetime as dt
import json

from sqlalchemy import func
from sqlalchemy.orm import Session

from collectors.billing_collector import project_month_end_spend
from storage.models import Alert, CostFact, ResourceFact


def _load_raw(row: ResourceFact) -> dict:
    try:
        return json.loads(row.raw_json) if row.raw_json else {}
    except (ValueError, TypeError):
        return {}


def billing_summary(session: Session, organization_id: str, budget_usd: float) -> dict:
    total = (
        session.query(func.sum(CostFact.amount))
        .filter(CostFact.organization_id == organization_id)
        .scalar()
        or 0.0
    )
    forecast = project_month_end_spend(total)
    return {
        "mtd_spend_usd": round(total, 2),
        "budget_usd": budget_usd,
        "pct_of_budget": round((total / budget_usd * 100.0), 1) if budget_usd else 0.0,
        "forecast_usd": round(forecast, 2),
        "forecast_pct_of_budget": round((forecast / budget_usd * 100.0), 1) if budget_usd else 0.0,
        "over_budget_forecast": forecast > budget_usd if budget_usd else False,
        "data_freshness": "accrued_estimate",
    }


def cost_by_day(session: Session, organization_id: str) -> list[dict]:
    rows = (
        session.query(CostFact.start_date, func.sum(CostFact.amount))
        .filter(CostFact.organization_id == organization_id)
        .group_by(CostFact.start_date)
        .order_by(CostFact.start_date)
        .all()
    )
    return [{"date": d, "amount": round(amt or 0.0, 2)} for d, amt in rows if d]


def cost_by_resource(session: Session, organization_id: str, limit: int = 10) -> list[dict]:
    rows = (
        session.query(CostFact.resource_id, func.sum(CostFact.amount))
        .filter(CostFact.organization_id == organization_id)
        .group_by(CostFact.resource_id)
        .order_by(func.sum(CostFact.amount).desc())
        .limit(limit)
        .all()
    )
    return [{"resource_id": rid or "UNKNOWN", "amount": round(amt or 0.0, 2)} for rid, amt in rows]


def cost_by_product(session: Session, organization_id: str) -> list[dict]:
    rows = (
        session.query(CostFact.product, func.sum(CostFact.amount))
        .filter(CostFact.organization_id == organization_id)
        .group_by(CostFact.product)
        .order_by(func.sum(CostFact.amount).desc())
        .all()
    )
    return [{"product": p or "OTHER", "amount": round(amt or 0.0, 2)} for p, amt in rows]


def recent_alerts(session: Session, limit: int = 50) -> list[dict]:
    rows = session.query(Alert).order_by(Alert.sent_at.desc()).limit(limit).all()
    return [
        {
            "alert_name": a.alert_name,
            "category": a.category,
            "severity": a.severity,
            "resource_id": a.resource_id,
            "message": a.message,
            "current_value": a.current_value,
            "threshold": a.threshold,
            "sent_at": a.sent_at.isoformat() if a.sent_at else None,
            "delivery_status": a.delivery_status,
        }
        for a in rows
    ]


def alert_counts_last_24h(session: Session) -> dict:
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24)
    rows = (
        session.query(Alert.severity, func.count(Alert.id))
        .filter(Alert.sent_at >= cutoff)
        .group_by(Alert.severity)
        .all()
    )
    counts = {"INFO": 0, "WARNING": 0, "CRITICAL": 0}
    for sev, n in rows:
        counts[sev] = n
    counts["total"] = sum(v for k, v in counts.items() if k != "total")
    return counts


def quota_utilization(session: Session, organization_id: str) -> list[dict]:
    rows = (
        session.query(ResourceFact)
        .filter(ResourceFact.organization_id == organization_id)
        .filter(ResourceFact.resource_type == "quota")
        .all()
    )
    out = []
    for r in rows:
        raw = _load_raw(r)
        out.append(
            {
                "id": raw.get("id") or r.resource_id,
                "display_name": raw.get("display_name") or r.resource_id,
                "utilization_pct": round(r.value, 1),
                "applied_limit": raw.get("applied_limit"),
                "usage": raw.get("usage"),
            }
        )
    return sorted(out, key=lambda x: x["utilization_pct"], reverse=True)


def cluster_health(session: Session, organization_id: str) -> list[dict]:
    rows = (
        session.query(ResourceFact)
        .filter(ResourceFact.organization_id == organization_id)
        .filter(ResourceFact.resource_type == "kafka_cluster")
        .all()
    )
    out = []
    for r in rows:
        raw = _load_raw(r)
        out.append(
            {
                "resource_id": r.resource_id,
                "display_name": raw.get("display_name"),
                "kind": raw.get("cluster_kind"),
                "cloud": raw.get("cloud"),
                "region": raw.get("region"),
                "cku": raw.get("cku"),
                "phase": raw.get("phase"),
                "environment_id": raw.get("environment_id"),
            }
        )
    return out


def flink_pool_health(session: Session, organization_id: str) -> list[dict]:
    rows = (
        session.query(ResourceFact)
        .filter(ResourceFact.organization_id == organization_id)
        .filter(ResourceFact.resource_type == "flink_compute_pool")
        .all()
    )
    out = []
    for r in rows:
        raw = _load_raw(r)
        out.append(
            {
                "resource_id": r.resource_id,
                "display_name": raw.get("display_name"),
                "cloud": raw.get("cloud"),
                "region": raw.get("region"),
                "max_cfu": raw.get("max_cfu"),
                "current_cfu": raw.get("current_cfu"),
                "phase": raw.get("phase"),
                "environment_id": raw.get("environment_id"),
            }
        )
    return out


def connector_health(session: Session, organization_id: str) -> list[dict]:
    rows = (
        session.query(ResourceFact)
        .filter(ResourceFact.organization_id == organization_id)
        .filter(ResourceFact.resource_type == "connector")
        .all()
    )
    out = []
    for r in rows:
        raw = _load_raw(r)
        out.append(
            {
                "resource_id": r.resource_id,
                "connector_name": raw.get("connector_name"),
                "state": raw.get("state"),
                "task_count": raw.get("task_count"),
                "failed_task_count": raw.get("failed_task_count"),
                "environment_id": raw.get("environment_id"),
                "cluster_id": raw.get("cluster_id"),
            }
        )
    return out


def full_snapshot(session: Session, organization_id: str, budget_usd: float) -> dict:
    """Everything the dashboard needs, in one call."""
    return {
        "summary": billing_summary(session, organization_id, budget_usd),
        "alert_counts": alert_counts_last_24h(session),
        "cost_by_day": cost_by_day(session, organization_id),
        "cost_by_resource": cost_by_resource(session, organization_id),
        "cost_by_product": cost_by_product(session, organization_id),
        "alerts": recent_alerts(session),
        "quotas": quota_utilization(session, organization_id),
        "clusters": cluster_health(session, organization_id),
        "flink_pools": flink_pool_health(session, organization_id),
        "connectors": connector_health(session, organization_id),
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
