"""Populate the database with synthetic data so the dashboard can be viewed
without a real Confluent Cloud org or a scheduler run.

    python -m dashboard.seed_demo

Uses the same persistence path as the real scheduler (storage/repository),
so what you see is exactly how real data would render. Safe to re-run — it
replaces the cost/resource snapshot each time.
"""
from __future__ import annotations

import datetime as dt

from config_loader import load_config
from rules_engine.engine import CandidateAlert
from storage.db import get_session, init_db
from storage.repository import (
    record_alert,
    replace_cost_snapshot,
    replace_resource_snapshot,
)



def _month_days(n: int) -> list[str]:
    today = dt.date.today()
    start = today.replace(day=1)
    return [(start + dt.timedelta(days=i)).isoformat() for i in range(min(n, today.day))]


def main() -> None:
    init_db()
    config = load_config()
    org = config["organization"]["id"]

    days = _month_days(17)
    cost_items = []
    for i, day in enumerate(days):
        # Kafka cluster daily cost with a gentle upward drift + one spike.
        # Tuned so month-to-date lands ~82% of a $1,000 budget with the
        # linear forecast crossing the budget — exercising both the budget
        # WARNING and the forecast WARNING for a realistic demo.
        base = 20 + i * 0.6 + (18 if i == len(days) - 2 else 0)
        cost_items.append({"start_date": day, "amount": round(base, 2), "product": "KAFKA",
                           "line_type": "KAFKA_NUM_CKUS", "resource": {"id": "lkc-demo01", "environment": {"id": "env-demo1"}}})
        cost_items.append({"start_date": day, "amount": round(base * 0.35, 2), "product": "CONNECT",
                           "line_type": "CONNECT_NUM_TASKS", "resource": {"id": "lkc-demo01", "environment": {"id": "env-demo1"}}})
        cost_items.append({"start_date": day, "amount": round(base * 0.5, 2), "product": "KAFKA",
                           "line_type": "KAFKA_STORAGE", "resource": {"id": "lkc-demo02", "environment": {"id": "env-demo2"}}})
        cost_items.append({"start_date": day, "amount": round(base * 0.6, 2), "product": "FLINK",
                           "line_type": "FLINK_NUM_CFUS", "resource": {"id": "lfcp-demo01", "environment": {"id": "env-demo1"}}})
        cost_items.append({"start_date": day, "amount": round(base * 0.2, 2), "product": "TABLEFLOW",
                           "line_type": "TABLEFLOW_NUM_GBS", "resource": {"id": "lkc-demo01", "environment": {"id": "env-demo1"}}})

    clusters = [
        {"resource_id": "lkc-demo01", "display_name": "prod-events", "cluster_kind": "Dedicated",
         "cloud": "AWS", "region": "us-east-1", "cku": 2, "phase": "PROVISIONED", "environment_id": "env-demo1"},
        {"resource_id": "lkc-demo02", "display_name": "analytics", "cluster_kind": "Standard",
         "cloud": "GCP", "region": "us-central1", "cku": None, "phase": "PROVISIONED", "environment_id": "env-demo2"},
    ]
    connectors = [
        {"resource_id": "lkc-demo01/s3-sink", "connector_name": "s3-sink", "state": "RUNNING",
         "task_count": 3, "failed_task_count": 0, "environment_id": "env-demo1", "cluster_id": "lkc-demo01"},
        {"resource_id": "lkc-demo01/pg-source", "connector_name": "pg-source", "state": "FAILED",
         "task_count": 2, "failed_task_count": 1, "environment_id": "env-demo1", "cluster_id": "lkc-demo01"},
    ]
    quotas = [
        {"id": "partitions_per_cluster", "display_name": "Partitions per cluster",
         "applied_limit": 4096, "usage": 3980, "_utilization_pct": 97.2, "scope": "kafka_cluster"},
        {"id": "connectors_per_cluster", "display_name": "Connectors per cluster",
         "applied_limit": 50, "usage": 22, "_utilization_pct": 44.0, "scope": "kafka_cluster"},
        {"id": "acls_per_cluster", "display_name": "ACLs per cluster",
         "applied_limit": 1000, "usage": 610, "_utilization_pct": 61.0, "scope": "kafka_cluster"},
    ]
    flink_pools = [
        {"resource_id": "lfcp-demo01", "display_name": "streaming-etl", "cloud": "AWS",
         "region": "us-east-1", "max_cfu": 10, "current_cfu": 4, "phase": "PROVISIONED",
         "environment_id": "env-demo1"},
        {"resource_id": "lfcp-demo02", "display_name": "adhoc-analytics", "cloud": "AWS",
         "region": "us-east-1", "max_cfu": 5, "current_cfu": 0, "phase": "PROVISIONED",
         "environment_id": "env-demo1"},
    ]

    session = get_session()
    try:
        n_cost = replace_cost_snapshot(session, org, cost_items)
        n_res = replace_resource_snapshot(session, org, clusters, connectors, quotas,
                                          flink_pools=flink_pools)

        now = dt.datetime.now(dt.timezone.utc)
        demo_alerts = [
            CandidateAlert("monthly_budget_threshold", "billing", "WARNING", None, None,
                           81.3, 80.0, "Month-to-date accrued spend is 81.3% of the $1,000 monthly budget.", now, 360),
            CandidateAlert("connector_failed", "resource_utilization", "CRITICAL", "lkc-demo01/pg-source", "env-demo1",
                           1.0, 1.0, "Connector lkc-demo01/pg-source is in a FAILED state.", now, 30),
            CandidateAlert("quota_utilization", "capacity_and_quotas", "WARNING", "partitions_per_cluster", None,
                           97.2, 90.0, 'Quota "partitions_per_cluster" is at 97.2% of its applied limit.', now, 240),
            CandidateAlert("flink_pool_idle", "resource_utilization", "WARNING", "lfcp-demo02", "env-demo1",
                           1.0, 1.0, "Flink compute pool lfcp-demo02 is provisioned but using 0 CFUs — it may be incurring cost without active queries.", now, 1440),
        ]
        for c in demo_alerts:
            record_alert(session, c, {"organization_id": org}, delivery_status="dry_run")
    finally:
        session.close()

    print(f"Seeded {n_cost} cost rows, {n_res} resource rows, and {len(demo_alerts)} demo alerts for org {org}.")
    print("Now run:  python -m dashboard.app   and open http://localhost:5000")


if __name__ == "__main__":
    main()
