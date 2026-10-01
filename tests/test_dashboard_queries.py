import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dashboard import queries
from rules_engine.engine import CandidateAlert
from storage.models import Base
from storage.repository import (
    record_alert,
    replace_cost_snapshot,
    replace_resource_snapshot,
)

ORG = "org-test"


@pytest.fixture()
def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    s = Session()
    yield s
    s.close()


COST_ITEMS = [
    {"start_date": "2026-09-01", "amount": 100.0, "product": "KAFKA", "resource": {"id": "lkc-1", "environment": {"id": "env-1"}}},
    {"start_date": "2026-09-02", "amount": 50.0, "product": "KAFKA", "resource": {"id": "lkc-1", "environment": {"id": "env-1"}}},
    {"start_date": "2026-09-02", "amount": 25.0, "product": "CONNECT", "resource": {"id": "lkc-2", "environment": {"id": "env-1"}}},
]

CLUSTERS = [
    {"resource_id": "lkc-1", "display_name": "prod", "cluster_kind": "Dedicated", "cloud": "AWS",
     "region": "us-east-1", "cku": 2, "phase": "PROVISIONED", "environment_id": "env-1"},
]
CONNECTORS = [
    {"resource_id": "lkc-1/sink", "connector_name": "sink", "state": "FAILED", "task_count": 2,
     "failed_task_count": 1, "environment_id": "env-1", "cluster_id": "lkc-1"},
]
QUOTAS = [
    {"id": "q1", "display_name": "Partitions", "applied_limit": 100, "usage": 95, "_utilization_pct": 95.0},
]


def test_cost_snapshot_and_summary(session):
    replace_cost_snapshot(session, ORG, COST_ITEMS)
    summary = queries.billing_summary(session, ORG, budget_usd=1000.0)
    assert summary["mtd_spend_usd"] == 175.0
    assert summary["pct_of_budget"] == 17.5
    assert summary["data_freshness"] == "accrued_estimate"


def test_cost_snapshot_is_replaced_not_appended(session):
    replace_cost_snapshot(session, ORG, COST_ITEMS)
    replace_cost_snapshot(session, ORG, COST_ITEMS)  # second cycle
    # Should still total 175, not 350 — snapshot semantics
    assert queries.billing_summary(session, ORG, 1000.0)["mtd_spend_usd"] == 175.0


def test_cost_by_day(session):
    replace_cost_snapshot(session, ORG, COST_ITEMS)
    by_day = queries.cost_by_day(session, ORG)
    assert by_day == [{"date": "2026-09-01", "amount": 100.0}, {"date": "2026-09-02", "amount": 75.0}]


def test_cost_by_resource_sorted_desc(session):
    replace_cost_snapshot(session, ORG, COST_ITEMS)
    rows = queries.cost_by_resource(session, ORG)
    assert rows[0] == {"resource_id": "lkc-1", "amount": 150.0}


def test_resource_snapshot_and_health(session):
    replace_resource_snapshot(session, ORG, CLUSTERS, CONNECTORS, QUOTAS)
    clusters = queries.cluster_health(session, ORG)
    assert clusters[0]["kind"] == "Dedicated"
    assert clusters[0]["phase"] == "PROVISIONED"
    connectors = queries.connector_health(session, ORG)
    assert connectors[0]["state"] == "FAILED"
    quotas = queries.quota_utilization(session, ORG)
    assert quotas[0]["utilization_pct"] == 95.0


def test_alerts_recorded_and_counted(session):
    candidate = CandidateAlert(
        rule_name="monthly_budget_threshold", category="billing", severity="WARNING",
        resource_id=None, environment_id=None, current_value=85.0, threshold=80.0,
        message="over budget", observed_at=dt.datetime.now(dt.timezone.utc), cooldown_minutes=60,
    )
    record_alert(session, candidate, {"organization_id": ORG}, delivery_status="dry_run")
    assert len(queries.recent_alerts(session)) == 1
    counts = queries.alert_counts_last_24h(session)
    assert counts["WARNING"] == 1
    assert counts["total"] == 1


def test_full_snapshot_shape(session):
    replace_cost_snapshot(session, ORG, COST_ITEMS)
    replace_resource_snapshot(session, ORG, CLUSTERS, CONNECTORS, QUOTAS)
    snap = queries.full_snapshot(session, ORG, 1000.0)
    for key in ["summary", "alert_counts", "cost_by_day", "cost_by_resource",
                "cost_by_product", "alerts", "quotas", "clusters", "connectors", "generated_at"]:
        assert key in snap


def test_normalize_cost_items_handles_legacy_and_current_env_formats(session):
    from normalizer.normalize import normalize_cost_items
    from storage.models import CostFact
    current = {"start_date": "2026-09-01", "amount": 10.0, "product": "KAFKA",
               "resource": {"id": "lkc-1", "environment": {"id": "env-new"}}}
    legacy = {"start_date": "2022-10-12", "amount": 20.0, "product": "KAFKA",
              "environment": {"id": "env-old"},
              "resource": {"id": "lkc-2", "environment": "string"}}  # legacy: string, not object
    facts = normalize_cost_items([current, legacy], "org-x")  # must not raise
    session.add_all(facts)
    session.commit()
    rows = {f.resource_id: f.environment_id for f in session.query(CostFact).all()}
    assert rows["lkc-1"] == "env-new"   # current format
    assert rows["lkc-2"] == "env-old"   # legacy format (previously an AttributeError)
