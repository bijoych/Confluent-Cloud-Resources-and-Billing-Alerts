import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from collectors.cloud_resources_collector import collect_flink_compute_pools
from dashboard import queries
from storage.models import Base
from storage.repository import replace_resource_snapshot

ORG = "org-test"


class FakeClient:
    def __init__(self, pools):
        self._pools = pools

    def list_flink_compute_pools(self, environment_id):
        return self._pools


RAW_POOLS = [
    {"id": "lfcp-1", "spec": {"display_name": "etl", "cloud": "AWS", "region": "us-east-1", "max_cfu": 10},
     "status": {"phase": "PROVISIONED", "current_cfu": 4}},
    {"id": "lfcp-2", "spec": {"display_name": "idle-pool", "cloud": "GCP", "region": "us-central1", "max_cfu": 5},
     "status": {"phase": "PROVISIONED", "current_cfu": 0}},
]


def test_collect_flink_compute_pools_flattens_fields():
    pools = collect_flink_compute_pools(FakeClient(RAW_POOLS), "env-1")
    assert pools[0]["resource_id"] == "lfcp-1"
    assert pools[0]["max_cfu"] == 10 and pools[0]["current_cfu"] == 4
    assert pools[0]["phase"] == "PROVISIONED"
    assert pools[1]["current_cfu"] == 0


def test_collect_flink_handles_client_error():
    class Boom:
        def list_flink_compute_pools(self, environment_id):
            raise RuntimeError("boom")
    assert collect_flink_compute_pools(Boom(), "env-1") == []


@pytest.fixture()
def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    s = sessionmaker(bind=engine, future=True)()
    yield s
    s.close()


def test_flink_pools_persist_and_query(session):
    pools = collect_flink_compute_pools(FakeClient(RAW_POOLS), "env-1")
    replace_resource_snapshot(session, ORG, [], [], [], flink_pools=pools)
    health = queries.flink_pool_health(session, ORG)
    ids = {p["resource_id"] for p in health}
    assert ids == {"lfcp-1", "lfcp-2"}
    etl = next(p for p in health if p["resource_id"] == "lfcp-1")
    assert etl["current_cfu"] == 4


def test_full_snapshot_includes_flink(session):
    pools = collect_flink_compute_pools(FakeClient(RAW_POOLS), "env-1")
    replace_resource_snapshot(session, ORG, [], [], [], flink_pools=pools)
    snap = queries.full_snapshot(session, ORG, 1000.0)
    assert "flink_pools" in snap and len(snap["flink_pools"]) == 2
