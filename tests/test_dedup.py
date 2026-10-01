import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dedup.dedup import filter_suppressed, make_dedup_key, should_suppress
from rules_engine.engine import CandidateAlert
from storage.models import Base

NOW = dt.datetime(2026, 9, 11, 12, 0, tzinfo=dt.timezone.utc)


@pytest.fixture()
def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    s = Session()
    yield s
    s.close()


def make_candidate(rule_name="rule1", resource_id="lkc-1", severity="WARNING", cooldown=60):
    return CandidateAlert(
        rule_name=rule_name,
        category="resource_utilization",
        severity=severity,
        resource_id=resource_id,
        environment_id="env-1",
        current_value=90.0,
        threshold=80.0,
        message="test",
        observed_at=NOW,
        cooldown_minutes=cooldown,
    )


def test_first_fire_is_not_suppressed(session):
    c = make_candidate()
    assert should_suppress(session, c, now=NOW) is False


def test_second_fire_within_cooldown_is_suppressed(session):
    c = make_candidate(cooldown=60)
    filter_suppressed(session, [c], now=NOW)
    c2 = make_candidate(cooldown=60)
    assert should_suppress(session, c2, now=NOW + dt.timedelta(minutes=10)) is True


def test_fire_after_cooldown_expires_is_not_suppressed(session):
    c = make_candidate(cooldown=60)
    filter_suppressed(session, [c], now=NOW)
    c2 = make_candidate(cooldown=60)
    assert should_suppress(session, c2, now=NOW + dt.timedelta(minutes=61)) is False


def test_escalation_bypasses_cooldown(session):
    c = make_candidate(severity="WARNING", cooldown=60)
    filter_suppressed(session, [c], now=NOW)
    c2 = make_candidate(severity="CRITICAL", cooldown=60)
    assert should_suppress(session, c2, now=NOW + dt.timedelta(minutes=5)) is False


def test_dedup_key_includes_resource_id():
    c = make_candidate(rule_name="r", resource_id="lkc-9")
    assert make_dedup_key(c) == "r:lkc-9"


def test_dedup_key_org_scoped_has_no_resource():
    c = make_candidate(rule_name="r", resource_id=None)
    assert make_dedup_key(c) == "r"
