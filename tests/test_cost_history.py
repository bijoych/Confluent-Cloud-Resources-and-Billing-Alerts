import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from dashboard import cost_history as ch
from storage.models import Base, CostPeriod

ORG = "org-test"


@pytest.fixture()
def session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True)
    s = Session()
    yield s
    s.close()


class CountingClient:
    """Returns one line item of $10 per call and counts calls."""
    def __init__(self):
        self.calls = []

    def list_costs(self, start, end):
        self.calls.append((start, end))
        return [{"start_date": start, "end_date": end, "amount": 10.0}]


class OutOfRangeClient:
    """Raises a 400 'too far in the past' for months before a cutoff,
    returns $10 otherwise — mimics an org with limited cost history."""
    def __init__(self, cutoff="2026-06-01"):
        self.cutoff = cutoff
        self.calls = []

    def list_costs(self, start, end):
        self.calls.append((start, end))
        if start < self.cutoff:
            from collectors.confluent_client import ConfluentApiError
            raise ConfluentApiError(400, "url", '{"detail":"start date is too far in the past"}')
        return [{"amount": 10.0, "start_date": start, "end_date": end}]


def test_month_bounds_normal_and_december():
    assert ch._month_bounds(2024, 3) == (dt.date(2024, 3, 1), dt.date(2024, 4, 1))
    assert ch._month_bounds(2024, 12) == (dt.date(2024, 12, 1), dt.date(2025, 1, 1))


def test_is_final_true_for_old_period_false_for_recent():
    now = dt.datetime(2026, 9, 17, tzinfo=dt.timezone.utc)
    assert ch._is_final(dt.date(2026, 9, 1), now) is True          # ended well before now
    assert ch._is_final(dt.date(2026, 9, 17), now) is False        # ends today, still settling


def test_shift_month_crosses_year_boundary():
    assert ch._shift_month(2026, 2, -5) == (2025, 9)
    assert ch._shift_month(2026, 1, -1) == (2025, 12)
    assert ch._shift_month(2025, 12, 1) == (2026, 1)


def test_trailing_months_series_last_12(session):
    now = dt.datetime(2026, 2, 10, tzinfo=dt.timezone.utc)
    client = CountingClient()
    result = ch.get_trailing_months_series(session, client, ORG, 12, now)
    assert len(result["series"]) == 12
    assert result["series"][0]["label"] == "Mar 2025"   # 11 months before Feb 2026
    assert result["series"][-1]["label"] == "Feb 2026"
    assert len(client.calls) == 12
    assert result["total"] == 120.0


def test_trailing_months_caches_final_months(session):
    now = dt.datetime(2026, 2, 10, tzinfo=dt.timezone.utc)
    client = CountingClient()
    ch.get_trailing_months_series(session, client, ORG, 6, now)
    first = len(client.calls)
    ch.get_trailing_months_series(session, client, ORG, 6, now)
    # Only the current (non-final) month refetched on the second call.
    assert len(client.calls) == first + 1


def test_unavailable_months_shown_as_gap_not_cached(session):
    now = dt.datetime(2026, 9, 18, tzinfo=dt.timezone.utc)
    client = OutOfRangeClient(cutoff="2026-06-01")
    res = ch.get_trailing_months_series(session, client, ORG, 6, now)  # Apr..Sep
    by = {s["label"]: s for s in res["series"]}
    assert by["Apr 2026"]["amount"] is None and by["Apr 2026"]["available"] is False
    assert by["May 2026"]["amount"] is None and by["May 2026"]["available"] is False
    assert by["Jun 2026"]["amount"] == 10.0 and by["Jun 2026"]["available"] is True
    assert res["total"] == 40.0  # Jun..Sep x $10
    cached_apr = session.query(CostPeriod).filter_by(
        organization_id=ORG, granularity="MONTH", period="2026-04").one_or_none()
    assert cached_apr is None


def test_db_only_makes_no_live_calls_and_gaps_uncached(session):
    now = dt.datetime(2026, 9, 18, tzinfo=dt.timezone.utc)
    client = CountingClient()
    ch.get_trailing_months_series(session, client, ORG, 6, now)  # warm cache
    calls_after_warm = len(client.calls)
    # DB-only (client=None): no live calls; final months from cache, current
    # month (not final -> not cached) shows as a gap, never $0.
    res = ch.get_trailing_months_series(session, None, ORG, 6, now)
    assert len(client.calls) == calls_after_warm
    by = {s["label"]: s for s in res["series"]}
    assert by["Sep 2026"]["amount"] is None and by["Sep 2026"]["available"] is False


def test_db_only_empty_cache_is_all_gaps(session):
    now = dt.datetime(2026, 9, 18, tzinfo=dt.timezone.utc)
    res = ch.get_trailing_months_series(session, None, ORG, 6, now)
    assert all(s["amount"] is None and s["available"] is False for s in res["series"])
    assert res["total"] == 0.0
