"""Yearly (per-month) and monthly (per-day) cost series for the dashboard
line chart, with caching of settled periods.

The Costs API aggregates over a requested range, so a month total is one
ranged call (start=1st, end=1st of next month) and a day total is one
ranged call (start=day, end=day+1). We cache each period in `cost_periods`
and only re-query periods that aren't final yet — a period is final once
its window ended more than the Costs API's ~72h lag ago, after which its
cost no longer changes. So a fully past year serves from cache with zero
API calls; the current year re-fetches only the current month (yearly view)
or the last few days (monthly view).
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import TYPE_CHECKING

from sqlalchemy.orm import Session

from collectors.billing_collector import collect_costs_for_range, summarize_total_amount
from storage.models import CostPeriod

if TYPE_CHECKING:
    from collectors.confluent_client import ConfluentCloudClient

logger = logging.getLogger(__name__)

# Costs API can lag actual usage by up to 72h; treat a period as settled a
# little beyond that so we never cache an amount that's still moving.
SETTLE_LAG_DAYS = 4


def _month_bounds(year: int, month: int) -> tuple[dt.date, dt.date]:
    start = dt.date(year, month, 1)
    end = dt.date(year + 1, 1, 1) if month == 12 else dt.date(year, month + 1, 1)
    return start, end


def _is_final(end_exclusive: dt.date, now: dt.datetime | None = None) -> bool:
    now = now or dt.datetime.now(dt.timezone.utc)
    return now.date() >= end_exclusive + dt.timedelta(days=SETTLE_LAG_DAYS)


def _cached(session: Session, org: str, granularity: str, period: str) -> CostPeriod | None:
    return (
        session.query(CostPeriod)
        .filter_by(organization_id=org, granularity=granularity, period=period)
        .one_or_none()
    )


def _upsert(session: Session, org: str, granularity: str, period: str, amount: float, final: bool):
    row = _cached(session, org, granularity, period)
    if row is None:
        row = CostPeriod(
            organization_id=org, granularity=granularity, period=period,
            amount=amount, is_final=1 if final else 0,
        )
        session.add(row)
    else:
        row.amount = amount
        row.is_final = 1 if final else 0
        row.collected_at = dt.datetime.now(dt.timezone.utc)
    session.commit()


def _resolve_period(
    session: Session,
    client: "ConfluentCloudClient | None",
    org: str,
    granularity: str,
    period: str,
    start: dt.date,
    end: dt.date,
    now: dt.datetime | None = None,
) -> tuple[float | None, bool, bool]:
    """Return (amount, final, available) for one period, using cache when final.

    available=False means the Costs API cannot serve this period (a 400, e.g.
    "start date is too far in the past") — we do NOT cache it and the caller
    shows it as a gap, never a misleading $0. A genuine $0 month returns an
    empty cost list -> amount 0.0, available True.
    """
    final = _is_final(end, now)
    cached = _cached(session, org, granularity, period)
    if cached is not None and cached.is_final:
        return cached.amount, True, True
    if client is None:
        # DB-only mode: serve cache; a period that hasn't been synced yet is a
        # gap (not a misleading $0). Run `python -m dashboard.sync` to populate.
        if cached is not None:
            return cached.amount, bool(cached.is_final), True
        return None, False, False
    try:
        items = collect_costs_for_range(client, start, end)
        amount = round(summarize_total_amount(items), 2)
    except Exception as exc:
        status = getattr(exc, "status_code", None)
        if status == 400:
            # The API won't serve this period for this org. Do NOT cache it as
            # $0 — mark unavailable so it shows as a gap and can recover later.
            logger.info("Costs API 400 for %s %s (period not retrievable): %s",
                        granularity, period, getattr(exc, "body", ""))
            return None, False, False
        # Transient (429 exhausted, network) — serve cache, retry next time.
        logger.warning("Costs fetch failed for %s %s: %s", granularity, period, exc)
        return (cached.amount if cached else 0.0), False, True
    _upsert(session, org, granularity, period, amount, final)
    return amount, final, True


_MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                 "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


def _shift_month(year: int, month: int, delta: int) -> tuple[int, int]:
    """Return (year, month) shifted by `delta` months (delta may be negative)."""
    total = year * 12 + (month - 1) + delta
    return total // 12, total % 12 + 1


def get_trailing_months_series(
    session: Session,
    client: "ConfluentCloudClient | None",
    org: str,
    n_months: int = 6,
    now: dt.datetime | None = None,
) -> dict:
    """Rolling last-N-months monthly totals, ending with the current month.

    Cheaper and more relevant than a full calendar year: only N ranged calls
    on first load (default 6), and every month but the current one is final
    and served from cache thereafter.
    """
    now = now or dt.datetime.now(dt.timezone.utc)
    n_months = max(1, min(n_months, 24))
    series = []
    for i in range(n_months - 1, -1, -1):
        yy, mm = _shift_month(now.year, now.month, -i)
        start, end = _month_bounds(yy, mm)
        amount, final, available = _resolve_period(
            session, client, org, "MONTH", f"{yy}-{mm:02d}", start, end, now
        )
        series.append({
            "year": yy, "month": mm,
            "label": f"{_MONTH_LABELS[mm - 1]} {yy}",
            "amount": amount, "final": final, "available": available,
        })
    total = round(sum(s["amount"] or 0.0 for s in series), 2)
    return {"granularity": "MONTH", "months": n_months, "series": series, "total": total}
