"""Collects billing facts from the Costs API (billing/v1/costs).

Per Confluent's docs: cost data can lag actual usage by up to 72 hours, and
billing accrues hourly while invoices are generated monthly. Everything
this collector emits should be treated as an accrued estimate, never a
final invoice number — the rules engine and notifiers both label output
accordingly.
"""
from __future__ import annotations

import datetime as dt
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collectors.confluent_client import ConfluentCloudClient

logger = logging.getLogger(__name__)


def collect_month_to_date_costs(client: ConfluentCloudClient, today: dt.date | None = None):
    """Fetches month-to-date cost line items in a SINGLE ranged call.

    This feeds the snapshot's MTD total, cost-by-resource, and cost-by-product
    — all of which aggregate correctly regardless of the per-row date, so one
    call suffices. (Earlier this looped per-day to build a daily series, but
    that's now served separately by the cached dashboard cost-history
    endpoints, so the ~1-call-per-day-of-month volume here just risked
    tripping the Costs API rate limit every cycle.) End date is exclusive, so
    add a day to include today.
    """
    today = today or dt.date.today()
    start = today.replace(day=1)
    return collect_costs_for_range(client, start, today + dt.timedelta(days=1))


def collect_costs_for_range(client: ConfluentCloudClient, start: dt.date, end: dt.date):
    """Single ranged call. Returns rows aggregated over the whole range — use
    this for a range TOTAL, not for a per-day breakdown."""
    return client.list_costs(start.isoformat(), end.isoformat())


def summarize_total_amount(cost_items: list[dict]) -> float:
    return sum(item.get("amount", 0.0) or 0.0 for item in cost_items)


def summarize_by_resource(cost_items: list[dict]) -> dict[str, float]:
    totals: dict[str, float] = {}
    for item in cost_items:
        resource = (item.get("resource") or {}).get("id") or "UNKNOWN"
        totals[resource] = totals.get(resource, 0.0) + (item.get("amount", 0.0) or 0.0)
    return totals


def summarize_by_day(cost_items: list[dict]) -> dict[str, float]:
    """Buckets cost line items by day.

    Buckets rows by `_queried_day` if present, else `start_date`. (A ranged
    Costs API call echoes the request range in start_date rather than a per-day
    value, so a per-day source would set `_queried_day`; the current scheduler
    uses a single ranged month-to-date call, so this mainly groups by start_date.)
    """
    totals: dict[str, float] = {}
    for item in cost_items:
        day = item.get("_queried_day") or item.get("start_date")
        if not day:
            continue
        totals[day] = totals.get(day, 0.0) + (item.get("amount", 0.0) or 0.0)
    return totals


def project_month_end_spend(mtd_total: float, today: dt.date | None = None) -> float:
    """Naive linear projection: (mtd_total / days_elapsed) * days_in_month.

    This is intentionally simple for the POC — see the design doc's note
    that full forecasting is out of the "recommended starting scope".
    """
    today = today or dt.date.today()
    days_elapsed = today.day
    if days_elapsed == 0:
        return mtd_total
    next_month = today.replace(day=28) + dt.timedelta(days=4)
    days_in_month = (next_month.replace(day=1) - dt.timedelta(days=1)).day
    daily_rate = mtd_total / days_elapsed
    return daily_rate * days_in_month
