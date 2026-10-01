import datetime as dt

from collectors.billing_collector import (
    project_month_end_spend,
    summarize_by_day,
    summarize_by_resource,
    summarize_total_amount,
)

SAMPLE_COST_ITEMS = [
    {"start_date": "2026-09-01", "amount": 10.0, "resource": {"id": "lkc-1"}},
    {"start_date": "2026-09-02", "amount": 15.0, "resource": {"id": "lkc-1"}},
    {"start_date": "2026-09-02", "amount": 5.0, "resource": {"id": "lkc-2"}},
]


def test_summarize_total_amount():
    assert summarize_total_amount(SAMPLE_COST_ITEMS) == 30.0


def test_summarize_total_amount_handles_missing_amount():
    items = [{"amount": None}, {"amount": 5.0}]
    assert summarize_total_amount(items) == 5.0


def test_summarize_by_resource():
    result = summarize_by_resource(SAMPLE_COST_ITEMS)
    assert result == {"lkc-1": 25.0, "lkc-2": 5.0}


def test_summarize_by_day():
    result = summarize_by_day(SAMPLE_COST_ITEMS)
    assert result == {"2026-09-01": 10.0, "2026-09-02": 20.0}


def test_project_month_end_spend_linear():
    # 100 spent over 10 days elapsed in a 30-day month -> 300 projected
    today = dt.date(2026, 9, 10)
    result = project_month_end_spend(100.0, today=today)
    assert round(result, 2) == 300.0


def test_summarize_by_day_falls_back_to_start_date_for_perday_sources():
    # Rows with genuine per-day start_dates bucket correctly.
    items = [
        {"start_date": "2026-09-01", "amount": 5.0},
        {"start_date": "2026-09-02", "amount": 7.0},
    ]
    assert summarize_by_day(items) == {"2026-09-01": 5.0, "2026-09-02": 7.0}
