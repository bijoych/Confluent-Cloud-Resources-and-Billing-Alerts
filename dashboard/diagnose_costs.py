"""Probe how far back the Costs API will actually serve data for your org.

    python -m dashboard.diagnose_costs [--months 12]

Queries each of the last N months directly against /billing/v1/costs and
reports, per month, whether it returned data, an amount, or an error (e.g. a
400 "start date is too far in the past"). Use this to find the real earliest
retrievable month — which can differ from what the Confluent Cloud Console
invoice view shows, since they're separate backends.
"""
from __future__ import annotations

import argparse
import datetime as dt

from client_factory import build_client
from collectors.billing_collector import summarize_total_amount
from dashboard.cost_history import _month_bounds, _shift_month
from dotenv import load_dotenv


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--env-file", default="config/secrets.env")
    args = parser.parse_args()

    load_dotenv(args.env_file)
    client = build_client()
    now = dt.datetime.now(dt.timezone.utc)

    print(f"Probing the Costs API for the last {args.months} month(s)\n")
    print(f"{'month':<10} {'start_date':<12} {'result'}")
    print("-" * 60)
    earliest_ok = None
    for i in range(args.months - 1, -1, -1):
        yy, mm = _shift_month(now.year, now.month, -i)
        start, end = _month_bounds(yy, mm)
        label = f"{yy}-{mm:02d}"
        try:
            items = client.list_costs(start.isoformat(), end.isoformat())
            amount = round(summarize_total_amount(items), 2)
            print(f"{label:<10} {start.isoformat():<12} OK  ${amount:,.2f}  ({len(items)} line items)")
            earliest_ok = earliest_ok or label
            if earliest_ok is None or label < earliest_ok:
                earliest_ok = label
        except Exception as exc:  # noqa: BLE001
            status = getattr(exc, "status_code", "?")
            detail = getattr(exc, "body", str(exc))
            print(f"{label:<10} {start.isoformat():<12} ERROR {status}: {detail[:120]}")

    print("-" * 60)
    if earliest_ok:
        print(f"Earliest month the Costs API served: {earliest_ok}")
        print("If that's later than what the Console shows, the API's retrievable")
        print("window for this org starts there — set the trend to that many months.")
    else:
        print("No month returned data — check credentials/roles (BillingAdmin) and org id.")


if __name__ == "__main__":
    main()
