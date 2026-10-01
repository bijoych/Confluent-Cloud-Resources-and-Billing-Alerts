"""Populate the local DB with what the dashboard needs, so its first open is
instant (no waiting on live Confluent calls).

    python -m dashboard.sync                 # snapshot + last 12 months (monthly)
    python -m dashboard.sync --months 6

What it populates:
  - cost + resource snapshot and alerts (what /api/snapshot shows)
  - trailing monthly totals for the spend-trend line

Run it once up front, then periodically (or run the scheduler, which also
refreshes the snapshot and monthly trend each cycle). The dashboard also loads
the last 12 months on its own by default; pre-populating here just avoids the
initial fetch. Set DASHBOARD_DB_ONLY=1 to have the dashboard rely solely on
what this command (or the scheduler) has cached.

All calls go through the client's spacing + 429 backoff, and monthly totals are
one call per month (cached as final), so this stays well within the rate limit.
"""
from __future__ import annotations

import argparse
import datetime as dt

from dotenv import load_dotenv

from client_factory import build_client
from config_loader import load_config
from dashboard.cost_history import get_trailing_months_series
from storage.db import get_session, init_db


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=int, default=12, help="How many trailing months to sync")
    parser.add_argument("--env-file", default="config/secrets.env")
    args = parser.parse_args()

    load_dotenv(args.env_file)
    init_db()
    config = load_config()
    org = config["organization"]["id"]

    # 1) Snapshot (cost facts + resource facts) + monthly trend warm + alerts.
    #    dry_run=True: populate the DB and record alerts for display, send none.
    from scheduler.main import run_once

    print("[1/2] Snapshot (costs, resources), monthly trend, and alerts…")
    run_once(config, dry_run=True)

    # 2) Ensure the trailing window the dashboard shows is fully cached.
    client = build_client()
    now = dt.datetime.now(dt.timezone.utc)
    session = get_session()
    try:
        print(f"[2/2] Monthly trend, last {args.months} month(s)…")
        m = get_trailing_months_series(session, client, org, args.months, now)
        available = [s for s in m["series"] if s.get("available")]
        print(f"      {len(available)}/{len(m['series'])} months available, total ${m['total']:,.2f}")
    finally:
        session.close()

    print("\nSync complete. The dashboard serves the trend from the local DB.")


if __name__ == "__main__":
    main()
