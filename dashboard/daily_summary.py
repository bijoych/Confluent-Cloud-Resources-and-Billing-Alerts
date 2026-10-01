"""Generates a daily cost + resource summary, satisfying the POC success
criterion "Provide a daily cost and resource summary."

    python -m dashboard.daily_summary                 # print to stdout
    python -m dashboard.daily_summary --slack          # also post to Slack

Intended to be run once a day (see config.yaml: dashboard.daily_summary_send_hour_utc)
by cron/CronJob, separately from the main alerting loop.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os

from dotenv import load_dotenv
from sqlalchemy import func

from storage.db import get_session
from storage.models import Alert, CostFact


def build_summary(organization_id: str) -> str:
    session = get_session()
    try:
        today = dt.date.today()
        month_start = today.replace(day=1)

        total_mtd = (
            session.query(func.sum(CostFact.amount))
            .filter(CostFact.organization_id == organization_id)
            .filter(CostFact.start_date >= month_start.isoformat())
            .scalar()
            or 0.0
        )

        alerts_last_24h = (
            session.query(Alert)
            .filter(Alert.sent_at >= dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=24))
            .order_by(Alert.severity.desc())
            .all()
        )

        by_severity: dict[str, int] = {}
        for a in alerts_last_24h:
            by_severity[a.severity] = by_severity.get(a.severity, 0) + 1

        lines = [
            f"Daily summary for {organization_id} — {today.isoformat()}",
            f"Month-to-date accrued spend: ${total_mtd:,.2f} (accrued estimate, not final invoice)",
            f"Alerts in last 24h: {sum(by_severity.values())} "
            f"({', '.join(f'{k}: {v}' for k, v in sorted(by_severity.items()))})" if by_severity else "Alerts in last 24h: 0",
        ]
        return "\n".join(lines)
    finally:
        session.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--org-id", default=os.environ.get("CONFLUENT_ORG_ID", ""))
    parser.add_argument("--slack", action="store_true")
    parser.add_argument("--env-file", default="config/secrets.env")
    args = parser.parse_args()

    load_dotenv(args.env_file)
    summary = build_summary(args.org_id)
    print(summary)

    if args.slack:
        from notifiers.slack_notifier import send_slack_alert

        send_slack_alert(
            os.environ.get("SLACK_WEBHOOK_URL", ""),
            {
                "severity": "INFO",
                "alert_name": "daily_summary",
                "category": "summary",
                "message": summary,
                "dashboard_url": "https://example.internal/costs",
            },
        )


if __name__ == "__main__":
    main()
