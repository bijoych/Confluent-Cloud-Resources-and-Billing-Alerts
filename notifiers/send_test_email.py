"""Send one test alert email using the SMTP settings in secrets.env.

    python -m notifiers.send_test_email

Reads SMTP_HOST/PORT/USERNAME/PASSWORD and ALERT_EMAIL_FROM/ALERT_EMAIL_TO
from the environment (config/secrets.env), then sends a sample alert through
the same code path the scheduler uses. Prints exactly what failed if it does,
so you can tell an auth problem (535) from an unverified-sender problem (554)
from a connectivity problem (timeout).

Works with any SMTP server; for Amazon SES set:
  SMTP_HOST=email-smtp.<region>.amazonaws.com
  SMTP_PORT=587
  SMTP_USERNAME / SMTP_PASSWORD = your SES SMTP credentials (not AWS keys)
  ALERT_EMAIL_FROM = a verified SES identity
  ALERT_EMAIL_TO   = recipient (must also be verified while SES is in sandbox)
"""
from __future__ import annotations

import datetime as dt
import logging
import os

from dotenv import load_dotenv

from notifiers.email_notifier import SmtpConfig, format_email, send_email_alert

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


def main() -> None:
    load_dotenv(os.environ.get("ENV_FILE", "config/secrets.env"))

    to_addrs = [a.strip() for a in os.environ.get("ALERT_EMAIL_TO", "").split(",") if a.strip()]
    config = SmtpConfig(
        host=os.environ.get("SMTP_HOST", ""),
        port=int(os.environ.get("SMTP_PORT", "587")),
        username=os.environ.get("SMTP_USERNAME", ""),
        password=os.environ.get("SMTP_PASSWORD", ""),
        from_addr=os.environ.get("ALERT_EMAIL_FROM", ""),
        to_addrs=to_addrs,
    )

    missing = [
        name for name, val in [
            ("SMTP_HOST", config.host),
            ("ALERT_EMAIL_FROM", config.from_addr),
            ("ALERT_EMAIL_TO", to_addrs),
        ] if not val
    ]
    if missing:
        print(f"Missing required settings in secrets.env: {', '.join(missing)}")
        return

    payload = {
        "alert_name": "test_email",
        "category": "billing",
        "severity": "INFO",
        "organization_id": os.environ.get("CONFLUENT_ORG_ID", "org-test"),
        "environment_id": "env-test",
        "resource_id": "lkc-test",
        "current_value": 123.45,
        "threshold": 100.00,
        "unit": "USD",
        "message": "This is a test alert email from the Confluent Cloud alerting POC.",
        "observed_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "dashboard_url": "https://example.internal/costs",
        "data_freshness": "accrued_estimate",
    }

    subject, _ = format_email(payload)
    print(f"Sending test email via {config.host}:{config.port}")
    print(f"  From: {config.from_addr}")
    print(f"  To:   {', '.join(to_addrs)}")
    print(f"  Subject: {subject}")

    ok = send_email_alert(config, payload)
    if ok:
        print("\nSent. Check the recipient inbox (and spam). If it doesn't arrive, check the")
        print("SES sending dashboard / CloudWatch for a delivery or bounce event.")
    else:
        print("\nFailed — see the logged error above. Common causes:")
        print("  535 auth        -> wrong SES SMTP username/password (must be SES SMTP creds)")
        print("  554 / rejected  -> From/To not a verified identity, or account still in SES sandbox")
        print("  timeout         -> wrong host/port, region mismatch, or egress blocked on 587")


if __name__ == "__main__":
    main()
