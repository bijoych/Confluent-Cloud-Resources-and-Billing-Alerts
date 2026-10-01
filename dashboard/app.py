"""Confluent Cloud Alerting — dashboard web application.

    python -m dashboard.app                 # http://localhost:5000
    FLASK_PORT=8080 python -m dashboard.app

Serves a single-page dashboard (templates/dashboard.html) plus a small
JSON API that reads the snapshot the scheduler persists each cycle. If you
haven't run the scheduler yet, the panels render empty with a hint to run
`python -m scheduler.main --once --dry-run` first.

Read-only: this app never writes to the database or calls Confluent — it
only visualizes what the collectors already stored.
"""
from __future__ import annotations

import os

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

from config_loader import get_monthly_budget, load_config
from dashboard import cost_history, queries
from storage.db import get_session, init_db

load_dotenv(os.environ.get("ENV_FILE", "config/secrets.env"))

app = Flask(__name__)

_config = load_config(os.environ.get("CONFIG_FILE", "config/config.yaml"))
ORG_ID = _config["organization"]["id"]
BUDGET_USD = get_monthly_budget(_config)

init_db()


@app.route("/")
def index():
    return render_template("dashboard.html", org_id=ORG_ID, budget_usd=BUDGET_USD)


@app.route("/api/snapshot")
def api_snapshot():
    session = get_session()
    try:
        return jsonify(queries.full_snapshot(session, ORG_ID, BUDGET_USD))
    finally:
        session.close()


@app.route("/api/summary")
def api_summary():
    session = get_session()
    try:
        return jsonify(
            {
                "summary": queries.billing_summary(session, ORG_ID, BUDGET_USD),
                "alert_counts": queries.alert_counts_last_24h(session),
            }
        )
    finally:
        session.close()


@app.route("/api/alerts")
def api_alerts():
    session = get_session()
    try:
        return jsonify(queries.recent_alerts(session))
    finally:
        session.close()


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok", "organization_id": ORG_ID})


_client = None
_client_lock = __import__("threading").Lock()


def _db_only_enabled() -> bool:
    return os.environ.get("DASHBOARD_DB_ONLY", "").strip().lower() in ("1", "true", "yes", "on")


def _build_shared_client():
    """Build/reuse a single shared client (throttle state shared across
    requests). Raises if credentials aren't configured."""
    global _client
    if _client is not None:
        return _client
    with _client_lock:
        if _client is None:
            from client_factory import build_client

            _client = build_client()
    return _client


def _client_or_none():
    """Client used by the trend endpoint. By default the trend loads the last
    six months automatically (fetching any uncached months once, then serving
    them from cache — the auto-refresh timer never reloads the trend, so this
    isn't a repeated cost). Set DASHBOARD_DB_ONLY=1 to make the dashboard read
    only the local DB and never call Confluent on its own (populate it with
    `python -m dashboard.sync` or the scheduler; use the Sync button per month).
    """
    if _db_only_enabled():
        return None
    try:
        return _build_shared_client()
    except Exception:
        app.logger.warning("No Confluent credentials available; serving cached cost history only")
        return None


@app.route("/api/costs/trailing")
def api_costs_trailing():
    try:
        months = int(request.args.get("months", "12"))
    except ValueError:
        months = 12
    session = get_session()
    try:
        return jsonify(cost_history.get_trailing_months_series(session, _client_or_none(), ORG_ID, months))
    finally:
        session.close()


def main() -> None:
    port = int(os.environ.get("FLASK_PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=os.environ.get("FLASK_DEBUG") == "1")


if __name__ == "__main__":
    main()
