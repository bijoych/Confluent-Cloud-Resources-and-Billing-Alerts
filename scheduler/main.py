"""Orchestration entrypoint.

    python -m scheduler.main --once --dry-run     # single pass, log only
    python -m scheduler.main --once                # single pass, real sends
    python -m scheduler.main                        # loop forever on config intervals

Design: each polling cycle collects fresh facts from every configured data
source, assembles the `facts` dict
the rules engine expects, evaluates rules, deduplicates, and dispatches
notifications. State (dedup cooldowns, historical facts for daily summary)
lives in Postgres/SQLite via storage/.
"""
from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import time

from dotenv import load_dotenv

from collectors.billing_collector import (
    collect_month_to_date_costs,
    project_month_end_spend,
    summarize_by_day,
    summarize_total_amount,
)
from collectors.cloud_resources_collector import (
    collect_clusters,
    collect_connectors,
    collect_environments,
    collect_flink_compute_pools,
)
from collectors.confluent_client import ApiCredentials, ConfluentCloudClient
from collectors.metrics_collector import (
    collect_cluster_utilization,
    collect_throughput_for_idle_check,
    resolve_available_metric_names,
)
from collectors.quota_collector import collect_applied_quotas, compute_utilization_pct
from dedup.dedup import filter_suppressed
from notifiers.email_notifier import SmtpConfig, send_email_alert
from notifiers.payload import build_alert_payload
from notifiers.slack_notifier import send_slack_alert
from notifiers.webhook_notifier import send_webhook_alert
from rules_engine.engine import evaluate_rules, load_rules
from storage.db import get_session, init_db
from storage.repository import (
    record_alert,
    replace_cost_snapshot,
    replace_resource_snapshot,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("scheduler")


from config_loader import get_monthly_budget, load_config  # noqa: E402  (re-exported for back-compat)


def build_client() -> ConfluentCloudClient:
    from client_factory import build_client as _build

    return _build()


def gather_facts(client: ConfluentCloudClient, config: dict):
    """Returns (facts, bundle).

    `facts` is the structure the rules engine consumes. `bundle` holds the
    raw collected objects (cost items, clusters, connectors, quotas) so the
    scheduler can persist a snapshot for the dashboard without re-fetching.
    """
    org_id = config["organization"]["id"]
    env_include = config["organization"].get("environment_ids") or None
    env_exclude = config["organization"].get("exclude_environment_ids") or None

    facts: dict = {"organization": {}, "per_quota": {}, "per_resource": {}}
    bundle: dict = {"cost_items": [], "clusters": [], "connectors": [], "quotas": [], "flink_pools": []}

    # --- Billing ---
    try:
        cost_items = collect_month_to_date_costs(client)
        bundle["cost_items"] = cost_items
        mtd_total = summarize_total_amount(cost_items)
        budget = get_monthly_budget(config)
        forecast = project_month_end_spend(mtd_total)
        by_day = summarize_by_day(cost_items)
        sorted_days = sorted(by_day.items())
        yesterday_spend = sorted_days[-1][1] if sorted_days else 0.0
        trailing = [amt for _, amt in sorted_days[-8:-1]]
        trailing_avg = sum(trailing) / len(trailing) if trailing else 0.0

        facts["organization"].update(
            {
                "mtd_spend_usd": mtd_total,
                "mtd_spend_pct_of_budget": (mtd_total / budget * 100.0) if budget else 0.0,
                "budget_usd": budget,
                "forecast_usd": forecast,
                "forecast_pct_of_budget": (forecast / budget * 100.0) if budget else 0.0,
                "yesterday_spend_usd": yesterday_spend,
                "trailing_avg_usd": trailing_avg,
                "daily_spend_ratio_to_trailing_avg": (
                    yesterday_spend / trailing_avg if trailing_avg else 0.0
                ),
            }
        )
    except Exception:
        logger.exception("Billing collection failed; skipping billing facts this cycle")

    # --- Quotas ---
    try:
        for quota in collect_applied_quotas(client, scope="organization"):
            pct = compute_utilization_pct(quota)
            if pct is not None:
                facts["per_quota"][quota.get("id", "unknown")] = {"quota_utilization_pct": pct}
                # stash the computed pct so the dashboard snapshot can store it
                quota["_utilization_pct"] = pct
                quota["scope"] = "organization"
                bundle["quotas"].append(quota)
    except Exception:
        logger.exception("Quota collection failed; skipping quota facts this cycle")

    # --- Resources: environments -> clusters -> connectors ---
    try:
        environments = collect_environments(
            client, include_ids=env_include, exclude_ids=env_exclude
        )
        available_metrics = resolve_available_metric_names(client)
        for env in environments:
            env_id = env.get("id")
            clusters = collect_clusters(client, env_id)
            bundle["clusters"].extend(clusters)
            cluster_ids = [c["resource_id"] for c in clusters]

            flink_pools = collect_flink_compute_pools(client, env_id)
            bundle["flink_pools"].extend(flink_pools)
            for pool in flink_pools:
                pid = pool["resource_id"]
                facts["per_resource"].setdefault(pid, {})
                facts["per_resource"][pid].update(
                    {
                        "environment_id": env_id,
                        # A provisioned pool with zero CFUs in use is a candidate
                        # idle/cost-without-use resource.
                        "flink_pool_idle": 1 if (pool.get("phase") == "PROVISIONED" and (pool.get("current_cfu") or 0) == 0) else 0,
                    }
                )

            utilization = collect_cluster_utilization(
                client, cluster_ids, available_metrics=available_metrics
            )
            throughput = collect_throughput_for_idle_check(
                client, cluster_ids, available_metrics=available_metrics
            )

            for c in clusters:
                cid = c["resource_id"]
                util_points = utilization.get(cid, [])
                latest_util = util_points[-1]["value"] if util_points else None
                total_throughput = throughput.get(cid, 0.0)
                resource_health_cfg = config["resource_health"]

                facts["per_resource"].setdefault(cid, {})
                facts["per_resource"][cid].update({"environment_id": env_id})
                if latest_util is not None:
                    facts["per_resource"][cid]["cluster_utilization_pct"] = latest_util
                if c.get("amount_incurred", 0) or True:
                    idle_threshold = resource_health_cfg["idle_throughput_bytes_threshold"]
                    facts["per_resource"][cid]["idle_with_nonzero_cost"] = (
                        1 if total_throughput <= idle_threshold else 0
                    )

                connectors = collect_connectors(client, env_id, cid)
                bundle["connectors"].extend(connectors)
                for conn in connectors:
                    conn_id = conn["resource_id"]
                    facts["per_resource"].setdefault(conn_id, {})
                    facts["per_resource"][conn_id].update(
                        {
                            "environment_id": env_id,
                            "connector_failed": 1 if (conn.get("state") == "FAILED" or conn.get("failed_task_count")) else 0,
                        }
                    )
    except Exception:
        logger.exception("Resource collection failed; skipping resource facts this cycle")

    return facts, bundle


def dispatch(payload: dict, config: dict, dry_run: bool) -> None:
    if dry_run:
        logger.info("[DRY RUN] Would send alert: %s", payload)
        return

    notifiers_cfg = config.get("notifiers", {})
    if notifiers_cfg.get("slack", {}).get("enabled"):
        send_slack_alert(os.environ.get("SLACK_WEBHOOK_URL", ""), payload)
    if notifiers_cfg.get("email", {}).get("enabled"):
        smtp_cfg = SmtpConfig(
            host=os.environ.get("SMTP_HOST", ""),
            port=int(os.environ.get("SMTP_PORT", "587")),
            username=os.environ.get("SMTP_USERNAME", ""),
            password=os.environ.get("SMTP_PASSWORD", ""),
            from_addr=os.environ.get("ALERT_EMAIL_FROM", ""),
            to_addrs=[a.strip() for a in os.environ.get("ALERT_EMAIL_TO", "").split(",") if a.strip()],
        )
        send_email_alert(smtp_cfg, payload)
    if notifiers_cfg.get("generic_webhook", {}).get("enabled"):
        send_webhook_alert(os.environ.get("GENERIC_WEBHOOK_URL", ""), payload)


def run_once(config: dict, dry_run: bool) -> int:
    client = build_client()
    facts, bundle = gather_facts(client, config)

    org_id = config["organization"]["id"]

    # Persist a fresh snapshot so the dashboard reflects this cycle. Done
    # regardless of --dry-run: the dashboard should show current state even
    # when we're only validating thresholds and not sending anything.
    session = get_session()
    try:
        try:
            replace_cost_snapshot(session, org_id, bundle["cost_items"])
            replace_resource_snapshot(
                session,
                org_id,
                bundle["clusters"],
                bundle["connectors"],
                bundle["quotas"],
                flink_pools=bundle["flink_pools"],
            )
        except Exception:
            session.rollback()
            logger.exception("Failed to persist snapshot for dashboard")

        # Warm the cost-history cache so the dashboard's spend-trend serves it
        # from the DB (≈1 live Costs API call) instead of bursting 6+ calls per
        # cold load and tripping the rate limit. Past months are cached final,
        # so after the first cycle this refetches only the current month.
        try:
            from dashboard.cost_history import get_trailing_months_series

            get_trailing_months_series(session, client, org_id, n_months=12)
        except Exception:
            logger.exception("Failed to warm cost-history cache")

        rules = load_rules()
        candidates = evaluate_rules(facts, rules=rules)
        surviving = filter_suppressed(session, candidates)

        for candidate in surviving:
            payload = build_alert_payload(candidate, organization_id=org_id)
            dispatch(payload, config, dry_run=dry_run)
            record_alert(
                session,
                candidate,
                payload,
                delivery_status="dry_run" if dry_run else "sent",
            )
    finally:
        session.close()

    logger.info(
        "Cycle complete: %d candidate alert(s), %d after dedup", len(candidates), len(surviving)
    )
    return len(surviving)


def main() -> None:
    parser = argparse.ArgumentParser(description="Confluent Cloud billing/resource alerting POC")
    parser.add_argument("--once", action="store_true", help="Run a single collection+eval cycle and exit")
    parser.add_argument("--dry-run", action="store_true", help="Log alerts instead of sending them")
    parser.add_argument("--config", default="config/config.yaml")
    parser.add_argument("--env-file", default="config/secrets.env")
    parser.add_argument(
        "--loop-interval-minutes",
        type=int,
        default=15,
        help="Sleep between cycles when not using --once (a real deployment should "
        "instead run each collector on its own configured interval; this loop "
        "mode is a simple POC convenience).",
    )
    args = parser.parse_args()

    load_dotenv(args.env_file)
    init_db()
    config = load_config(args.config)

    if args.once:
        run_once(config, dry_run=args.dry_run)
        return

    logger.info("Starting continuous loop (interval=%d min). Ctrl+C to stop.", args.loop_interval_minutes)
    while True:
        try:
            run_once(config, dry_run=args.dry_run)
        except Exception:
            logger.exception("Cycle failed; will retry next interval")
        time.sleep(args.loop_interval_minutes * 60)


if __name__ == "__main__":
    main()
