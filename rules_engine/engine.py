"""Evaluates declarative rules (rules.yaml) against a "facts" structure
assembled per polling cycle by the scheduler.

Facts shape (see scheduler/main.py for how this is built):

    {
        "organization": {"mtd_spend_pct_of_budget": 62.0, "mtd_spend_usd": 620.0,
                          "forecast_pct_of_budget": 95.0, "forecast_usd": 950.0,
                          "daily_spend_ratio_to_trailing_avg": 1.1,
                          "yesterday_spend_usd": ..., "trailing_avg_usd": ...,
                          "budget_usd": 1000.0},
        "per_quota": {"quota-id-1": {"quota_utilization_pct": 55.0}, ...},
        "per_resource": {"lkc-xxxxx": {"connector_failed": 0, "cluster_utilization_pct": 91.0,
                                        "idle_with_nonzero_cost": 1,
                                        "resource_disappeared": 0,
                                        "environment_id": "env-xxxxx"}, ...},
    }

This module has no knowledge of Confluent API response shapes — that
translation lives in normalizer/. Keeping the engine generic makes it easy
to unit test with synthetic facts (see tests/test_rules_engine.py).
"""
from __future__ import annotations

import dataclasses
import datetime as dt
import pathlib
from typing import Any

import yaml

DEFAULT_RULES_PATH = pathlib.Path(__file__).parent / "rules.yaml"


@dataclasses.dataclass
class CandidateAlert:
    rule_name: str
    category: str
    severity: str
    resource_id: str | None
    environment_id: str | None
    current_value: float
    threshold: float
    message: str
    observed_at: dt.datetime
    cooldown_minutes: int


def load_rules(path: str | pathlib.Path = DEFAULT_RULES_PATH) -> list[dict]:
    with pathlib.Path(path).open() as f:
        doc = yaml.safe_load(f)
    return doc.get("rules", [])


def _highest_breached_threshold(value: float, severity_thresholds: list[dict]) -> dict | None:
    """Returns the highest-severity threshold entry that `value` meets or
    exceeds, or None if it doesn't clear the lowest threshold.
    """
    breached = [t for t in severity_thresholds if value >= t["value"]]
    if not breached:
        return None
    return max(breached, key=lambda t: t["value"])


def evaluate_rules(
    facts: dict[str, Any],
    rules: list[dict] | None = None,
    default_cooldown_minutes: int = 60,
    now: dt.datetime | None = None,
) -> list[CandidateAlert]:
    rules = rules if rules is not None else load_rules()
    now = now or dt.datetime.now(dt.timezone.utc)
    candidates: list[CandidateAlert] = []

    for rule in rules:
        scope = rule["scope"]
        metric = rule["metric"]

        if scope == "organization":
            org_facts = facts.get("organization", {})
            value = org_facts.get(metric)
            if value is None:
                continue
            breach = _highest_breached_threshold(value, rule["severity_thresholds"])
            if breach is None:
                continue
            candidates.append(
                _build_candidate(rule, breach, value, resource_id=None, ctx=org_facts, now=now)
            )

        elif scope == "per_quota":
            for quota_id, quota_facts in facts.get("per_quota", {}).items():
                value = quota_facts.get(metric)
                if value is None:
                    continue
                breach = _highest_breached_threshold(value, rule["severity_thresholds"])
                if breach is None:
                    continue
                ctx = {**quota_facts, "resource_id": quota_id}
                candidates.append(
                    _build_candidate(rule, breach, value, resource_id=quota_id, ctx=ctx, now=now)
                )

        elif scope == "per_resource":
            for resource_id, res_facts in facts.get("per_resource", {}).items():
                value = res_facts.get(metric)
                if value is None:
                    continue
                breach = _highest_breached_threshold(value, rule["severity_thresholds"])
                if breach is None:
                    continue
                ctx = {**res_facts, "resource_id": resource_id}
                candidates.append(
                    _build_candidate(
                        rule,
                        breach,
                        value,
                        resource_id=resource_id,
                        environment_id=res_facts.get("environment_id"),
                        ctx=ctx,
                        now=now,
                    )
                )
        else:
            raise ValueError(f"Unknown rule scope: {scope!r} in rule {rule['name']!r}")

    return candidates


def _build_candidate(
    rule: dict,
    breach: dict,
    value: float,
    resource_id: str | None,
    ctx: dict,
    now: dt.datetime,
    environment_id: str | None = None,
) -> CandidateAlert:
    message = rule["message_template"].format(current_value=value, **ctx)
    return CandidateAlert(
        rule_name=rule["name"],
        category=rule["category"],
        severity=breach["severity"],
        resource_id=resource_id,
        environment_id=environment_id,
        current_value=float(value),
        threshold=float(breach["value"]),
        message=message,
        observed_at=now,
        cooldown_minutes=rule.get("cooldown_minutes", 60),
    )
