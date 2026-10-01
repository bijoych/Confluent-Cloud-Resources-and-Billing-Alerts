import datetime as dt

from rules_engine.engine import evaluate_rules

NOW = dt.datetime(2026, 9, 11, tzinfo=dt.timezone.utc)

MINIMAL_RULES = [
    {
        "name": "monthly_budget_threshold",
        "category": "billing",
        "metric": "mtd_spend_pct_of_budget",
        "scope": "organization",
        "severity_thresholds": [
            {"value": 50, "severity": "INFO"},
            {"value": 80, "severity": "WARNING"},
            {"value": 100, "severity": "CRITICAL"},
        ],
        "message_template": "MTD spend is {current_value:.1f}% of budget (${budget_usd}).",
        "cooldown_minutes": 360,
    },
    {
        "name": "quota_utilization",
        "category": "capacity_and_quotas",
        "metric": "quota_utilization_pct",
        "scope": "per_quota",
        "severity_thresholds": [
            {"value": 50, "severity": "INFO"},
            {"value": 90, "severity": "WARNING"},
        ],
        "message_template": "Quota {resource_id} at {current_value:.1f}%.",
        "cooldown_minutes": 240,
    },
    {
        "name": "connector_failed",
        "category": "resource_utilization",
        "metric": "connector_failed",
        "scope": "per_resource",
        "severity_thresholds": [{"value": 1, "severity": "CRITICAL"}],
        "message_template": "Connector {resource_id} failed.",
        "cooldown_minutes": 30,
    },
]


def test_organization_scope_no_breach_below_lowest_threshold():
    facts = {"organization": {"mtd_spend_pct_of_budget": 10, "budget_usd": 1000}}
    result = evaluate_rules(facts, rules=MINIMAL_RULES, now=NOW)
    assert result == []


def test_organization_scope_picks_highest_breached_severity():
    facts = {"organization": {"mtd_spend_pct_of_budget": 85, "budget_usd": 1000}}
    result = evaluate_rules(facts, rules=MINIMAL_RULES, now=NOW)
    assert len(result) == 1
    assert result[0].severity == "WARNING"
    assert result[0].rule_name == "monthly_budget_threshold"


def test_organization_scope_critical_breach():
    facts = {"organization": {"mtd_spend_pct_of_budget": 120, "budget_usd": 1000}}
    result = evaluate_rules(facts, rules=MINIMAL_RULES, now=NOW)
    assert result[0].severity == "CRITICAL"


def test_per_quota_scope_multiple_quotas():
    facts = {
        "per_quota": {
            "quota-a": {"quota_utilization_pct": 30},
            "quota-b": {"quota_utilization_pct": 95},
        }
    }
    result = evaluate_rules(facts, rules=MINIMAL_RULES, now=NOW)
    assert len(result) == 1
    assert result[0].resource_id == "quota-b"
    assert result[0].severity == "WARNING"


def test_per_resource_scope_connector_failed():
    facts = {"per_resource": {"lkc-123/my-connector": {"connector_failed": 1, "environment_id": "env-1"}}}
    result = evaluate_rules(facts, rules=MINIMAL_RULES, now=NOW)
    assert len(result) == 1
    assert result[0].resource_id == "lkc-123/my-connector"
    assert result[0].environment_id == "env-1"
    assert result[0].severity == "CRITICAL"


def test_missing_metric_is_skipped_not_errored():
    facts = {"per_resource": {"lkc-123": {"some_other_metric": 1}}}
    result = evaluate_rules(facts, rules=MINIMAL_RULES, now=NOW)
    assert result == []


def test_default_rules_yaml_loads_and_evaluates():
    facts = {
        "organization": {
            "mtd_spend_pct_of_budget": 55,
            "budget_usd": 1000,
            "mtd_spend_usd": 550,
            "forecast_pct_of_budget": 40,
            "forecast_usd": 400,
            "daily_spend_ratio_to_trailing_avg": 1.0,
            "yesterday_spend_usd": 20,
            "trailing_avg_usd": 20,
        },
        "per_quota": {},
        "per_resource": {},
    }
    result = evaluate_rules(facts, now=NOW)  # loads rules_engine/rules.yaml
    names = {c.rule_name for c in result}
    assert "monthly_budget_threshold" in names
