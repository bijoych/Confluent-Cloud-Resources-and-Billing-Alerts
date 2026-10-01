"""Tiny shared config loader with ${VAR} substitution.

Extracted so the dashboard can read config.yaml without importing the full
collector/Kafka stack that scheduler.main pulls in.
"""
from __future__ import annotations

import logging
import os

import yaml

logger = logging.getLogger(__name__)

DEFAULT_MONTHLY_BUDGET_USD = 1000.0


def load_config(path: str = "config/config.yaml") -> dict:
    with open(path) as f:
        raw = f.read()
    for key, val in os.environ.items():
        raw = raw.replace(f"${{{key}}}", val)
    return yaml.safe_load(raw)


def get_monthly_budget(config: dict) -> float:
    """Resolve the monthly budget in USD.

    Precedence: the MONTHLY_BUDGET_USD environment variable (set in
    secrets.env) overrides the config.yaml default, so the budget can be
    changed without editing config. Falls back to the config value, then to
    DEFAULT_MONTHLY_BUDGET_USD.
    """
    env_val = os.environ.get("MONTHLY_BUDGET_USD", "").strip()
    if env_val:
        try:
            return float(env_val)
        except ValueError:
            logger.warning(
                "MONTHLY_BUDGET_USD=%r is not a number; falling back to config.yaml", env_val
            )
    try:
        return float((config.get("budget") or {}).get("monthly_budget_usd", DEFAULT_MONTHLY_BUDGET_USD))
    except (TypeError, ValueError):
        return DEFAULT_MONTHLY_BUDGET_USD
