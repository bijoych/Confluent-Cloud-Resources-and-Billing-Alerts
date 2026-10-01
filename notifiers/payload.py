"""Builds the alert payload dict, matching the shape from the design doc's
"Suggested alert payload" section.
"""
from __future__ import annotations

import datetime as dt

from rules_engine.engine import CandidateAlert


def build_alert_payload(
    candidate: CandidateAlert,
    organization_id: str,
    dashboard_base_url: str = "https://example.internal/costs",
) -> dict:
    return {
        "alert_name": candidate.rule_name,
        "category": candidate.category,
        "severity": candidate.severity,
        "organization_id": organization_id,
        "environment_id": candidate.environment_id,
        "resource_id": candidate.resource_id,
        "current_value": candidate.current_value,
        "threshold": candidate.threshold,
        "message": candidate.message,
        "observed_at": candidate.observed_at.isoformat()
        if isinstance(candidate.observed_at, dt.datetime)
        else candidate.observed_at,
        "dashboard_url": f"{dashboard_base_url}/{organization_id}",
        # Cost/billing-derived alerts are always labeled as such, per the
        # design doc's requirement to never present accrued usage as a
        # final invoice value.
        "data_freshness": "accrued_estimate" if candidate.category in {"billing", "forecasting"} else "live",
    }
