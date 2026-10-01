"""Turns raw collector dicts into storage.models rows.

Currently only cost line items are normalized here; resource-state snapshots
are built directly in storage/repository.py. Kept as pure functions returning
ORM instances (not yet added to a session) so callers control the transaction
boundary and this stays easy to unit test.
"""
from __future__ import annotations

import json

from storage.models import CostFact


def normalize_cost_items(cost_items: list[dict], organization_id: str) -> list[CostFact]:
    facts = []
    for item in cost_items:
        resource = item.get("resource") or {}
        resource_id = resource.get("id")
        # Environment id location differs by org age (verified against Confluent
        # docs): the current format nests it as an object under
        # resource.environment.id; the legacy (pre-2024-05-15) format puts a
        # string in resource.environment and the environment object at the top
        # level. Handle both, and never call .get() on a string.
        env_under_resource = resource.get("environment")
        if isinstance(env_under_resource, dict):
            environment_id = env_under_resource.get("id")
        else:
            environment_id = (item.get("environment") or {}).get("id")
        facts.append(
            CostFact(
                organization_id=organization_id,
                environment_id=environment_id,
                resource_id=resource_id,
                product=item.get("product"),
                line_type=item.get("line_type"),
                start_date=item.get("_queried_day") or item.get("start_date"),
                end_date=item.get("end_date"),
                granularity=item.get("granularity"),
                quantity=item.get("quantity"),
                unit=item.get("unit"),
                original_amount=item.get("original_amount"),
                discount_amount=item.get("discount_amount"),
                amount=item.get("amount", 0.0) or 0.0,
                is_accrued_estimate=1,
                raw_json=json.dumps(item),
            )
        )
    return facts
