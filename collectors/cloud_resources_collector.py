"""Collects resource-lifecycle facts from the Cloud Management APIs:
environments, Kafka clusters, and connectors + their status.

Emits plain dicts (not ORM objects) so this stays easy to unit test with
mocked HTTP responses. `normalizer/normalize.py` turns these into
`ResourceFact` rows.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collectors.confluent_client import ConfluentCloudClient

logger = logging.getLogger(__name__)


PLACEHOLDER_ENV_PREFIXES = ("env-example",)


def collect_environments(
    client: ConfluentCloudClient,
    include_ids: list[str] | None = None,
    exclude_ids: list[str] | None = None,
):
    """Discovers environments dynamically from the org every call.

    A new environment is picked up automatically — no config change needed.
    `include_ids` is an *optional* allowlist to scope monitoring to specific
    environments; `exclude_ids` skips specific ones (e.g. sandboxes). Any
    obvious placeholder IDs (env-example...) are ignored so a leftover
    config sample can't silently filter everything out.
    """
    envs = client.list_environments()

    include = [
        e for e in (include_ids or [])
        if e and not e.startswith(PLACEHOLDER_ENV_PREFIXES)
    ]
    if include:
        envs = [e for e in envs if e.get("id") in set(include)]
    if exclude_ids:
        envs = [e for e in envs if e.get("id") not in set(exclude_ids)]

    logger.info("Discovered %d environment(s) to monitor", len(envs))
    return envs


def collect_clusters(client: ConfluentCloudClient, environment_id: str) -> list[dict[str, Any]]:
    clusters = client.list_clusters(environment_id)
    out = []
    for c in clusters:
        spec = c.get("spec", {})
        status = c.get("status", {})
        out.append(
            {
                "resource_id": c.get("id"),
                "environment_id": environment_id,
                "display_name": spec.get("display_name"),
                "cluster_kind": (spec.get("config") or {}).get("kind"),
                "cloud": spec.get("cloud"),
                "region": spec.get("region"),
                "cku": (spec.get("config") or {}).get("cku"),
                "phase": status.get("phase"),  # PROVISIONED, PROVISIONING, FAILED
                "raw": c,
            }
        )
    return out


def collect_flink_compute_pools(
    client: ConfluentCloudClient, environment_id: str
) -> list[dict[str, Any]]:
    """Lists Flink compute pools in an environment and flattens the fields
    the dashboard/rules care about.
    """
    try:
        pools = client.list_flink_compute_pools(environment_id)
    except Exception:
        logger.exception("Failed to list Flink compute pools in env %s", environment_id)
        return []
    out = []
    for p in pools:
        spec = p.get("spec", {})
        status = p.get("status", {})
        out.append(
            {
                "resource_id": p.get("id"),  # lfcp-...
                "environment_id": environment_id,
                "display_name": spec.get("display_name"),
                "cloud": spec.get("cloud"),
                "region": spec.get("region"),
                "max_cfu": spec.get("max_cfu"),
                "current_cfu": status.get("current_cfu"),
                "phase": status.get("phase"),  # PROVISIONING/PROVISIONED/FAILED/DEPROVISIONING
                "raw": p,
            }
        )
    return out


def collect_connectors(
    client: ConfluentCloudClient, environment_id: str, cluster_id: str
) -> list[dict[str, Any]]:
    """Lists connectors then fetches per-connector status.

    Connector list responses vary by API version (sometimes a bare list of
    names, sometimes objects) — handle both defensively rather than
    assuming one shape.
    """
    try:
        raw = client.list_connectors(environment_id, cluster_id)
    except Exception:
        logger.exception(
            "Failed to list connectors for cluster %s in env %s", cluster_id, environment_id
        )
        return []

    if isinstance(raw, dict):
        names = list(raw.keys()) if raw else []
    elif isinstance(raw, list):
        names = [item.get("name", item) if isinstance(item, dict) else item for item in raw]
    else:
        names = []

    out = []
    for name in names:
        try:
            status = client.get_connector_status(environment_id, cluster_id, name)
        except Exception:
            logger.exception("Failed to fetch status for connector %s", name)
            status = {}
        connector_state = (status.get("connector") or {}).get("state")
        tasks = status.get("tasks") or []
        failed_tasks = [t for t in tasks if t.get("state") == "FAILED"]
        out.append(
            {
                "resource_id": f"{cluster_id}/{name}",
                "environment_id": environment_id,
                "cluster_id": cluster_id,
                "connector_name": name,
                "state": connector_state,  # RUNNING, PAUSED, FAILED
                "task_count": len(tasks),
                "failed_task_count": len(failed_tasks),
                "raw": status,
            }
        )
    return out
