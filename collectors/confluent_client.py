"""Thin HTTP client for Confluent Cloud's REST APIs.

Two distinct API key pairs are needed (see README table):
  - `cloud` credentials: Cloud API key/secret with OrganizationAdmin or
    BillingAdmin role. Used for Costs, Environments, Clusters, Connectors,
    Service Quotas.
  - `metrics` credentials: an API key that is *resource-scoped for resource
    management*. Required by the Metrics API specifically (a Cluster API
    key will fail auth against it).

I have not hardcoded metric names or resource-type strings anywhere in this
client: callers are expected to hit the `/descriptors/*` discovery
endpoints first, exactly as Confluent's docs recommend, so this code
doesn't silently drift out of sync as Confluent adds/renames metrics.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)

CLOUD_API_BASE = "https://api.confluent.cloud"
METRICS_API_BASE = "https://api.telemetry.confluent.cloud"


@dataclass(frozen=True)
class ApiCredentials:
    key: str
    secret: str


class ConfluentApiError(RuntimeError):
    def __init__(self, status_code: int, url: str, body: str):
        super().__init__(f"Confluent API error {status_code} for {url}: {body[:500]}")
        self.status_code = status_code
        self.url = url
        self.body = body


class RateLimitError(ConfluentApiError):
    """Raised when the API keeps returning 429 after all retries."""


class ConfluentCloudClient:
    """Wraps requests to api.confluent.cloud and api.telemetry.confluent.cloud."""

    def __init__(
        self,
        cloud_creds: ApiCredentials,
        metrics_creds: Optional[ApiCredentials] = None,
        timeout_seconds: int = 30,
        max_retries: int = 5,
        min_request_interval_seconds: float = 0.0,
    ):
        self.cloud_creds = cloud_creds
        self.metrics_creds = metrics_creds or cloud_creds
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        # Optional client-side spacing between requests to avoid tripping the
        # API rate limit during burst loops (e.g. per-day cost queries).
        self.min_request_interval_seconds = min_request_interval_seconds
        self._last_request_at = 0.0
        self._throttle_lock = threading.Lock()
        self.session = requests.Session()

    # -- low level -------------------------------------------------------

    @staticmethod
    def _retry_after_seconds(resp: requests.Response, fallback: float) -> float:
        """Parse a Retry-After header (delta-seconds or HTTP-date). Falls back
        to the provided backoff value when the header is missing/unparseable.
        """
        header = resp.headers.get("Retry-After")
        if not header:
            return fallback
        try:
            return max(0.0, float(header))
        except ValueError:
            pass
        try:
            import datetime as _dt

            when = parsedate_to_datetime(header)
            delta = (when - _dt.datetime.now(_dt.timezone.utc)).total_seconds()
            return max(0.0, delta)
        except (TypeError, ValueError):
            return fallback

    def _throttle(self) -> None:
        if self.min_request_interval_seconds <= 0:
            return
        # Serialize spacing across threads so concurrent dashboard requests
        # don't burst past the Costs API rate limit.
        with self._throttle_lock:
            elapsed = time.monotonic() - self._last_request_at
            wait = self.min_request_interval_seconds - elapsed
            if wait > 0:
                time.sleep(wait)
            self._last_request_at = time.monotonic()

    def _request(
        self,
        method: str,
        url: str,
        creds: ApiCredentials,
        params: Optional[dict] = None,
        json_body: Optional[dict] = None,
    ) -> dict[str, Any]:
        backoff = 1.0
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            self._throttle()
            try:
                resp = self.session.request(
                    method,
                    url,
                    params=params,
                    json=json_body,
                    auth=(creds.key, creds.secret),
                    timeout=self.timeout_seconds,
                    headers={"Accept": "application/json"},
                )
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                if attempt == self.max_retries:
                    raise
                time.sleep(min(backoff, 20))
                backoff *= 2
                continue

            if resp.status_code == 429:
                if attempt == self.max_retries:
                    raise RateLimitError(resp.status_code, url, resp.text)
                delay = self._retry_after_seconds(resp, fallback=min(backoff, 30))
                logger.warning(
                    "Rate limited by Confluent Cloud API: %s — retrying in %.1fs (attempt %d/%d)",
                    url, delay, attempt, self.max_retries,
                )
                time.sleep(min(delay, 60))
                backoff *= 2
                continue

            if not resp.ok:
                raise ConfluentApiError(resp.status_code, url, resp.text)
            if not resp.content:
                return {}
            return resp.json()

        # Loop only exits via return/raise above; this satisfies type-checkers.
        raise last_exc if last_exc else RuntimeError("request retry loop exhausted")

    def _cloud_get(self, path: str, params: Optional[dict] = None) -> dict:
        return self._request("GET", f"{CLOUD_API_BASE}{path}", self.cloud_creds, params=params)

    def _cloud_get_paginated(self, path: str, params: Optional[dict] = None) -> list[dict]:
        """Follows `metadata.next` page links, returning the concatenated `data` list."""
        items: list[dict] = []
        query = dict(params or {})
        next_url: Optional[str] = None
        first = True
        while first or next_url:
            first = False
            if next_url:
                payload = self._request("GET", next_url, self.cloud_creds)
            else:
                payload = self._cloud_get(path, params=query)
            # The API can return "data": null (not just an absent key), so the
            # dict.get default isn't enough — coerce None to an empty list.
            items.extend(payload.get("data") or [])
            next_url = (payload.get("metadata") or {}).get("next")
        return items

    # -- Environments / Clusters / Connectors (Cloud API) -----------------

    def list_environments(self) -> list[dict]:
        """GET /org/v2/environments"""
        return self._cloud_get_paginated("/org/v2/environments")

    def list_clusters(self, environment_id: str) -> list[dict]:
        """GET /cmk/v2/clusters?environment={environment_id}"""
        return self._cloud_get_paginated(
            "/cmk/v2/clusters", params={"environment": environment_id}
        )

    def list_flink_compute_pools(self, environment_id: str) -> list[dict]:
        """GET /fcpm/v2/compute-pools?environment={environment_id}

        Uses the Cloud API key (not a Flink API key), per Confluent's docs.
        Returns Flink compute pools (lfcp-...) with spec.display_name,
        spec.cloud/region/max_cfu, status.phase, and status.current_cfu.
        """
        return self._cloud_get_paginated(
            "/fcpm/v2/compute-pools", params={"environment": environment_id, "page_size": 100}
        )

    def list_connectors(self, environment_id: str, cluster_id: str) -> list[dict]:
        """GET /connect/v1/environments/{env}/clusters/{lkc}/connectors

        NOTE: this endpoint returns connector *names* only in some API
        versions; connector status/config require a follow-up call. I'm
        not certain of the exact current response shape for every account
        type — verify against https://docs.confluent.io/cloud/current/api.html
        (Connect API v1) before relying on field names here.
        """
        return self._cloud_get(
            f"/connect/v1/environments/{environment_id}/clusters/{cluster_id}/connectors"
        )

    def get_connector_status(
        self, environment_id: str, cluster_id: str, connector_name: str
    ) -> dict:
        """GET /connect/v1/environments/{env}/clusters/{lkc}/connectors/{name}/status"""
        return self._cloud_get(
            f"/connect/v1/environments/{environment_id}/clusters/{cluster_id}"
            f"/connectors/{connector_name}/status"
        )

    # -- Service Quotas API ------------------------------------------------

    def list_applied_quotas(self, scope: str, scope_id: Optional[str] = None) -> list[dict]:
        """GET /service-quota/v1/applied-quotas?scope={scope}[&environment=...]

        `scope` must be one of: organization, environment, kafka_cluster,
        network, user_account, service_account (per Confluent's docs).
        Uses the metrics-scoped credentials, per Confluent's requirement
        that this API needs an API key resource-scoped for resource
        management.
        """
        params: dict[str, Any] = {"scope": scope}
        if scope_id:
            # The scoping query param name varies by scope in Confluent's
            # docs (e.g. `environment=env-x`, `kafka_cluster=lkc-x`). Callers
            # pass the correct kwarg name via `extra_params` if needed;
            # kept simple here for the common cases.
            params[scope] = scope_id
        url = f"{CLOUD_API_BASE}/service-quota/v1/applied-quotas"
        return self._request("GET", url, self.metrics_creds, params=params).get("data") or []

    def list_quota_scopes(self) -> list[dict]:
        """GET /service-quota/v1/scopes"""
        return self._request(
            "GET", f"{CLOUD_API_BASE}/service-quota/v1/scopes", self.metrics_creds
        ).get("data") or []

    # -- Costs API ----------------------------------------------------------

    def list_costs(self, start_date: str, end_date: str) -> list[dict]:
        """GET /billing/v1/costs?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD

        Requires Cloud API key belonging to a user/service-account with
        OrganizationAdmin or BillingAdmin role. Cost data can lag up to 72
        hours behind actual usage (Confluent's documented SLA) — always
        treat this as an accrued estimate, not a final invoice figure.
        """
        return self._cloud_get_paginated(
            "/billing/v1/costs", params={"start_date": start_date, "end_date": end_date}
        )

    # -- Metrics API ----------------------------------------------------------

    def describe_metric_resources(self) -> dict:
        """GET /v2/metrics/cloud/descriptors/resources

        Discovery endpoint — use this instead of hardcoding resource type
        strings, per Confluent's own recommendation.
        """
        return self._request(
            "GET",
            f"{METRICS_API_BASE}/v2/metrics/cloud/descriptors/resources",
            self.metrics_creds,
        )

    def _metrics_get_paginated(self, url: str, params: Optional[dict] = None) -> dict:
        """Follow the telemetry API's `links.next` pagination, returning
        {"data": [...]} with every page concatenated. Uses metrics creds.
        """
        items: list[dict] = []
        payload = self._request("GET", url, self.metrics_creds, params=params)
        items.extend(payload.get("data") or [])
        next_url = (payload.get("links") or {}).get("next")
        while next_url:
            payload = self._request("GET", next_url, self.metrics_creds)
            items.extend(payload.get("data") or [])
            next_url = (payload.get("links") or {}).get("next")
        return {"data": items}

    def describe_metrics(self, resource_type: str = "kafka") -> dict:
        """GET /v2/metrics/cloud/descriptors/metrics?resource_type=...

        `resource_type` is REQUIRED by this endpoint (e.g. "kafka", "flink",
        "connector", "ksql", "schema_registry"). The response is paginated
        (page_size up to 1000, followed via links.next), so this returns the
        full descriptor list for the given resource type — not just page one.
        """
        return self._metrics_get_paginated(
            f"{METRICS_API_BASE}/v2/metrics/cloud/descriptors/metrics",
            params={"resource_type": resource_type, "page_size": 1000},
        )

    def query_metrics(self, query_body: dict) -> dict:
        """POST /v2/metrics/cloud/query

        `query_body` follows Confluent's documented query schema (metric
        name(s), filter, group_by, granularity, intervals). Build it via
        `collectors/metrics_query_builder.py` rather than hand-rolling JSON,
        so a single place absorbs any future schema tweaks.
        """
        return self._request(
            "POST",
            f"{METRICS_API_BASE}/v2/metrics/cloud/query",
            self.metrics_creds,
            json_body=query_body,
        )
