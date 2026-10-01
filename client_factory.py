"""Builds a ConfluentCloudClient from environment credentials.

Shared so both the scheduler and the dashboard construct the client the
same way (two credential pairs: Cloud API key for most APIs, a
resource-management-scoped key for Metrics/Quotas).
"""
from __future__ import annotations

import logging
import os

from collectors.confluent_client import ApiCredentials, ConfluentCloudClient

logger = logging.getLogger(__name__)


def build_client() -> ConfluentCloudClient:
    cloud_creds = ApiCredentials(
        key=os.environ["CONFLUENT_CLOUD_API_KEY"],
        secret=os.environ["CONFLUENT_CLOUD_API_SECRET"],
    )
    metrics_key = os.environ.get("CONFLUENT_METRICS_API_KEY")
    metrics_secret = os.environ.get("CONFLUENT_METRICS_API_SECRET")
    if metrics_key:
        logger.info("Using a separate Metrics/Quotas API key")
        metrics_creds = ApiCredentials(key=metrics_key, secret=metrics_secret)
    else:
        logger.info("Using the single Cloud API key for all APIs (incl. Metrics/Quotas)")
        metrics_creds = None  # client falls back to the Cloud key

    # Gentle client-side spacing between requests to avoid tripping the Costs
    # API rate limit during burst loops (the dashboard's per-day cost history).
    # Overridable via env; the client also retries 429s with Retry-After.
    min_interval = float(os.environ.get("CONFLUENT_API_MIN_INTERVAL", "0.5"))
    max_retries = int(os.environ.get("CONFLUENT_API_MAX_RETRIES", "5"))
    return ConfluentCloudClient(
        cloud_creds=cloud_creds,
        metrics_creds=metrics_creds,
        max_retries=max_retries,
        min_request_interval_seconds=min_interval,
    )
