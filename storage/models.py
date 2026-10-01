"""SQLAlchemy models for the alerting POC.

Design notes:
- Uses immutable resource/principal IDs (lkc-..., sa-..., env-...) as the
  primary keys for attribution, per the design doc's guidance to avoid
  keying alert rules on mutable display names.
- Kept intentionally simple (a handful of tables) for POC scope. A
  production version would likely want partitioned fact tables per
  collector and a proper time-series store for metrics.
"""
from __future__ import annotations

import datetime as dt

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base

Base = declarative_base()


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class ResourceFact(Base):
    """A normalized point-in-time observation about a resource.

    One row per (resource_id, metric_name, observed_at). `value` holds the
    numeric reading; `raw_json` keeps the original payload for debugging.
    """

    __tablename__ = "resource_facts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    organization_id = Column(String, nullable=False, index=True)
    environment_id = Column(String, nullable=True, index=True)
    resource_id = Column(String, nullable=False, index=True)  # e.g. lkc-xxxxx
    resource_type = Column(String, nullable=False)  # kafka_cluster, connector, ...
    principal_id = Column(String, nullable=True, index=True)  # sa-xxxxx if applicable
    metric_name = Column(String, nullable=False, index=True)
    value = Column(Float, nullable=False)
    unit = Column(String, nullable=True)
    source = Column(String, nullable=False)  # metrics_api, costs_api, cmk_v2, ...
    observed_at = Column(DateTime(timezone=True), nullable=False, index=True)
    collected_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    raw_json = Column(Text, nullable=True)


class CostFact(Base):
    """One row per Costs API line item (billing/v1/costs `data[]` entry)."""

    __tablename__ = "cost_facts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    organization_id = Column(String, nullable=False, index=True)
    environment_id = Column(String, nullable=True, index=True)
    resource_id = Column(String, nullable=True, index=True)
    product = Column(String, nullable=True)  # KAFKA, CONNECT, FLINK, ...
    line_type = Column(String, nullable=True)  # e.g. KAFKA_NUM_CKUS
    start_date = Column(String, nullable=False)
    end_date = Column(String, nullable=False)
    granularity = Column(String, nullable=True)
    quantity = Column(Float, nullable=True)
    unit = Column(String, nullable=True)
    original_amount = Column(Float, nullable=True)
    discount_amount = Column(Float, nullable=True)
    amount = Column(Float, nullable=False)
    is_accrued_estimate = Column(Integer, nullable=False, default=1)  # bool
    collected_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    raw_json = Column(Text, nullable=True)


class Alert(Base):
    """A fired alert, post-deduplication."""

    __tablename__ = "alerts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    alert_name = Column(String, nullable=False, index=True)
    category = Column(String, nullable=False, index=True)
    severity = Column(String, nullable=False)
    organization_id = Column(String, nullable=False)
    environment_id = Column(String, nullable=True)
    resource_id = Column(String, nullable=True, index=True)
    current_value = Column(Float, nullable=True)
    threshold = Column(Float, nullable=True)
    unit = Column(String, nullable=True)
    message = Column(Text, nullable=False)
    observed_at = Column(DateTime(timezone=True), nullable=False)
    sent_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    delivery_status = Column(String, nullable=False, default="pending")
    raw_payload_json = Column(Text, nullable=True)


class AlertDedupState(Base):
    """Cooldown tracking so identical alerts don't spam notifiers.

    Keyed by a composite dedup_key (rule name + resource id, typically).
    """

    __tablename__ = "alert_dedup_state"
    __table_args__ = (UniqueConstraint("dedup_key", name="uq_dedup_key"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    dedup_key = Column(String, nullable=False, index=True)
    last_fired_at = Column(DateTime(timezone=True), nullable=False)
    last_severity = Column(String, nullable=True)
    fire_count = Column(Integer, nullable=False, default=1)


class CostPeriod(Base):
    """Cached cost total for one settled or in-progress period.

    Backs the dashboard's yearly (per-month) and monthly (per-day) line
    charts. `granularity` is MONTH or DAY; `period` is 'YYYY-MM' or
    'YYYY-MM-DD'. `is_final` marks a period whose window ended long enough
    ago that its cost won't change (accounting for the Costs API's ~72h
    lag), so it can be served from cache without re-querying.
    """

    __tablename__ = "cost_periods"
    __table_args__ = (
        UniqueConstraint("organization_id", "granularity", "period", name="uq_cost_period"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    organization_id = Column(String, nullable=False, index=True)
    granularity = Column(String, nullable=False)  # MONTH | DAY
    period = Column(String, nullable=False, index=True)  # 'YYYY-MM' or 'YYYY-MM-DD'
    amount = Column(Float, nullable=False, default=0.0)
    is_final = Column(Integer, nullable=False, default=0)  # bool
    collected_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
