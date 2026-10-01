"""Alert deduplication with a configurable cooldown window per rule+resource.

A dedup key is `rule_name` + `resource_id` (or just `rule_name` for
organization-scoped rules). If the same key fired within its cooldown
window, the new candidate is suppressed *unless* severity has escalated
(e.g. WARNING -> CRITICAL always gets through, so a worsening situation
isn't silenced by an earlier lower-severity alert).
"""
from __future__ import annotations

import datetime as dt

from sqlalchemy.orm import Session

from rules_engine.engine import CandidateAlert
from storage.models import AlertDedupState

SEVERITY_RANK = {"INFO": 0, "WARNING": 1, "CRITICAL": 2}


def make_dedup_key(candidate: CandidateAlert) -> str:
    if candidate.resource_id:
        return f"{candidate.rule_name}:{candidate.resource_id}"
    return candidate.rule_name


def should_suppress(
    session: Session,
    candidate: CandidateAlert,
    now: dt.datetime | None = None,
) -> bool:
    now = now or dt.datetime.now(dt.timezone.utc)
    key = make_dedup_key(candidate)
    state = session.query(AlertDedupState).filter_by(dedup_key=key).one_or_none()
    if state is None:
        return False

    last_fired_at = state.last_fired_at
    if last_fired_at.tzinfo is None:
        last_fired_at = last_fired_at.replace(tzinfo=dt.timezone.utc)
    within_cooldown = now - last_fired_at < dt.timedelta(minutes=candidate.cooldown_minutes)

    if not within_cooldown:
        return False

    # Allow escalation through even inside the cooldown window.
    prior_rank = SEVERITY_RANK.get(state.last_severity or "INFO", 0)
    new_rank = SEVERITY_RANK.get(candidate.severity, 0)
    if new_rank > prior_rank:
        return False

    return True


def record_fired(session: Session, candidate: CandidateAlert, now: dt.datetime | None = None) -> None:
    now = now or dt.datetime.now(dt.timezone.utc)
    key = make_dedup_key(candidate)
    state = session.query(AlertDedupState).filter_by(dedup_key=key).one_or_none()
    if state is None:
        state = AlertDedupState(
            dedup_key=key, last_fired_at=now, last_severity=candidate.severity, fire_count=1
        )
        session.add(state)
    else:
        state.last_fired_at = now
        state.last_severity = candidate.severity
        state.fire_count += 1
    session.commit()


def filter_suppressed(
    session: Session, candidates: list[CandidateAlert], now: dt.datetime | None = None
) -> list[CandidateAlert]:
    """Returns only the candidates that should actually fire, and records
    dedup state for each one that survives.
    """
    now = now or dt.datetime.now(dt.timezone.utc)
    surviving = []
    for c in candidates:
        if should_suppress(session, c, now=now):
            continue
        surviving.append(c)
        record_fired(session, c, now=now)
    return surviving
