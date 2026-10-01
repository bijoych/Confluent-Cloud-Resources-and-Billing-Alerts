"""Engine/session helpers.

Falls back to a local SQLite file when DATABASE_URL is unset, so the POC
runs with zero external dependencies out of the box. Point DATABASE_URL at
Postgres for anything beyond a laptop demo.
"""
from __future__ import annotations

import os
import pathlib

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from storage.models import Base

_DEFAULT_SQLITE_PATH = pathlib.Path("data/alerts.db")


def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL", "").strip()
    if url:
        return url
    _DEFAULT_SQLITE_PATH.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{_DEFAULT_SQLITE_PATH}"


_engine = None
_SessionFactory = None


def get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(get_database_url(), future=True)
    return _engine


def init_db() -> None:
    Base.metadata.create_all(get_engine())


def get_session() -> Session:
    global _SessionFactory
    if _SessionFactory is None:
        _SessionFactory = sessionmaker(bind=get_engine(), future=True)
    return _SessionFactory()
