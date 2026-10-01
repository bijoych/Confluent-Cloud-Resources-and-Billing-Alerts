"""Tests for dynamic environment discovery in collect_environments.

Uses a tiny fake client so no network/credentials are needed — the point
is to verify the include/exclude/placeholder filtering, not the HTTP call.
"""
from collectors.cloud_resources_collector import collect_environments

ALL_ENVS = [
    {"id": "env-prod01"},
    {"id": "env-staging"},
    {"id": "env-sandbox"},
]


class FakeClient:
    def __init__(self, envs):
        self._envs = envs

    def list_environments(self):
        return self._envs


def test_discovers_all_by_default():
    client = FakeClient(ALL_ENVS)
    result = collect_environments(client)
    assert {e["id"] for e in result} == {"env-prod01", "env-staging", "env-sandbox"}


def test_empty_allowlist_still_discovers_all():
    client = FakeClient(ALL_ENVS)
    result = collect_environments(client, include_ids=[])
    assert len(result) == 3


def test_placeholder_ids_are_ignored_not_treated_as_allowlist():
    # A leftover sample config must NOT filter everything out.
    client = FakeClient(ALL_ENVS)
    result = collect_environments(client, include_ids=["env-example1", "env-example2"])
    assert len(result) == 3


def test_real_allowlist_scopes_to_listed_envs():
    client = FakeClient(ALL_ENVS)
    result = collect_environments(client, include_ids=["env-prod01"])
    assert {e["id"] for e in result} == {"env-prod01"}


def test_exclude_skips_listed_envs():
    client = FakeClient(ALL_ENVS)
    result = collect_environments(client, exclude_ids=["env-sandbox"])
    assert {e["id"] for e in result} == {"env-prod01", "env-staging"}


def test_include_and_exclude_combine():
    client = FakeClient(ALL_ENVS)
    result = collect_environments(
        client, include_ids=["env-prod01", "env-staging"], exclude_ids=["env-staging"]
    )
    assert {e["id"] for e in result} == {"env-prod01"}
