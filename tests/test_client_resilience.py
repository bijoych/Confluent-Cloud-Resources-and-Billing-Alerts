"""Regression tests for the two failures seen in the field:
  1. HTTP 429 crashing the run (now retried with backoff / Retry-After)
  2. `"data": null` in a list response crashing pagination with a TypeError
"""
import pytest

from collectors.confluent_client import (
    ApiCredentials,
    ConfluentApiError,
    ConfluentCloudClient,
    RateLimitError,
)


class FakeResp:
    def __init__(self, status, headers=None, payload=None, content=b"{}"):
        self.status_code = status
        self.headers = headers or {}
        self.ok = 200 <= status < 300
        self.text = "body"
        self.content = content
        self._payload = payload if payload is not None else {}

    def json(self):
        return self._payload


def make_client():
    return ConfluentCloudClient(
        ApiCredentials("k", "s"), min_request_interval_seconds=0, max_retries=4
    )


def test_429_then_success_is_retried():
    client = make_client()
    seq = [FakeResp(429, {"Retry-After": "0"}), FakeResp(429, {"Retry-After": "0"}),
           FakeResp(200, payload={"data": [{"id": "x"}]})]
    calls = {"n": 0}

    def fake(*a, **k):
        r = seq[calls["n"]]
        calls["n"] += 1
        return r

    client.session.request = fake
    out = client._request("GET", "http://t", client.cloud_creds)
    assert out == {"data": [{"id": "x"}]}
    assert calls["n"] == 3


def test_persistent_429_raises_rate_limit_error():
    client = make_client()
    client.session.request = lambda *a, **k: FakeResp(429, {"Retry-After": "0"})
    with pytest.raises(RateLimitError):
        client._request("GET", "http://t", client.cloud_creds)


def test_non_429_error_raises_immediately():
    client = make_client()
    calls = {"n": 0}

    def fake(*a, **k):
        calls["n"] += 1
        return FakeResp(403, content=b"x")

    client.session.request = fake
    with pytest.raises(ConfluentApiError):
        client._request("GET", "http://t", client.cloud_creds)
    assert calls["n"] == 1  # not retried


def test_null_data_pagination_does_not_crash():
    client = make_client()
    client.session.request = lambda *a, **k: FakeResp(200, payload={"data": None, "metadata": {}})
    assert client._cloud_get_paginated("/cmk/v2/clusters", params={"environment": "env-1"}) == []


def test_retry_after_parsing():
    assert ConfluentCloudClient._retry_after_seconds(FakeResp(429, {"Retry-After": "2.5"}), 1.0) == 2.5
    assert ConfluentCloudClient._retry_after_seconds(FakeResp(429, {}), 1.0) == 1.0
