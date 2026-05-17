"""Receiver hot-path tests.

We bring up the FastAPI app against the real Postgres pool, but mock the
queue so these tests don't require Redis. The queue's contract is a single
async function call (``enqueue_process``); the worker tests cover its
execution side.
"""

from __future__ import annotations

import hashlib
import hmac
from unittest.mock import AsyncMock

import httpx
import orjson
import pytest
import pytest_asyncio
from asgi_lifespan import LifespanManager

from app.config import get_settings
from app.hashing import compute_event_id

pytestmark = pytest.mark.e2e


@pytest_asyncio.fixture(loop_scope="session")
async def app_client(clean_db, monkeypatch):
    """FastAPI test client backed by the real DB pool, mocked queue."""

    enqueue_mock = AsyncMock(return_value=None)
    init_queue_mock = AsyncMock(return_value=None)
    close_queue_mock = AsyncMock(return_value=None)

    # Ensure the receiver's bound name is the mock and that startup/shutdown
    # don't touch real Redis.
    from app import db as db_mod
    from app import queue as queue_mod

    monkeypatch.setattr(queue_mod, "init_queue", init_queue_mock)
    monkeypatch.setattr(queue_mod, "close_queue", close_queue_mock)
    monkeypatch.setattr(queue_mod, "enqueue_process", enqueue_mock)
    monkeypatch.setattr("app.api.receiver.enqueue_process", enqueue_mock)

    # The lifespan calls init_pool/close_pool. Make those a no-op and reuse
    # the test pool so the receiver inserts hit the same DB the test reads.
    monkeypatch.setattr(db_mod, "init_pool", AsyncMock(return_value=clean_db))
    monkeypatch.setattr(db_mod, "close_pool", AsyncMock(return_value=None))
    monkeypatch.setattr(db_mod, "_pool", clean_db, raising=False)
    monkeypatch.setattr("app.main.init_pool", AsyncMock(return_value=clean_db))
    monkeypatch.setattr("app.main.close_pool", AsyncMock(return_value=None))
    monkeypatch.setattr("app.main.init_queue", init_queue_mock)
    monkeypatch.setattr("app.main.close_queue", close_queue_mock)

    from app.main import app

    async with (
        LifespanManager(app),
        httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client,
    ):
        yield client, enqueue_mock


async def test_post_webhook_returns_202_and_inserts_one_row(app_client, fixture_payloads, clean_db):
    client, enqueue_mock = app_client
    payload = fixture_payloads["01_maersk_in_transit"]

    resp = await client.post("/webhooks", json=payload)
    assert resp.status_code == 202
    body = resp.json()
    assert body["deduplicated"] is False
    assert body["event_id"] == compute_event_id(payload, payload.get("event_msg_id"))

    async with clean_db.acquire() as conn:
        n = await conn.fetchval("SELECT count(*) FROM raw_events WHERE event_id=$1", body["event_id"])
    assert n == 1
    assert enqueue_mock.await_count == 1


async def test_duplicate_post_dedups_and_does_not_reenqueue(app_client, fixture_payloads, clean_db):
    client, enqueue_mock = app_client
    payload = fixture_payloads["01_maersk_in_transit"]

    r1 = await client.post("/webhooks", json=payload)
    r2 = await client.post("/webhooks", json=payload)
    r3 = await client.post("/webhooks", json=payload)

    assert r1.json()["deduplicated"] is False
    assert r2.json()["deduplicated"] is True
    assert r3.json()["deduplicated"] is True
    assert r1.json()["event_id"] == r2.json()["event_id"] == r3.json()["event_id"]

    async with clean_db.acquire() as conn:
        n = await conn.fetchval("SELECT count(*) FROM raw_events")
    assert n == 1
    assert enqueue_mock.await_count == 1


async def test_invalid_json_returns_400(app_client):
    client, _ = app_client
    resp = await client.post(
        "/webhooks",
        content=b"not-json",
        headers={"content-type": "application/json"},
    )
    assert resp.status_code == 400


async def test_array_payload_rejected(app_client):
    client, _ = app_client
    resp = await client.post("/webhooks", json=[1, 2, 3])
    assert resp.status_code == 400


async def test_empty_body_rejected(app_client):
    client, _ = app_client
    resp = await client.post("/webhooks", content=b"")
    assert resp.status_code == 400


async def test_vendor_scoped_path(app_client, fixture_payloads, clean_db):
    client, _ = app_client
    payload = fixture_payloads["01_maersk_in_transit"]

    resp = await client.post("/webhooks/maersk", json=payload)
    assert resp.status_code == 202

    async with clean_db.acquire() as conn:
        vendor = await conn.fetchval(
            "SELECT vendor_id FROM raw_events WHERE event_id=$1", resp.json()["event_id"]
        )
    assert vendor == "maersk"


async def test_vendor_signature_verified_when_secret_configured(app_client, fixture_payloads, clean_db):
    client, _ = app_client
    payload = fixture_payloads["01_maersk_in_transit"]
    body = orjson.dumps(payload)

    settings = get_settings()
    old_secrets = dict(settings.webhook_vendor_secrets)
    old_enforce = settings.webhook_signature_enforce
    old_header = settings.webhook_signature_header
    settings.webhook_vendor_secrets = {"maersk": "test-secret"}
    settings.webhook_signature_enforce = True
    settings.webhook_signature_header = "X-Signature"

    try:
        sig = hmac.new(b"test-secret", body, hashlib.sha256).hexdigest()
        resp = await client.post(
            "/webhooks/maersk",
            content=body,
            headers={"content-type": "application/json", "X-Signature": f"sha256={sig}"},
        )
        assert resp.status_code == 202
        event_id = resp.json()["event_id"]
        async with clean_db.acquire() as conn:
            verified = await conn.fetchval(
                "SELECT signature_verified FROM raw_events WHERE event_id=$1",
                event_id,
            )
        assert verified is True
    finally:
        settings.webhook_vendor_secrets = old_secrets
        settings.webhook_signature_enforce = old_enforce
        settings.webhook_signature_header = old_header


async def test_vendor_signature_rejected_when_invalid_and_enforced(app_client, fixture_payloads):
    client, _ = app_client
    payload = fixture_payloads["01_maersk_in_transit"]
    body = orjson.dumps(payload)

    settings = get_settings()
    old_secrets = dict(settings.webhook_vendor_secrets)
    old_enforce = settings.webhook_signature_enforce
    old_header = settings.webhook_signature_header
    settings.webhook_vendor_secrets = {"maersk": "test-secret"}
    settings.webhook_signature_enforce = True
    settings.webhook_signature_header = "X-Signature"

    try:
        resp = await client.post(
            "/webhooks/maersk",
            content=body,
            headers={"content-type": "application/json", "X-Signature": "sha256=deadbeef"},
        )
        assert resp.status_code == 401
    finally:
        settings.webhook_vendor_secrets = old_secrets
        settings.webhook_signature_enforce = old_enforce
        settings.webhook_signature_header = old_header
