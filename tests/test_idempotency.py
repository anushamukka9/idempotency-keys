"""Tests for the idempotency-key middleware.

Run with:  pytest
"""

import asyncio
import time
import uuid

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import app as charge_app
import idempotency
from idempotency import IdempotencyMiddleware, _idempotency_store, _reset_store


@pytest.fixture()
def client():
    _reset_store()
    charge_app.charges.clear()
    with TestClient(charge_app.app) as c:
        yield c


def _key() -> str:
    return str(uuid.uuid4())


def test_no_key_runs_the_naive_path(client):
    client.post("/charge?amount_cents=5000&customer_id=cus_42")
    client.post("/charge?amount_cents=5000&customer_id=cus_42")
    assert client.get("/charges").json()["count"] == 2


def test_retry_with_same_key_replays_identical_response(client):
    key = _key()
    headers = {"Idempotency-Key": key}
    url = "/charge?amount_cents=5000&customer_id=cus_42"

    first = client.post(url, headers=headers)
    second = client.post(url, headers=headers)

    assert first.status_code == 200
    assert second.status_code == 200
    # The retry sees the identical response, down to the charge id.
    assert second.json()["charge_id"] == first.json()["charge_id"]
    assert first.headers["idempotent-replayed"] == "false"
    assert second.headers["idempotent-replayed"] == "true"
    # ...and the work ran exactly once.
    assert client.get("/charges").json()["count"] == 1


def test_same_key_with_different_payload_is_rejected(client):
    key = _key()
    headers = {"Idempotency-Key": key}

    ok = client.post("/charge?amount_cents=5000&customer_id=cus_42",
                     headers=headers)
    assert ok.status_code == 200

    bad = client.post("/charge?amount_cents=9999&customer_id=cus_42",
                      headers=headers)
    assert bad.status_code == 422
    assert "different request" in bad.json()["detail"]
    assert client.get("/charges").json()["count"] == 1


def test_different_keys_execute_independently(client):
    client.post("/charge?amount_cents=5000&customer_id=cus_42",
                headers={"Idempotency-Key": _key()})
    client.post("/charge?amount_cents=5000&customer_id=cus_42",
                headers={"Idempotency-Key": _key()})
    assert client.get("/charges").json()["count"] == 2


def test_concurrent_duplicate_gets_409_while_first_is_in_flight():
    _reset_store()
    release = asyncio.Event()

    slow = FastAPI()

    @slow.post("/slow")
    async def slow_endpoint():
        await release.wait()
        return {"ok": True}

    slow.add_middleware(IdempotencyMiddleware)

    async def main():
        transport = httpx.ASGITransport(app=slow)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://test") as ac:
            key = _key()
            headers = {"Idempotency-Key": key}

            async def first():
                return await ac.post("/slow", headers=headers)

            async def second():
                await asyncio.sleep(0.3)  # arrive while first is in flight
                return await ac.post("/slow", headers=headers)

            first_task = asyncio.create_task(first())
            await asyncio.sleep(0.1)  # let the first request mark in-flight
            second_resp = await second()
            release.set()
            first_resp = await first_task
            return first_resp, second_resp

    first_resp, second_resp = asyncio.run(main())
    assert first_resp.status_code == 200
    assert second_resp.status_code == 409
    assert "in flight" in second_resp.json()["detail"]


def test_failed_handler_drops_the_record_so_the_client_can_retry():
    _reset_store()
    calls = {"n": 0}
    flaky = FastAPI()

    @flaky.post("/flaky")
    def flaky_endpoint():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return {"attempt": calls["n"]}

    flaky.add_middleware(IdempotencyMiddleware)

    async def main():
        transport = httpx.ASGITransport(app=flaky)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://test") as ac:
            key = _key()
            headers = {"Idempotency-Key": key}
            with pytest.raises(RuntimeError):
                await ac.post("/flaky", headers=headers)
            # The dead in-flight record is gone, so the retry runs for real.
            retry = await ac.post("/flaky", headers=headers)
            return retry

    retry = asyncio.run(main())
    assert retry.status_code == 200
    assert retry.json() == {"attempt": 2}
    assert retry.headers["idempotent-replayed"] == "false"


def test_expired_key_allows_reexecution(monkeypatch, client):
    monkeypatch.setattr(idempotency, "KEY_TTL_SECONDS", 0.2)
    key = _key()
    headers = {"Idempotency-Key": key}
    url = "/charge?amount_cents=5000&customer_id=cus_42"

    first = client.post(url, headers=headers)
    assert first.status_code == 200

    time.sleep(0.5)  # let the key expire; the prune runs on the next request
    second = client.post(url, headers=headers)

    assert second.status_code == 200
    assert second.json()["charge_id"] != first.json()["charge_id"]
    assert second.headers["idempotent-replayed"] == "false"
    assert client.get("/charges").json()["count"] == 2


def test_prune_removes_only_expired_records():
    _reset_store()
    now = time.time()
    _idempotency_store["old"] = {"status": "completed", "fingerprint": "x",
                                 "expires_at": now - 1}
    _idempotency_store["fresh"] = {"status": "completed", "fingerprint": "y",
                                   "expires_at": now + 3600}
    idempotency._prune_expired(now)
    assert "old" not in _idempotency_store
    assert "fresh" in _idempotency_store
