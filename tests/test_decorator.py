"""Tests for the @idempotent per-endpoint decorator."""

import asyncio
import uuid

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from starlette.responses import JSONResponse

from idempotency import _reset_store, idempotent
from stores import MemoryStore


def _key():
    return str(uuid.uuid4())


@pytest.fixture()
def setup():
    store = MemoryStore()
    calls = {"n": 0}
    app = FastAPI()

    @app.post("/refund")
    @idempotent(store=store)
    async def refund(request: Request, amount_cents: int):
        calls["n"] += 1
        return {"n": calls["n"], "amount_cents": amount_cents}

    @app.post("/sync")
    @idempotent(store=store)
    def sync_endpoint(request: Request):
        calls["n"] += 1
        return {"n": calls["n"], "sync": True}

    @app.post("/raw")
    @idempotent(store=store)
    async def raw(request: Request):
        calls["n"] += 1
        return JSONResponse(status_code=201, content={"created": True})

    @app.post("/plain")
    def plain():
        calls["n"] += 1
        return {"n": calls["n"]}

    return app, store, calls


def test_decorator_replays_retry(setup):
    app, _, calls = setup
    client = TestClient(app)
    headers = {"Idempotency-Key": _key()}

    first = client.post("/refund?amount_cents=500", headers=headers)
    second = client.post("/refund?amount_cents=500", headers=headers)

    assert first.status_code == 200
    assert second.json() == first.json()
    assert first.headers["idempotent-replayed"] == "false"
    assert second.headers["idempotent-replayed"] == "true"
    assert calls["n"] == 1


def test_decorator_ignores_requests_without_a_key(setup):
    app, _, calls = setup
    client = TestClient(app)
    client.post("/refund?amount_cents=500")
    client.post("/refund?amount_cents=500")
    assert calls["n"] == 2


def test_decorator_rejects_changed_payload(setup):
    app, _, calls = setup
    client = TestClient(app)
    headers = {"Idempotency-Key": _key()}
    ok = client.post("/refund?amount_cents=500", headers=headers)
    bad = client.post("/refund?amount_cents=501", headers=headers)
    assert ok.status_code == 200
    assert bad.status_code == 422
    assert calls["n"] == 1


def test_decorator_leaves_other_routes_alone(setup):
    app, _, calls = setup
    client = TestClient(app)
    headers = {"Idempotency-Key": _key()}
    client.post("/plain", headers=headers)
    client.post("/plain", headers=headers)
    assert calls["n"] == 2


def test_decorator_works_on_sync_endpoints(setup):
    app, _, calls = setup
    client = TestClient(app)
    headers = {"Idempotency-Key": _key()}
    first = client.post("/sync", headers=headers)
    second = client.post("/sync", headers=headers)
    assert second.json() == first.json() == {"n": 1, "sync": True}
    assert second.headers["idempotent-replayed"] == "true"
    assert calls["n"] == 1


def test_decorator_replays_starlette_responses(setup):
    app, _, calls = setup
    client = TestClient(app)
    headers = {"Idempotency-Key": _key()}
    first = client.post("/raw", headers=headers)
    second = client.post("/raw", headers=headers)
    assert first.status_code == 201
    assert second.status_code == 201
    assert second.json() == {"created": True}
    assert second.headers["idempotent-replayed"] == "true"
    assert calls["n"] == 1


def test_decorator_requires_a_request_parameter():
    with pytest.raises(TypeError, match="request"):
        @idempotent(store=MemoryStore())
        def no_request():
            return {}


def test_decorator_returns_409_while_in_flight():
    _reset_store()
    store = MemoryStore()
    release = asyncio.Event()
    app = FastAPI()

    @app.post("/slow")
    @idempotent(store=store)
    async def slow(request: Request):
        await release.wait()
        return {"ok": True}

    async def main():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://test") as ac:
            headers = {"Idempotency-Key": _key()}

            async def first():
                return await ac.post("/slow", headers=headers)

            task = asyncio.create_task(first())
            await asyncio.sleep(0.3)  # arrive while first is in flight
            second = await ac.post("/slow", headers=headers)
            release.set()
            return await task, second

    first_resp, second_resp = asyncio.run(main())
    assert first_resp.status_code == 200
    assert second_resp.status_code == 409


def test_decorator_drops_record_when_handler_raises():
    store = MemoryStore()
    calls = {"n": 0}
    app = FastAPI()

    @app.post("/flaky")
    @idempotent(store=store)
    async def flaky(request: Request):
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return {"attempt": calls["n"]}

    async def main():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://test") as ac:
            headers = {"Idempotency-Key": _key()}
            with pytest.raises(RuntimeError):
                await ac.post("/flaky", headers=headers)
            return await ac.post("/flaky", headers=headers)

    retry = asyncio.run(main())
    assert retry.status_code == 200
    assert retry.json() == {"attempt": 2}
