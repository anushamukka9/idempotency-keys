"""Tests for the storage backends, fingerprint options, and middleware config."""

import time
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from idempotency import (
    FingerprintOptions,
    IdempotencyMiddleware,
    _idempotency_store,
    _reset_store,
)
from stores import (
    FingerprintMismatch,
    MemoryStore,
    SQLiteStore,
    redis_store_or_memory,
)


def _exercise_contract(store):
    """The behavior every backend must honor."""
    # First claim wins.
    assert store.put_in_flight("k1", "fp-a", 60) == "claimed"
    # Same fingerprint while in flight.
    assert store.put_in_flight("k1", "fp-a", 60) == "in-flight"
    # Same key, different fingerprint.
    with pytest.raises(FingerprintMismatch):
        store.put_in_flight("k1", "fp-b", 60)
    # Complete, then the key replays.
    store.complete("k1", 200, b'{"ok":true}', {"x-a": "b"}, "application/json")
    assert store.put_in_flight("k1", "fp-a", 60) == "completed"
    record = store.get("k1")
    assert record["status"] == "completed"
    assert record["status_code"] == 200
    assert record["response_body"] == b'{"ok":true}'
    assert record["headers"] == {"x-a": "b"}
    assert record["media_type"] == "application/json"
    # Discard lets the client retry fresh.
    assert store.put_in_flight("k2", "fp", 60) == "claimed"
    store.discard("k2")
    assert store.get("k2") is None
    assert store.put_in_flight("k2", "fp", 60) == "claimed"
    # Expiry makes the key reusable.
    assert store.put_in_flight("k3", "fp", 0.05) == "claimed"
    time.sleep(0.15)
    assert store.get("k3") is None
    assert store.put_in_flight("k3", "fp", 60) == "claimed"
    # Pruning removes expired records and reports the count.
    assert store.put_in_flight("k4", "fp", 0.05) == "claimed"
    time.sleep(0.15)
    assert store.prune_expired() >= 1
    assert store.get("k4") is None
    store.clear()


def test_memory_store_contract():
    _exercise_contract(MemoryStore())


def test_sqlite_store_contract(tmp_path):
    _exercise_contract(SQLiteStore(str(tmp_path / "keys.db")))


def test_sqlite_survives_reopen(tmp_path):
    path = str(tmp_path / "keys.db")
    first = SQLiteStore(path)
    first.put_in_flight("k", "fp", 3600)
    first.complete("k", 201, b"hi", {}, "text/plain")
    first.close()

    second = SQLiteStore(path)
    record = second.get("k")
    assert record["status"] == "completed"
    assert record["status_code"] == 201
    assert record["response_body"] == b"hi"
    second.close()


def test_memory_store_is_a_mutable_mapping():
    store = MemoryStore()
    store["a"] = {"x": 1}
    assert store["a"] == {"x": 1}
    assert "a" in store
    assert len(store) == 1
    assert list(store) == ["a"]
    del store["a"]
    assert len(store) == 0
    store["b"] = {"x": 2}
    store.clear()
    assert len(store) == 0


def test_redis_fallback_returns_memory_when_unavailable():
    # The redis package is not installed in this environment, so this
    # exercises the missing-package branch. Against a bad URL with the
    # package installed it exercises the unreachable-server branch.
    with pytest.warns(UserWarning, match="in-memory"):
        store = redis_store_or_memory("redis://127.0.0.1:9/0",
                                      socket_timeout=1,
                                      socket_connect_timeout=1)
    assert isinstance(store, MemoryStore)


def _fastapi_app(**middleware_kwargs):
    _reset_store()
    app = FastAPI()
    app.add_middleware(IdempotencyMiddleware, **middleware_kwargs)
    calls = {"n": 0}

    @app.post("/x")
    def x(a: int = 0):
        calls["n"] += 1
        return {"n": calls["n"]}

    return app, calls


def test_fingerprint_can_ignore_query_string():
    app, calls = _fastapi_app(
        fingerprint_options=FingerprintOptions(include_query=False))
    client = TestClient(app)
    headers = {"Idempotency-Key": str(uuid.uuid4())}
    first = client.post("/x?a=1", headers=headers)
    second = client.post("/x?a=2", headers=headers)
    assert first.status_code == 200
    assert second.headers["idempotent-replayed"] == "true"
    assert second.json() == first.json()
    assert calls["n"] == 1


def test_fingerprint_can_scope_by_header():
    app, _ = _fastapi_app(
        fingerprint_options=FingerprintOptions(include_headers=("X-Tenant",)))
    client = TestClient(app)
    key = str(uuid.uuid4())
    ok = client.post("/x", headers={"Idempotency-Key": key, "X-Tenant": "a"})
    clash = client.post("/x", headers={"Idempotency-Key": key, "X-Tenant": "b"})
    assert ok.status_code == 200
    assert clash.status_code == 422


def test_middleware_accepts_custom_key_header_and_store():
    _reset_store()
    store = MemoryStore()
    app = FastAPI()
    app.add_middleware(IdempotencyMiddleware, store=store,
                       key_header="X-Request-Id")

    @app.post("/x")
    def x():
        return {"ok": True}

    client = TestClient(app)
    key = str(uuid.uuid4())
    first = client.post("/x", headers={"X-Request-Id": key})
    second = client.post("/x", headers={"X-Request-Id": key})
    # The default header is ignored now.
    third = client.post("/x", headers={"Idempotency-Key": key})
    assert second.headers["idempotent-replayed"] == "true"
    assert "idempotent-replayed" not in third.headers
    # The private store got the records, not the shared default one.
    assert len(store) == 1
    assert len(_idempotency_store) == 0


def test_middleware_ttl_param():
    app, calls = _fastapi_app(ttl_seconds=0.2)
    client = TestClient(app)
    headers = {"Idempotency-Key": str(uuid.uuid4())}
    first = client.post("/x", headers=headers)
    time.sleep(0.4)
    second = client.post("/x", headers=headers)
    assert first.json()["n"] == 1
    assert second.json()["n"] == 2
    assert calls["n"] == 2
