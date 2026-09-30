"""Tests for the Flask variant of the idempotency guard."""

import uuid

import pytest
from flask import Flask, jsonify

from flask_idempotency import FlaskIdempotency
from stores import MemoryStore, SQLiteStore


def _key():
    return str(uuid.uuid4())


@pytest.fixture()
def wired():
    store = MemoryStore()
    app = Flask(__name__)
    FlaskIdempotency(app, store=store)
    calls = {"n": 0}

    @app.post("/order")
    def order():
        calls["n"] += 1
        return jsonify(order_n=calls["n"]), 201

    return app, store, calls


def test_flask_replays_retry(wired):
    app, _, calls = wired
    client = app.test_client()
    headers = {"Idempotency-Key": _key()}

    first = client.post("/order", json={"sku": "widget"}, headers=headers)
    retry = client.post("/order", json={"sku": "widget"}, headers=headers)

    assert first.status_code == 201
    assert retry.get_json() == first.get_json()
    assert first.headers["Idempotent-Replayed"] == "false"
    assert retry.headers["Idempotent-Replayed"] == "true"
    assert calls["n"] == 1


def test_flask_ignores_requests_without_a_key(wired):
    app, _, calls = wired
    client = app.test_client()
    client.post("/order", json={"sku": "widget"})
    client.post("/order", json={"sku": "widget"})
    assert calls["n"] == 2


def test_flask_rejects_changed_payload(wired):
    app, _, calls = wired
    client = app.test_client()
    headers = {"Idempotency-Key": _key()}
    ok = client.post("/order", json={"sku": "widget"}, headers=headers)
    bad = client.post("/order", json={"sku": "gadget"}, headers=headers)
    assert ok.status_code == 201
    assert bad.status_code == 422
    assert "different request" in bad.get_json()["detail"]
    assert calls["n"] == 1


def test_flask_failed_view_drops_record_so_client_can_retry():
    store = MemoryStore()
    app = Flask(__name__)
    FlaskIdempotency(app, store=store)
    calls = {"n": 0}

    @app.post("/flaky")
    def flaky():
        calls["n"] += 1
        if calls["n"] == 1:
            raise RuntimeError("boom")
        return jsonify(attempt=calls["n"])

    client = app.test_client()
    headers = {"Idempotency-Key": _key()}
    # The view blew up; Flask converts it to a 500 response.
    first = client.post("/flaky", json={}, headers=headers)
    assert first.status_code == 500
    # The 500 was not cached, so the retry runs the view again.
    retry = client.post("/flaky", json={}, headers=headers)
    assert retry.status_code == 200
    assert retry.get_json() == {"attempt": 2}
    assert retry.headers["Idempotent-Replayed"] == "false"


def test_flask_custom_key_header():
    store = MemoryStore()
    app = Flask(__name__)
    FlaskIdempotency(app, store=store, key_header="X-Request-Id")

    @app.post("/x")
    def x():
        return jsonify(ok=True)

    client = app.test_client()
    key = _key()
    first = client.post("/x", headers={"X-Request-Id": key})
    retry = client.post("/x", headers={"X-Request-Id": key})
    assert retry.headers["Idempotent-Replayed"] == "true"
    assert first.get_json() == {"ok": True}


def test_flask_works_with_sqlite_store(tmp_path):
    store = SQLiteStore(str(tmp_path / "flask.db"))
    app = Flask(__name__)
    FlaskIdempotency(app, store=store)
    calls = {"n": 0}

    @app.post("/order")
    def order():
        calls["n"] += 1
        return jsonify(order_n=calls["n"])

    client = app.test_client()
    headers = {"Idempotency-Key": _key()}
    first = client.post("/order", json={}, headers=headers)
    retry = client.post("/order", json={}, headers=headers)
    assert retry.get_json() == first.get_json()
    assert retry.headers["Idempotent-Replayed"] == "true"
    assert calls["n"] == 1
    store.close()
