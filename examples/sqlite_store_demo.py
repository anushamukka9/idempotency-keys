"""Demo: FastAPI + SQLite-backed idempotency store.

Keys survive a "restart" (a fresh store on the same file still replays),
which the in-memory store cannot do. Run from the repo root:

    python examples/sqlite_store_demo.py
"""

import os
import sys
import tempfile
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI
from fastapi.testclient import TestClient

from idempotency import IdempotencyMiddleware
from stores import SQLiteStore

DB = os.path.join(tempfile.gettempdir(), "idempotency-demo.db")
if os.path.exists(DB):
    os.remove(DB)

charges = []


def build_app(store):
    app = FastAPI()
    app.add_middleware(IdempotencyMiddleware, store=store)

    @app.post("/charge")
    def charge(amount_cents: int):
        charge_id = f"ch_{uuid.uuid4().hex[:8]}"
        charges.append(charge_id)
        return {"charge_id": charge_id}

    return app


def main():
    key = str(uuid.uuid4())
    headers = {"Idempotency-Key": key}

    # First boot.
    store = SQLiteStore(DB)
    client = TestClient(build_app(store))
    first = client.post("/charge?amount_cents=5000", headers=headers)
    print("first:            ", first.status_code, first.json(),
          "| replayed:", first.headers["idempotent-replayed"])

    # "Restart": new store object, same file. The key still replays.
    store.close()
    store2 = SQLiteStore(DB)
    client2 = TestClient(build_app(store2))
    retry = client2.post("/charge?amount_cents=5000", headers=headers)
    print("retry after restart:", retry.status_code, retry.json(),
          "| replayed:", retry.headers["idempotent-replayed"])

    # Same key, different payload: rejected, no new charge.
    bad = client2.post("/charge?amount_cents=9999", headers=headers)
    print("different payload:", bad.status_code, bad.json())

    print("charges executed:", len(charges), "(expected 1)")
    assert len(charges) == 1
    assert retry.json() == first.json()
    store2.close()
    os.remove(DB)
    print("OK")


if __name__ == "__main__":
    main()
