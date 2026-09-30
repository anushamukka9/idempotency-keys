"""Demo: the @idempotent per-endpoint decorator.

Only the decorated route is guarded; the rest of the app runs normally.
Run from the repo root:

    python examples/decorator_demo.py
"""

import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from idempotency import idempotent
from stores import MemoryStore

store = MemoryStore()
refunds = []

app = FastAPI()


@app.post("/refund")
@idempotent(store=store)
async def refund(request: Request, amount_cents: int):
    refund_id = f"rf_{uuid.uuid4().hex[:8]}"
    refunds.append(refund_id)
    return {"refund_id": refund_id, "status": "issued"}


@app.post("/ping")
def ping():
    # Not decorated: runs every time, key header or not.
    return {"pong": True}


def main():
    client = TestClient(app)
    key = str(uuid.uuid4())
    headers = {"Idempotency-Key": key}

    first = client.post("/refund?amount_cents=1200", headers=headers)
    retry = client.post("/refund?amount_cents=1200", headers=headers)
    print("first: ", first.status_code, first.json(),
          "| replayed:", first.headers["idempotent-replayed"])
    print("retry: ", retry.status_code, retry.json(),
          "| replayed:", retry.headers["idempotent-replayed"])

    bad = client.post("/refund?amount_cents=9999", headers=headers)
    print("changed payload:", bad.status_code, bad.json()["detail"][:40], "...")

    # The undecorated route ignores the header entirely.
    client.post("/ping", headers=headers)
    client.post("/ping", headers=headers)
    print("refunds executed:", len(refunds), "(expected 1)")

    assert retry.json() == first.json()
    assert retry.headers["idempotent-replayed"] == "true"
    assert bad.status_code == 422
    assert len(refunds) == 1
    print("OK")


if __name__ == "__main__":
    main()
