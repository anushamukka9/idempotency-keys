"""Demo: the Flask variant of the idempotency guard.

Run from the repo root:

    python examples/flask_demo.py
"""

import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import Flask, jsonify

from flask_idempotency import FlaskIdempotency
from stores import MemoryStore

orders = []

app = Flask(__name__)
FlaskIdempotency(app, store=MemoryStore())


@app.post("/order")
def create_order():
    order_id = f"ord_{uuid.uuid4().hex[:8]}"
    orders.append(order_id)
    return jsonify(order_id=order_id), 201


def main():
    client = app.test_client()
    key = str(uuid.uuid4())
    headers = {"Idempotency-Key": key}

    first = client.post("/order", json={"sku": "widget"}, headers=headers)
    retry = client.post("/order", json={"sku": "widget"}, headers=headers)
    print("first: ", first.status_code, first.get_json(),
          "| replayed:", first.headers["Idempotent-Replayed"])
    print("retry: ", retry.status_code, retry.get_json(),
          "| replayed:", retry.headers["Idempotent-Replayed"])

    bad = client.post("/order", json={"sku": "gadget"}, headers=headers)
    print("changed payload:", bad.status_code, bad.get_json())

    print("orders executed:", len(orders), "(expected 1)")
    assert first.status_code == 201
    assert retry.get_json() == first.get_json()
    assert retry.headers["Idempotent-Replayed"] == "true"
    assert bad.status_code == 422
    assert len(orders) == 1
    print("OK")


if __name__ == "__main__":
    main()
