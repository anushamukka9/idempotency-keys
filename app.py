"""Example API: a charge endpoint protected by idempotency keys.

Run with:  uvicorn app:app --reload

Without an Idempotency-Key header, POST /charge double-charges on retry
(the bug from the tutorial). With a key, the retry is free.
"""

import uuid

from fastapi import FastAPI

from idempotency import IdempotencyMiddleware

app = FastAPI(title="Idempotent Charges API")
app.add_middleware(IdempotencyMiddleware)

charges = []  # stand-in for your database


@app.post("/charge")
def charge(amount_cents: int, customer_id: str):
    charge_id = f"ch_{uuid.uuid4().hex[:12]}"
    charges.append({"id": charge_id, "amount_cents": amount_cents,
                    "customer_id": customer_id})
    return {"charge_id": charge_id, "status": "captured"}


@app.get("/charges")
def list_charges():
    return {"count": len(charges), "charges": charges}
