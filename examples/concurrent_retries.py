"""Demo: 20 concurrent retries, one execution.

Fires 20 requests with the same Idempotency-Key at a slow endpoint at
once. Exactly one runs the handler; the rest get 409 while it is in
flight. Run from the repo root:

    python examples/concurrent_retries.py
"""

import asyncio
import os
import sys
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx
from fastapi import FastAPI

from idempotency import IdempotencyMiddleware, _reset_store

executions = []

app = FastAPI()
app.add_middleware(IdempotencyMiddleware)


@app.post("/slow-charge")
async def slow_charge():
    executions.append(1)
    await asyncio.sleep(1.0)  # stand-in for a slow payment call
    return {"status": "captured"}


async def main():
    _reset_store()
    executions.clear()
    key = str(uuid.uuid4())
    headers = {"Idempotency-Key": key}
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport,
                                 base_url="http://test") as client:
        responses = await asyncio.gather(*[
            client.post("/slow-charge", headers=headers)
            for _ in range(20)
        ])
    ok = sum(1 for r in responses if r.status_code == 200)
    conflicts = sum(1 for r in responses if r.status_code == 409)
    print(f"200s: {ok}, 409s: {conflicts}, handler executions: {len(executions)}")
    assert len(executions) == 1, "handler must run exactly once"
    # The winner's response is stored; a late retry replays it.
    transport2 = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport2,
                                 base_url="http://test") as client:
        late = await client.post("/slow-charge", headers=headers)
    print("late retry:", late.status_code,
          "| replayed:", late.headers["idempotent-replayed"])
    assert late.status_code == 200
    assert late.headers["idempotent-replayed"] == "true"
    print("OK")


if __name__ == "__main__":
    asyncio.run(main())
