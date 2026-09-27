# Idempotency Keys for FastAPI

Companion code for the tutorial **[Add Idempotency Keys to Your API in an
Afternoon](https://anushamukka.com/posts/add-idempotency-keys-to-your-api/)**
by Anusha Mukka. The tutorial walks through every line here; this repo is
the runnable version.

The problem: a client retries a `POST` after a lost response, and the API
executes it twice. The fix: the client sends an `Idempotency-Key` header
with a unique value per logical operation, and this middleware promises
that repeating the same key repeats the *result* instead of repeating the
*work*.

## How it works

`idempotency.py` provides `IdempotencyMiddleware`, which:

- fingerprints each request: key + method + path + query string + body,
- rejects a reused key that arrives with a **different** payload (`422`),
- replays the recorded response when a completed key is retried
  (marked with an `Idempotent-Replayed: true` header),
- returns `409` while the first request with a key is still in flight,
- drops the in-flight record if the handler raises, so the client can
  retry with the same key,
- expires keys after `KEY_TTL_SECONDS` (24 hours by default).

Requests without a key run the naive path unchanged, so idempotency is
opt-in per request and existing clients keep working.

## Prerequisites

Python 3.10 or newer.

## Run it

```bash
pip install -r requirements.txt
uvicorn app:app --reload
```

Then watch the bug, then the fix:

```bash
# Without a key: two charges, one customer. This is the entire problem.
curl -s -X POST "http://127.0.0.1:8000/charge?amount_cents=5000&customer_id=cus_42"
curl -s -X POST "http://127.0.0.1:8000/charge?amount_cents=5000&customer_id=cus_42"

# With a key: the retry is free. Same charge id, Idempotent-Replayed: true.
KEY=$(uuidgen)
curl -s -i -X POST "http://127.0.0.1:8000/charge?amount_cents=5000&customer_id=cus_42" \
  -H "Idempotency-Key: $KEY" | head -20
curl -s -i -X POST "http://127.0.0.1:8000/charge?amount_cents=5000&customer_id=cus_42" \
  -H "Idempotency-Key: $KEY" | head -20

# Same key, different payload: 422, no new charge.
curl -s -X POST "http://127.0.0.1:8000/charge?amount_cents=9999&customer_id=cus_42" \
  -H "Idempotency-Key: $KEY"

curl -s "http://127.0.0.1:8000/charges" | python3 -m json.tool
```

## Run the tests

```bash
pytest -v
```

The suite covers the naive path, replay on retry, the 422 on key reuse
with a different payload, the 409 on concurrent duplicates, record
cleanup when the handler raises, and key expiry.

## Layout

- `idempotency.py` — the middleware, request fingerprinting, and the
  key store.
- `app.py` — the example charge API the middleware protects.
- `tests/test_idempotency.py` — the test suite.

## Production notes

This repo is the teaching version. For production, per the tutorial:

1. Move the store to Redis: one hash per key with a TTL, and an atomic
   check-and-set (Lua script or `SET NX`) for the in-flight mark. That
   fixes both durability across deploys and the concurrency edge.
2. Scope keys per account: prefix the store key with the authenticated
   account ID.
3. Make it opt-in per endpoint (a decorator on money-moving routes)
   instead of middleware on everything.

## License

MIT. See [LICENSE](LICENSE).
