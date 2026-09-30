# Idempotency Keys for FastAPI (and Flask)

Companion code for the tutorial **[Add Idempotency Keys to Your API in an
Afternoon](https://anushamukka.com/posts/add-idempotency-keys-to-your-api/)**
by Anusha Mukka. The tutorial walks through the original version line by
line; this repo is the grown-up, runnable version.

The problem: a client retries a `POST` after a lost response, and the API
executes it twice. The fix: the client sends an `Idempotency-Key` header
with a unique value per logical operation (a UUID generated once on the
client), and this library promises that repeating the same key repeats
the *result* instead of repeating the *work*.

## How it works

`IdempotencyMiddleware` (in `idempotency.py`):

- fingerprints each request: key + method + path + query string + body
  (tunable with `FingerprintOptions`),
- rejects a reused key that arrives with a **different** payload (`422`),
- replays the recorded response when a completed key is retried
  (marked with an `Idempotent-Replayed: true` header),
- returns `409` while the first request with a key is still in flight,
- drops the in-flight record if the handler raises, so the client can
  retry with the same key,
- expires keys after a TTL (`KEY_TTL_SECONDS`, 24 hours by default).

Requests without a key run the naive path unchanged, so idempotency is
opt-in per request and existing clients keep working.

Prefer guarding one route instead of the whole app? Use the
`@idempotent` decorator (below). On Flask? See `flask_idempotency.py`.

## Quickstart

Python 3.10 or newer.

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

## Storage backends

The middleware talks to a store through a small contract
(`stores.BaseIdempotencyStore`), so you can swap backends without
touching request handling. The in-flight claim is atomic on every
backend: exactly one concurrent request with a key runs the handler.

| Backend | Shares across workers | Survives restart | Needs |
|---|---|---|---|
| `MemoryStore` (default) | no | no | nothing |
| `SQLiteStore(path)` | no | yes | nothing (stdlib) |
| `RedisStore(url)` | yes | yes | `redis` package + server |

```python
from idempotency import IdempotencyMiddleware
from stores import SQLiteStore, RedisStore, redis_store_or_memory

# Single node, durable:
app.add_middleware(IdempotencyMiddleware, store=SQLiteStore("keys.db"))

# Multi-worker, if you run Redis:
app.add_middleware(IdempotencyMiddleware, store=RedisStore("redis://localhost:6379/0"))

# Redis when available, memory otherwise (warns on fallback):
app.add_middleware(IdempotencyMiddleware, store=redis_store_or_memory())
```

Redis is optional. `pip install redis` only if you use `RedisStore`;
everything else works without it.

## Per-endpoint decorator

When only the money-moving routes need guarding:

```python
from fastapi import Request
from idempotency import idempotent

@app.post("/refund")
@idempotent()                      # apply UNDER the route decorator
async def refund(request: Request, amount_cents: int):
    ...
```

The endpoint must accept a `request: Request` parameter and return
JSON-serializable data or a Starlette `Response` (streaming responses
are refused: a stream cannot be replayed). Requests without the header
run normally. It takes the same `store=`, `ttl_seconds=`,
`key_header=`, and `fingerprint_options=` arguments as the middleware.

## Flask variant

```python
from flask import Flask
from flask_idempotency import FlaskIdempotency
from stores import SQLiteStore

app = Flask(__name__)
FlaskIdempotency(app, store=SQLiteStore("keys.db"))
```

Same semantics as the middleware, with one Flask-shaped difference:
Flask converts an unhandled view exception into a 5xx response before
the after-request hook runs, so any 5xx response is treated as a
failure. The record is dropped (never cached), and the client may retry
with the same key. Only non-5xx responses are stored for replay.

## Fingerprint options

What counts as "the same request" is configurable:

```python
from idempotency import FingerprintOptions

# Ignore cosmetic query-string differences:
FingerprintOptions(include_query=False)

# Scope keys per tenant via a header:
FingerprintOptions(include_headers=("X-Tenant-Id",))

# Skip hashing large bodies:
FingerprintOptions(hash_body=False)
```

## API reference

**`IdempotencyMiddleware(app, store=None, ttl_seconds=None, key_header="Idempotency-Key", fingerprint_options=None)`**
`store` defaults to a shared in-memory store; `ttl_seconds=None` means
`KEY_TTL_SECONDS` (24h), counted from the first claim.

**`idempotent(store=None, ttl_seconds=None, key_header="Idempotency-Key", fingerprint_options=None)`**
Decorator for FastAPI/Starlette routes; see above.

**`FlaskIdempotency(app=None, store=None, ttl_seconds=None, key_header="Idempotency-Key", fingerprint_options=None)`**
Flask extension; call `init_app(app)` if you do not pass `app`.

**`stores.MemoryStore()`** - process-local dict, also usable as a plain
mapping. **`stores.SQLiteStore(path="idempotency.db")`** - file-backed,
`close()` when done. **`stores.RedisStore(client=None, url=...,
key_prefix="idem:")`** - shared; `ping()` to check the server.
**`stores.redis_store_or_memory(url=...)`** - Redis if it answers,
memory with a warning otherwise.

Response contract: first execution carries
`Idempotent-Replayed: false`; replays carry `Idempotent-Replayed: true`.
`422` means the key was reused with a different payload; `409` means a
request with the key is still in flight (wait and retry the same key).

## Examples

Runnable scripts in `examples/` (run from the repo root):

- `sqlite_store_demo.py` - keys survive a restart with `SQLiteStore`.
- `decorator_demo.py` - the `@idempotent` decorator on one route.
- `flask_demo.py` - the Flask guard end to end.
- `redis_fallback_demo.py` - `redis_store_or_memory()` without a server.
- `concurrent_retries.py` - 20 concurrent same-key retries, one execution.

## Run the tests

```bash
pytest -v
```

The suite covers the naive path, replay on retry, the 422 on key reuse
with a different payload, the 409 on concurrent duplicates, record
cleanup when the handler raises, key expiry, every storage backend's
contract, the decorator, the Flask variant, and fingerprint options.

## Layout

- `idempotency.py` - the middleware, the `@idempotent` decorator,
  request fingerprinting.
- `stores.py` - pluggable backends: memory, SQLite, Redis.
- `flask_idempotency.py` - the Flask variant.
- `app.py` - the example charge API the middleware protects.
- `examples/` - runnable demos.
- `tests/` - the test suite.

## Production notes

1. Pick the store for your deployment: `MemoryStore` for one process,
   `SQLiteStore` for one durable node, `RedisStore` when workers need
   to share keys. Redis expiry is server-side; the other backends prune
   lazily on each request.
2. Scope keys per account: prefix the store key with the authenticated
   account ID, or fold a tenant header into the fingerprint
   (`FingerprintOptions(include_headers=...)`).
3. Size the TTL to your retry window. Twenty-four hours covers clients
   that retry for a long time; shorter is fine if yours do not.
4. Prefer the decorator on money-moving routes over middleware on
   everything, so read-heavy endpoints never pay the fingerprint cost.
5. A `409` while in flight means "your first request is still running":
   clients should wait and retry the same key, not mint a new one.

## License

MIT. See [LICENSE](LICENSE).
