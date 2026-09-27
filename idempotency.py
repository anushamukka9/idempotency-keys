"""Idempotency-key middleware for FastAPI.

Companion code for the tutorial "Add Idempotency Keys to Your API in an
Afternoon" (https://anushamukka.com/posts/add-idempotency-keys-to-your-api/).

The client sends an `Idempotency-Key` header holding a unique value per
logical operation (a UUID generated once on the client). The middleware:

  * fingerprints the request: key + method + path + query string + body,
  * rejects a reused key that arrives with a different payload (422),
  * replays the recorded response when a completed key is retried,
  * returns 409 while the first request with a key is still in flight,
  * drops the in-flight record if the handler raises, so the client can
    retry with the same key instead of wedging behind a dead record,
  * expires keys after KEY_TTL_SECONDS.

The store here is a plain dict so the whole mechanism is visible. In
production this is Redis: shared, persistent, and with an atomic
check-and-set for the in-flight mark.
"""

import hashlib
import time

from fastapi import Request
from fastapi.responses import JSONResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware

KEY_TTL_SECONDS = 24 * 60 * 60  # keep keys for a day

# key -> record. In production this is Redis, not a dict.
_idempotency_store: dict = {}


def _fingerprint(key: str, method: str, path: str, query: str, body: bytes) -> str:
    digest = hashlib.sha256()
    digest.update(key.encode("utf-8"))
    digest.update(method.encode("utf-8"))
    digest.update(path.encode("utf-8"))
    digest.update(query.encode("utf-8"))
    digest.update(body)
    return digest.hexdigest()


def _prune_expired(now: float) -> None:
    expired = [k for k, v in _idempotency_store.items()
               if v["expires_at"] <= now]
    for k in expired:
        del _idempotency_store[k]


def _reset_store() -> None:
    """Clear the store. Test helper only."""
    _idempotency_store.clear()


class IdempotencyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        key = request.headers.get("Idempotency-Key")
        if not key:
            # No key, no idempotency. The endpoint runs as before.
            return await call_next(request)

        body = await request.body()
        fingerprint = _fingerprint(
            key, request.method, request.url.path,
            str(request.url.query), body,
        )
        now = time.time()
        _prune_expired(now)

        record = _idempotency_store.get(key)

        if record is not None and record["fingerprint"] != fingerprint:
            return JSONResponse(
                status_code=422,
                content={"detail": "Idempotency-Key was already used "
                                  "with a different request."},
            )

        if record is not None and record["status"] == "completed":
            headers = dict(record["headers"])
            headers["Idempotent-Replayed"] = "true"
            return Response(
                content=record["response_body"],
                status_code=record["status_code"],
                headers=headers,
                media_type=record["media_type"],
            )

        if record is not None and record["status"] == "in-flight":
            return JSONResponse(
                status_code=409,
                content={"detail": "A request with this Idempotency-Key "
                                   "is already in flight."},
            )

        _idempotency_store[key] = {
            "status": "in-flight",
            "fingerprint": fingerprint,
            "expires_at": now + KEY_TTL_SECONDS,
        }

        try:
            response = await call_next(request)
        except Exception:
            # The handler blew up. Remove the record so the client
            # can retry with the same key.
            _idempotency_store.pop(key, None)
            raise

        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk

        _idempotency_store[key].update({
            "status": "completed",
            "status_code": response.status_code,
            "response_body": response_body,
            "headers": dict(response.headers),
            "media_type": response.media_type,
        })

        headers = dict(response.headers)
        headers["Idempotent-Replayed"] = "false"
        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=headers,
            media_type=response.media_type,
        )
