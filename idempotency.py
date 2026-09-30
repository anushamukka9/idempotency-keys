"""Idempotency-key middleware for FastAPI (and plain Starlette).

Companion code for the tutorial "Add Idempotency Keys to Your API in an
Afternoon" (https://anushamukka.com/posts/add-idempotency-keys-to-your-api/).

The client sends an `Idempotency-Key` header holding a unique value per
logical operation (a UUID generated once on the client). The middleware:

  * fingerprints the request: key + method + path + query string + body
    (tunable via FingerprintOptions),
  * rejects a reused key that arrives with a different payload (422),
  * replays the recorded response when a completed key is retried,
  * returns 409 while the first request with a key is still in flight,
  * drops the in-flight record if the handler raises, so the client can
    retry with the same key instead of wedging behind a dead record,
  * expires keys after the TTL (KEY_TTL_SECONDS by default).

Storage is pluggable (see stores.py): MemoryStore by default, with
SQLiteStore and RedisStore for durability and sharing across workers.

Two ways to use it:

  * middleware on the whole app (opt-out per request: no header, no check),
  * the @idempotent decorator on individual routes (opt-in per endpoint).
"""

import functools
import hashlib
import inspect
import json
import time
from dataclasses import dataclass, field

from fastapi import Request
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response as StarletteResponse
from starlette.responses import StreamingResponse

from stores import FingerprintMismatch, MemoryStore

KEY_TTL_SECONDS = 24 * 60 * 60  # keep keys for a day

# The default store. Tests reset it via _reset_store(); the middleware
# uses it unless you pass store=... explicitly.
_default_store = MemoryStore()
_idempotency_store = _default_store


@dataclass(frozen=True)
class FingerprintOptions:
    """What goes into the request fingerprint.

    * ``include_query``: fold the query string into the fingerprint.
      Turn it off if the same logical operation may arrive with
      reordered or cosmetic query params.
    * ``include_headers``: header names (case-insensitive) to fold in,
      e.g. ("X-Tenant-Id",) to scope keys per tenant.
    * ``hash_body``: fold the request body in. Turn it off for huge
      bodies you would rather not read twice.
    """
    include_query: bool = True
    include_headers: tuple = ()
    hash_body: bool = True


def _fingerprint(key: str, method: str, path: str, query: str, body: bytes,
                 headers=None, options: FingerprintOptions | None = None) -> str:
    opts = options or FingerprintOptions()
    digest = hashlib.sha256()
    parts = [key, method, path]
    if opts.include_query:
        parts.append(query)
    digest.update(b"\x00".join(p.encode("utf-8") for p in parts))
    digest.update(b"\x00")
    if opts.include_headers and headers is not None:
        wanted = {h.lower() for h in opts.include_headers}
        items = headers.items() if hasattr(headers, "items") else headers
        selected = sorted((k.lower(), v) for k, v in items
                          if k.lower() in wanted)
        for name, value in selected:
            digest.update(name.encode("utf-8"))
            digest.update(b"=")
            digest.update(value.encode("utf-8"))
            digest.update(b"\x00")
    if opts.hash_body:
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


def _replay_response(record: dict) -> Response:
    headers = dict(record["headers"])
    headers["Idempotent-Replayed"] = "true"
    return Response(
        content=record["response_body"],
        status_code=record["status_code"],
        headers=headers,
        media_type=record["media_type"],
    )


def _mismatch_response() -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"detail": "Idempotency-Key was already used "
                           "with a different request."},
    )


def _in_flight_response() -> JSONResponse:
    return JSONResponse(
        status_code=409,
        content={"detail": "A request with this Idempotency-Key "
                           "is already in flight."},
    )


class IdempotencyMiddleware(BaseHTTPMiddleware):
    """Guard every request carrying an idempotency-key header.

    Args:
        store: a stores.BaseIdempotencyStore. Defaults to the shared
            in-memory store.
        ttl_seconds: how long a key lives, counted from first claim.
            None means KEY_TTL_SECONDS (read at request time).
        key_header: the header clients send. Defaults to "Idempotency-Key".
        fingerprint_options: a FingerprintOptions, or None for defaults.
    """

    def __init__(self, app, store=None, ttl_seconds: float | None = None,
                 key_header: str = "Idempotency-Key",
                 fingerprint_options: FingerprintOptions | None = None):
        super().__init__(app)
        self.store = store if store is not None else _default_store
        self.ttl_seconds = ttl_seconds
        self.key_header = key_header
        self.fingerprint_options = fingerprint_options

    def _ttl(self) -> float:
        return self.ttl_seconds if self.ttl_seconds is not None else KEY_TTL_SECONDS

    async def dispatch(self, request: Request, call_next):
        key = request.headers.get(self.key_header)
        if not key:
            # No key, no idempotency. The endpoint runs as before.
            return await call_next(request)

        body = await request.body()
        fingerprint = _fingerprint(
            key, request.method, request.url.path,
            str(request.url.query), body,
            request.headers, self.fingerprint_options,
        )
        ttl = self._ttl()
        self.store.prune_expired(time.time())

        try:
            claim = self.store.put_in_flight(key, fingerprint, ttl)
        except FingerprintMismatch:
            return _mismatch_response()

        if claim == "completed":
            record = self.store.get(key)
            if record is not None:
                return _replay_response(record)
            # Expired between claim and read; start over.
            claim = self.store.put_in_flight(key, fingerprint, ttl)

        if claim == "in-flight":
            return _in_flight_response()

        try:
            response = await call_next(request)
        except Exception:
            # The handler blew up. Remove the record so the client
            # can retry with the same key.
            self.store.discard(key)
            raise

        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk

        self.store.complete(
            key,
            status_code=response.status_code,
            response_body=response_body,
            headers=dict(response.headers),
            media_type=response.media_type,
        )

        headers = dict(response.headers)
        headers["Idempotent-Replayed"] = "false"
        return Response(
            content=response_body,
            status_code=response.status_code,
            headers=headers,
            media_type=response.media_type,
        )


def _coerce_result(result):
    """Turn an endpoint return value into (response, record fields).

    Supports Starlette Responses and JSON-serializable values.
    Streaming responses are refused: we cannot replay a stream.
    """
    if isinstance(result, StarletteResponse):
        if isinstance(result, StreamingResponse):
            raise TypeError(
                "@idempotent does not support streaming responses; "
                "return JSON-serializable data instead")
        body = result.body
        headers = dict(result.headers)
        response = StarletteResponse(
            content=body,
            status_code=result.status_code,
            headers=headers,
            media_type=result.media_type,
        )
        record = {
            "status_code": result.status_code,
            "response_body": body,
            "headers": headers,
            "media_type": result.media_type,
        }
        return response, record
    payload = jsonable_encoder(result)
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    response = StarletteResponse(
        content=body, status_code=200, media_type="application/json")
    record = {
        "status_code": 200,
        "response_body": body,
        "headers": {},
        "media_type": "application/json",
    }
    return response, record


def idempotent(store=None, ttl_seconds: float | None = None,
               key_header: str = "Idempotency-Key",
               fingerprint_options: FingerprintOptions | None = None):
    """Per-endpoint idempotency for FastAPI/Starlette routes.

    Apply UNDER the route decorator, on endpoints that accept a
    ``request: Request`` parameter::

        @app.post("/charge")
        @idempotent()
        async def charge(request: Request, amount_cents: int):
            ...

    Endpoints must return JSON-serializable data or a Starlette
    Response. Requests without the key header run normally.
    """

    def decorator(func):
        sig = inspect.signature(func)
        if "request" not in sig.parameters:
            raise TypeError(
                "@idempotent endpoints must accept a `request` parameter")
        is_coro = inspect.iscoroutinefunction(func)
        resolved_store = store if store is not None else _default_store

        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            bound = sig.bind_partial(*args, **kwargs)
            request = bound.arguments.get("request")
            key = request.headers.get(key_header) if request is not None else None
            if not key:
                result = func(*args, **kwargs)
                return await result if is_coro else result

            body = await request.body()
            fingerprint = _fingerprint(
                key, request.method, request.url.path,
                str(request.url.query), body,
                request.headers, fingerprint_options,
            )
            ttl = (ttl_seconds if ttl_seconds is not None
                   else KEY_TTL_SECONDS)
            resolved_store.prune_expired(time.time())

            try:
                claim = resolved_store.put_in_flight(key, fingerprint, ttl)
            except FingerprintMismatch:
                return _mismatch_response()

            if claim == "completed":
                record = resolved_store.get(key)
                if record is not None:
                    return _replay_response(record)
                claim = resolved_store.put_in_flight(key, fingerprint, ttl)

            if claim == "in-flight":
                return _in_flight_response()

            try:
                result = func(*args, **kwargs)
                if is_coro:
                    result = await result
            except Exception:
                resolved_store.discard(key)
                raise

            response, record_fields = _coerce_result(result)
            resolved_store.complete(key, **record_fields)
            response.headers["Idempotent-Replayed"] = "false"
            return response

        return wrapper

    return decorator
