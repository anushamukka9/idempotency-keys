"""Idempotency-key guard for Flask.

Same promise as the FastAPI middleware in idempotency.py: a repeated
`Idempotency-Key` header repeats the *result* instead of repeating the
*work*. Needs the ``flask`` package.

Usage::

    from flask import Flask
    from flask_idempotency import FlaskIdempotency
    from stores import SQLiteStore

    app = Flask(__name__)
    idem = FlaskIdempotency(store=SQLiteStore("keys.db"))
    idem.init_app(app)

Requests without the header pass through untouched. A reused key with a
different payload gets 422; a key whose first request is still running
gets 409; a retried completed key replays the stored response with an
`Idempotent-Replayed: true` header. If the view raises, Flask turns it
into a 5xx response, and any 5xx response is treated as a failure: the
in-flight record is dropped so the client can retry with the same key.
Only non-5xx responses are cached for replay.
"""

import time

from flask import Response as FlaskResponse
from flask import g, jsonify, request

from idempotency import KEY_TTL_SECONDS, FingerprintOptions, _fingerprint
from stores import FingerprintMismatch, MemoryStore

_MISMATCH_DETAIL = "Idempotency-Key was already used with a different request."
_IN_FLIGHT_DETAIL = ("A request with this Idempotency-Key is already "
                     "in flight.")


class FlaskIdempotency:
    """Flask extension wiring idempotency keys into before/after hooks.

    Args:
        app: optional Flask app; otherwise call init_app later.
        store: a stores.BaseIdempotencyStore. Defaults to a private
            in-memory store owned by this extension instance.
        ttl_seconds: key lifetime from first claim. None means
            KEY_TTL_SECONDS.
        key_header: the header clients send. Defaults to "Idempotency-Key".
        fingerprint_options: a FingerprintOptions, or None for defaults.
    """

    def __init__(self, app=None, store=None, ttl_seconds: float | None = None,
                 key_header: str = "Idempotency-Key",
                 fingerprint_options: FingerprintOptions | None = None):
        self.store = store if store is not None else MemoryStore()
        self.ttl_seconds = ttl_seconds
        self.key_header = key_header
        self.fingerprint_options = fingerprint_options
        if app is not None:
            self.init_app(app)

    def init_app(self, app) -> None:
        app.before_request(self._before_request)
        app.after_request(self._after_request)
        app.teardown_request(self._teardown_request)

    def _ttl(self) -> float:
        return self.ttl_seconds if self.ttl_seconds is not None else KEY_TTL_SECONDS

    def _before_request(self):
        key = request.headers.get(self.key_header)
        if not key:
            return None
        body = request.get_data() or b""
        fingerprint = _fingerprint(
            key, request.method, request.path,
            request.query_string.decode("latin-1"), body,
            request.headers, self.fingerprint_options,
        )
        ttl = self._ttl()
        self.store.prune_expired(time.time())

        try:
            claim = self.store.put_in_flight(key, fingerprint, ttl)
        except FingerprintMismatch:
            return jsonify(detail=_MISMATCH_DETAIL), 422

        if claim == "completed":
            record = self.store.get(key)
            if record is not None:
                return self._replay(record)
            # Expired between claim and read; start over.
            claim = self.store.put_in_flight(key, fingerprint, ttl)

        if claim == "in-flight":
            return jsonify(detail=_IN_FLIGHT_DETAIL), 409

        g.idempotency_key = key
        g.idempotency_recorded = False
        return None

    def _after_request(self, response):
        key = getattr(g, "idempotency_key", None)
        if key is None:
            return response
        if response.status_code >= 500:
            # The view failed. (Flask turns an unhandled view exception
            # into a 5xx response, so this is also the "handler raised"
            # path.) A 5xx means the outcome is unknown, so drop the
            # record instead of caching the error page: the client may
            # retry with the same key.
            self.store.discard(key)
        else:
            body = response.get_data() or b""
            self.store.complete(
                key,
                status_code=response.status_code,
                response_body=body,
                headers=dict(response.headers),
                media_type=response.mimetype or None,
            )
            response.headers["Idempotent-Replayed"] = "false"
        g.idempotency_recorded = True
        return response

    def _teardown_request(self, exc):
        # Teardown runs even when the view raises (after_request is
        # skipped then), so the dead record is dropped here.
        if exc is None:
            return
        key = getattr(g, "idempotency_key", None)
        if key is not None and not getattr(g, "idempotency_recorded", False):
            self.store.discard(key)

    @staticmethod
    def _replay(record: dict):
        headers = dict(record["headers"])
        headers["Idempotent-Replayed"] = "true"
        return FlaskResponse(
            response=record["response_body"],
            status=record["status_code"],
            headers=headers,
            content_type=record["media_type"],
        )
