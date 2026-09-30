"""Pluggable storage backends for idempotency keys.

The middleware and the Flask guard both talk to a store through the
``BaseIdempotencyStore`` contract, so you can swap backends without
touching any request handling:

* ``MemoryStore`` - a process-local dict guarded by a lock. Good for
  demos, single-process apps, and tests. Records vanish on restart.
* ``SQLiteStore`` - a SQLite database file, standard library only.
  Survives restarts, no extra services, fine for a single node.
* ``RedisStore`` - shared across processes and machines. The in-flight
  claim runs as one Lua script, so check-and-set stays atomic even with
  many workers. Needs the ``redis`` package and a reachable server.

Use ``redis_store_or_memory`` when Redis is nice-to-have: it returns a
``RedisStore`` when the server answers, and a ``MemoryStore`` (with a
warning) when the ``redis`` package is missing or the server is down.

Every backend stores the same record shape::

    {
        "status": "in-flight" or "completed",
        "fingerprint": str,          # request fingerprint, see idempotency.py
        "expires_at": float,        # epoch seconds; TTL counts from first claim
        "status_code": int | None,
        "response_body": bytes,
        "headers": dict,
        "media_type": str | None,
    }
"""

import abc
import base64
import json
import sqlite3
import threading
import time
import warnings
from collections.abc import MutableMapping


class FingerprintMismatch(Exception):
    """A key was reused with a different request fingerprint."""


class BaseIdempotencyStore(abc.ABC):
    """Contract every backend implements.

    ``put_in_flight`` is the heart of it: it must atomically decide
    whether this call owns the key. It returns one of:

    * ``"claimed"`` - this call created the in-flight record; run the handler.
    * ``"in-flight"`` - another request owns the key right now; answer 409.
    * ``"completed"`` - the key already finished; replay the stored response.

    It raises ``FingerprintMismatch`` when the stored fingerprint differs
    from the one given (same key, different payload: answer 422).
    """

    @abc.abstractmethod
    def put_in_flight(self, key: str, fingerprint: str, ttl_seconds: float) -> str:
        ...

    @abc.abstractmethod
    def get(self, key: str) -> dict | None:
        """Return the record for ``key``, or ``None`` if absent or expired."""

    @abc.abstractmethod
    def complete(self, key: str, status_code: int, response_body: bytes,
                 headers: dict, media_type: str | None) -> None:
        """Mark ``key`` completed and store the response for replay."""

    @abc.abstractmethod
    def discard(self, key: str) -> None:
        """Drop the record, e.g. because the handler raised."""

    @abc.abstractmethod
    def prune_expired(self, now: float | None = None) -> int:
        """Delete expired records. Returns how many were removed."""

    @abc.abstractmethod
    def clear(self) -> None:
        """Wipe the store. Mostly useful in tests."""


def _in_flight_record(fingerprint: str, ttl_seconds: float) -> dict:
    return {
        "status": "in-flight",
        "fingerprint": fingerprint,
        "expires_at": time.time() + ttl_seconds,
        "status_code": None,
        "response_body": b"",
        "headers": {},
        "media_type": None,
    }


class MemoryStore(MutableMapping, BaseIdempotencyStore):
    """Process-local store. Fast, dependency-free, not shared, not durable.

    It is also a ``MutableMapping``, so code that treated the old
    module-level dict as a dict keeps working.
    """

    def __init__(self) -> None:
        self._data: dict = {}
        self._lock = threading.Lock()

    # -- MutableMapping interface --------------------------------------
    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, value):
        with self._lock:
            self._data[key] = value

    def __delitem__(self, key):
        with self._lock:
            del self._data[key]

    def __iter__(self):
        return iter(list(self._data.keys()))

    def __len__(self):
        return len(self._data)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    # -- BaseIdempotencyStore interface ----------------------------------
    def put_in_flight(self, key: str, fingerprint: str, ttl_seconds: float) -> str:
        with self._lock:
            self._prune_locked(time.time())
            record = self._data.get(key)
            if record is None:
                self._data[key] = _in_flight_record(fingerprint, ttl_seconds)
                return "claimed"
            if record["fingerprint"] != fingerprint:
                raise FingerprintMismatch(
                    "Idempotency-Key was already used with a different request.")
            return "completed" if record["status"] == "completed" else "in-flight"

    def get(self, key: str) -> dict | None:
        with self._lock:
            record = self._data.get(key)
            if record is None:
                return None
            if record["expires_at"] <= time.time():
                del self._data[key]
                return None
            return {**record, "headers": dict(record["headers"])}

    def complete(self, key: str, status_code: int, response_body: bytes,
                 headers: dict, media_type: str | None) -> None:
        with self._lock:
            record = self._data.get(key)
            if record is None:
                return
            record.update({
                "status": "completed",
                "status_code": status_code,
                "response_body": response_body,
                "headers": dict(headers),
                "media_type": media_type,
            })

    def discard(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def prune_expired(self, now: float | None = None) -> int:
        with self._lock:
            return self._prune_locked(time.time() if now is None else now)

    def _prune_locked(self, now: float) -> int:
        expired = [k for k, v in self._data.items() if v["expires_at"] <= now]
        for k in expired:
            del self._data[k]
        return len(expired)


class SQLiteStore(BaseIdempotencyStore):
    """SQLite-backed store. One file, no services, survives restarts.

    The in-flight claim runs inside ``BEGIN IMMEDIATE``, so concurrent
    threads and processes serialize on the write lock instead of
    racing on check-then-set.
    """

    _SCHEMA = """
    CREATE TABLE IF NOT EXISTS idempotency_keys (
        key TEXT PRIMARY KEY,
        status TEXT NOT NULL,
        fingerprint TEXT NOT NULL,
        expires_at REAL NOT NULL,
        status_code INTEGER,
        response_body BLOB,
        headers TEXT,
        media_type TEXT
    )
    """

    def __init__(self, path: str = "idempotency.db") -> None:
        self.path = path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute(self._SCHEMA)
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def put_in_flight(self, key: str, fingerprint: str, ttl_seconds: float) -> str:
        now = time.time()
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                cur.execute("DELETE FROM idempotency_keys WHERE expires_at <= ?",
                            (now,))
                cur.execute("SELECT status, fingerprint FROM idempotency_keys "
                            "WHERE key = ?", (key,))
                row = cur.fetchone()
                if row is None:
                    cur.execute(
                        "INSERT INTO idempotency_keys "
                        "(key, status, fingerprint, expires_at) "
                        "VALUES (?, 'in-flight', ?, ?)",
                        (key, fingerprint, now + ttl_seconds))
                    self._conn.commit()
                    return "claimed"
                status, stored_fingerprint = row
                if stored_fingerprint != fingerprint:
                    self._conn.rollback()
                    raise FingerprintMismatch(
                        "Idempotency-Key was already used with a different request.")
                self._conn.commit()
                return "completed" if status == "completed" else "in-flight"
            except Exception:
                try:
                    self._conn.rollback()
                except sqlite3.Error:
                    pass
                raise

    def get(self, key: str) -> dict | None:
        with self._lock:
            cur = self._conn.execute(
                "SELECT status, fingerprint, expires_at, status_code, "
                "response_body, headers, media_type "
                "FROM idempotency_keys WHERE key = ?", (key,))
            row = cur.fetchone()
            if row is None:
                return None
            status, fingerprint, expires_at, status_code, body, headers_json, media_type = row
            if expires_at <= time.time():
                self._conn.execute("DELETE FROM idempotency_keys WHERE key = ?",
                                   (key,))
                self._conn.commit()
                return None
            return {
                "status": status,
                "fingerprint": fingerprint,
                "expires_at": expires_at,
                "status_code": status_code,
                "response_body": bytes(body) if body is not None else b"",
                "headers": json.loads(headers_json) if headers_json else {},
                "media_type": media_type,
            }

    def complete(self, key: str, status_code: int, response_body: bytes,
                 headers: dict, media_type: str | None) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE idempotency_keys SET status = 'completed', "
                "status_code = ?, response_body = ?, headers = ?, media_type = ? "
                "WHERE key = ?",
                (status_code, bytes(response_body), json.dumps(dict(headers)),
                 media_type, key))
            self._conn.commit()

    def discard(self, key: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM idempotency_keys WHERE key = ?", (key,))
            self._conn.commit()

    def prune_expired(self, now: float | None = None) -> int:
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM idempotency_keys WHERE expires_at <= ?",
                (time.time() if now is None else now,))
            self._conn.commit()
            return cur.rowcount

    def clear(self) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM idempotency_keys")
            self._conn.commit()


# Lua script for the atomic in-flight claim.
# Returns "claimed", "in-flight", "completed", or "mismatch".
# KEYS[1]: record key. ARGV: fingerprint, now (epoch), ttl seconds.
_CLAIM_LUA = """
local stored_fp = redis.call('HGET', KEYS[1], 'fingerprint')
local now = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
if stored_fp == false then
  redis.call('HSET', KEYS[1], 'status', 'in-flight',
             'fingerprint', ARGV[1], 'expires_at', now + ttl)
  redis.call('EXPIRE', KEYS[1], math.max(1, math.ceil(ttl)))
  return 'claimed'
end
local expires_at = tonumber(redis.call('HGET', KEYS[1], 'expires_at') or '0')
if expires_at <= now then
  redis.call('DEL', KEYS[1])
  redis.call('HSET', KEYS[1], 'status', 'in-flight',
             'fingerprint', ARGV[1], 'expires_at', now + ttl)
  redis.call('EXPIRE', KEYS[1], math.max(1, math.ceil(ttl)))
  return 'claimed'
end
if stored_fp ~= ARGV[1] then
  return 'mismatch'
end
if redis.call('HGET', KEYS[1], 'status') == 'completed' then
  return 'completed'
end
return 'in-flight'
"""

# Marks a key completed without touching its TTL (TTL counts from first claim).
_COMPLETE_LUA = """
if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
redis.call('HSET', KEYS[1], 'status', 'completed',
           'status_code', ARGV[1],
           'response_body_b64', ARGV[2],
           'headers_json', ARGV[3],
           'media_type', ARGV[4])
return 1
"""


class RedisStore(BaseIdempotencyStore):
    """Redis-backed store, shared across processes and machines.

    Needs the ``redis`` package (``pip install redis``) and a reachable
    server. Records are hashes with a server-side TTL, so expiry needs
    no pruning.
    """

    def __init__(self, client=None, url: str = "redis://localhost:6379/0",
                 key_prefix: str = "idem:", **redis_kwargs) -> None:
        try:
            import redis
        except ImportError as exc:
            raise ImportError(
                "RedisStore needs the 'redis' package: pip install redis. "
                "Use redis_store_or_memory() if Redis is optional.") from exc
        if client is not None:
            self._client = client
        else:
            self._client = redis.Redis.from_url(
                url, decode_responses=True, **redis_kwargs)
        self._prefix = key_prefix
        self._claim = self._client.register_script(_CLAIM_LUA)
        self._complete = self._client.register_script(_COMPLETE_LUA)

    def ping(self) -> bool:
        return bool(self._client.ping())

    def _rkey(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def put_in_flight(self, key: str, fingerprint: str, ttl_seconds: float) -> str:
        result = self._claim(keys=[self._rkey(key)],
                             args=[fingerprint, time.time(), ttl_seconds])
        if result == "mismatch":
            raise FingerprintMismatch(
                "Idempotency-Key was already used with a different request.")
        return result

    def get(self, key: str) -> dict | None:
        data = self._client.hgetall(self._rkey(key))
        if not data:
            return None
        if float(data.get("expires_at", "0")) <= time.time():
            self._client.delete(self._rkey(key))
            return None
        body_b64 = data.get("response_body_b64") or ""
        return {
            "status": data.get("status"),
            "fingerprint": data.get("fingerprint"),
            "expires_at": float(data.get("expires_at", "0")),
            "status_code": int(data["status_code"]) if data.get("status_code") else None,
            "response_body": base64.b64decode(body_b64) if body_b64 else b"",
            "headers": json.loads(data["headers_json"]) if data.get("headers_json") else {},
            "media_type": data.get("media_type") or None,
        }

    def complete(self, key: str, status_code: int, response_body: bytes,
                 headers: dict, media_type: str | None) -> None:
        self._complete(
            keys=[self._rkey(key)],
            args=[status_code,
                  base64.b64encode(bytes(response_body)).decode("ascii"),
                  json.dumps(dict(headers)),
                  media_type or ""])

    def discard(self, key: str) -> None:
        self._client.delete(self._rkey(key))

    def prune_expired(self, now: float | None = None) -> int:
        # The server evicts expired keys via TTL; nothing to do.
        return 0

    def clear(self) -> None:
        for rkey in self._client.scan_iter(f"{self._prefix}*"):
            self._client.delete(rkey)


def redis_store_or_memory(url: str = "redis://localhost:6379/0",
                          key_prefix: str = "idem:",
                          **redis_kwargs) -> BaseIdempotencyStore:
    """Return a ``RedisStore`` if Redis answers, else a ``MemoryStore``.

    For apps where Redis is a nice-to-have: try the real thing, fall
    back to memory with a warning instead of crashing at startup.
    """
    try:
        import redis  # noqa: F401
    except ImportError:
        warnings.warn("redis package not installed; using in-memory "
                      "idempotency store")
        return MemoryStore()
    try:
        store = RedisStore(url=url, key_prefix=key_prefix, **redis_kwargs)
        store.ping()
        return store
    except Exception:
        warnings.warn(f"could not reach Redis at {url}; using in-memory "
                      "idempotency store")
        return MemoryStore()
