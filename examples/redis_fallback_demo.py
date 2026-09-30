"""Demo: redis_store_or_memory() graceful fallback.

Points at a Redis that is not there, so you can watch the fallback
happen without running a server. Run from the repo root:

    python examples/redis_fallback_demo.py

With a real server (REDIS_URL=redis://localhost:6379/0), the same code
returns a RedisStore instead.
"""

import os
import sys
import warnings

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from stores import MemoryStore, RedisStore, redis_store_or_memory


def main():
    url = os.environ.get("REDIS_URL", "redis://127.0.0.1:9/0")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        store = redis_store_or_memory(url, socket_timeout=1, socket_connect_timeout=1)
    for w in caught:
        print("warning:", w.message)

    print("backend chosen:", type(store).__name__)
    if isinstance(store, RedisStore):
        print("(a real Redis answered; records are shared and durable)")
    else:
        assert isinstance(store, MemoryStore)
        print("(no Redis reachable; running on the in-memory fallback)")

    # The contract is identical either way.
    claim = store.put_in_flight("demo-key", "fp-1", ttl_seconds=60)
    store.complete("demo-key", 200, b'{"ok": true}',
                   {"content-type": "application/json"}, "application/json")
    record = store.get("demo-key")
    print("claim:", claim, "| replay body:", record["response_body"])
    assert record["status"] == "completed"
    print("OK")


if __name__ == "__main__":
    main()
