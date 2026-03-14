"""
query_cache.py
--------------
In-memory query cache with LRU eviction.

Caches SELECT results by query string.
Invalidated automatically on INSERT/DELETE/UPDATE.

Two modes:
  - LRU cache: keeps the N most recently used queries
  - TTL cache: expires entries after N seconds
"""
import time
import hashlib
from collections import OrderedDict


class QueryCache:
    def __init__(self, max_size: int = 100, ttl_seconds: int = 60):
        """
        max_size    — max number of cached queries (LRU eviction)
        ttl_seconds — how long a cached result stays valid
                      set to None to disable TTL
        """
        self.max_size    = max_size
        self.ttl         = ttl_seconds
        self.cache       = OrderedDict()  # query_hash → (result, timestamp)
        self.hits        = 0
        self.misses      = 0
        self.invalidations = 0

    # ---------------------------------------------------------------- #
    #  Core operations                                                  #
    # ---------------------------------------------------------------- #

    def _hash(self, query: str) -> str:
        """
        Turn a query string into a short cache key.
        Normalize first so "SELECT * FROM users" and
        "select * from users" hit the same cache entry.
        """
        normalized = query.strip().lower()
        return hashlib.md5(normalized.encode()).hexdigest()

    def get(self, query: str):
        """
        Return cached result or None if not found / expired.
        Moves entry to end of LRU order on hit.
        """
        key = self._hash(query)

        if key not in self.cache:
            self.misses += 1
            return None

        result, timestamp = self.cache[key]

        # Check TTL
        if self.ttl is not None:
            age = time.time() - timestamp
            if age > self.ttl:
                del self.cache[key]
                self.misses += 1
                return None

        # Move to end — most recently used
        self.cache.move_to_end(key)
        self.hits += 1
        return result

    def set(self, query: str, result):
        """
        Store a query result in the cache.
        Evicts least recently used entry if cache is full.
        """
        key = self._hash(query)

        # Evict LRU entry if full
        if len(self.cache) >= self.max_size and key not in self.cache:
            self.cache.popitem(last=False)  # remove oldest

        self.cache[key] = (result, time.time())
        self.cache.move_to_end(key)

    def invalidate(self):
        """
        Clear the entire cache.
        Called after any write operation (INSERT/DELETE/UPDATE).
        """
        count = len(self.cache)
        self.cache.clear()
        self.invalidations += count

    def report(self) -> str:
        """Show cache statistics."""
        total    = self.hits + self.misses
        hit_rate = (self.hits / total * 100) if total > 0 else 0

        entries = []
        for key, (result, timestamp) in self.cache.items():
            age = time.time() - timestamp
            entries.append(f"  {key[:8]}... age={age:.1f}s")

        lines = [
            "--- Query Cache Report ---",
            f"Cached queries   : {len(self.cache)} / {self.max_size}",
            f"Cache hits       : {self.hits}",
            f"Cache misses     : {self.misses}",
            f"Hit rate         : {hit_rate:.1f}%",
            f"Invalidations    : {self.invalidations}",
            f"TTL              : {self.ttl}s",
        ]

        if self.cache:
            lines.append("Cached entries:")
            lines.extend(entries)

        return "\n".join(lines)

    def clear(self):
        """Manually clear the cache."""
        self.cache.clear()