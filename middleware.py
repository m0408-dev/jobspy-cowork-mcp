"""Bounded per-client rate limiting, independent of SSE response streams."""
import time
from collections import defaultdict

class RateLimit:
    def __init__(self, inner, per_min):
        self.inner, self.per_min = inner, per_min
        self.hits = defaultdict(list)

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or self.per_min <= 0 or scope.get("path") == "/health":
            return await self.inner(scope, receive, send)
        peer = (scope.get("client") or ["?"])[0]
        now = time.monotonic()
        # Do not trust arbitrary X-Forwarded-For values. uvicorn's trusted proxy handling
        # determines scope.client; deployment publishes only to loopback.
        self.hits[peer] = [t for t in self.hits[peer] if t > now - 60]
        if len(self.hits[peer]) >= self.per_min:
            await send({"type": "http.response.start", "status": 429, "headers": [(b"retry-after", b"60")]})
            return await send({"type": "http.response.body", "body": b"Rate limit exceeded"})
        self.hits[peer].append(now)
        if len(self.hits) > 4096:
            self.hits = defaultdict(list, {k: v for k, v in self.hits.items() if v and v[-1] > now - 60})
        return await self.inner(scope, receive, send)
