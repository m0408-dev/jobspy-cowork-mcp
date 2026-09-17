"""Shared bounded HTTP cache; failed upstreams use short TTL to avoid repeated blocked calls."""
import time
import threading
from collections import OrderedDict
import httpx

_cache = OrderedDict()
_lock = threading.Lock()
MAX_BYTES = 32 * 1024 * 1024

class CachedClient(httpx.AsyncClient):
    async def get(self, url, **kwargs):
        request = self.build_request("GET", url, params=kwargs.get("params"), headers=kwargs.get("headers"))
        # Only used for fixed public job APIs. Headers included to isolate response variants.
        key = (str(request.url), tuple(sorted(request.headers.items())))
        with _lock:
            value = _cache.get(key)
            if value and value[0] > time.monotonic():
                _cache.move_to_end(key)
                return httpx.Response(value[1], headers=value[2], content=value[3], request=request)
        response = await super().get(url, **kwargs)
        content = response.content
        if len(content) <= 8 * 1024 * 1024:
            ttl = 900 if response.status_code == 200 else 60
            with _lock:
                headers = {k: v for k, v in response.headers.items() if k.lower() not in ("content-encoding", "content-length", "transfer-encoding")}
                _cache[key] = (time.monotonic()+ttl, response.status_code, headers, content)
                while len(_cache) > 512 or sum(len(v[3]) for v in _cache.values()) > MAX_BYTES:
                    _cache.popitem(last=False)
        return response
