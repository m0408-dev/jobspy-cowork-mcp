FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY server.py sources.py ./

# Run as an unprivileged user (defense in depth — the app never needs root).
RUN useradd --create-home --uid 10001 appuser
USER appuser

# Cloud/Cowork defaults. HOST/PORT are usually overridden by the platform ($PORT).
ENV MCP_TRANSPORT=http \
    HOST=0.0.0.0 \
    PORT=8000

EXPOSE 8000

# Container-level liveness: marks the container unhealthy if the app hangs.
# (A host-side watchdog restarts unhealthy containers — see deploy/ / README.)
HEALTHCHECK --interval=30s --timeout=5s --start-period=45s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health',timeout=3).read()==b'ok' else 1)"

CMD ["python", "server.py"]
