# ── Stage 1: Dependencies ─────────────────────────────────────────────────────
FROM python:3.11-slim AS base

WORKDIR /app

# Create non-root user for security (principle of least privilege)
RUN adduser --disabled-password --gecos '' appuser \
    && chown -R appuser:appuser /app

# Install dependencies as root (before switching user) to allow pip install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# ── Stage 2: Application ──────────────────────────────────────────────────────
USER appuser

# Copy source code
COPY --chown=appuser:appuser src/ ./src/

# Persistent volume mount point for SQLite dedup store
# The data/ directory will be created at runtime by DedupStore
RUN mkdir -p /app/data

# Health check — Docker will poll this to determine container readiness
HEALTHCHECK --interval=5s --timeout=3s --start-period=10s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')" \
    || exit 1

EXPOSE 8080

# Override DB path via environment variable if needed
ENV DEDUP_DB_PATH=/app/data/dedup_store.db

CMD ["python", "-m", "src.main"]
