# ─────────────────────────────────────────────
#  OrSQL — Custom Python Database Engine
#  Usage:
#    docker build -t orsql .
#    docker run -it --env HF_TOKEN=your_token orsql
#
#  With persistent data (survives container restart):
#    docker run -it --env HF_TOKEN=your_token -v orsql_data:/data orsql
# ─────────────────────────────────────────────

# Use slim Python 3.11 — small image, matches your code
FROM python:3.11-slim

# ── Metadata ──────────────────────────────────
LABEL name="orsql"
LABEL description="Custom SQL engine built from scratch in Python"
LABEL version="1.0.0"

# ── Install dependencies ───────────────────────
# Copy requirements first so Docker caches this layer
# (only re-runs pip install if requirements change)
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Copy source code ───────────────────────────
COPY db/ ./db/

# ── Database files live in /data ───────────────
# This is a Docker volume — mount it to keep data
# between container restarts
ENV ORSQL_DB_DIR=/data
RUN mkdir -p /data

# ── Run the CLI ────────────────────────────────
WORKDIR /app/db
CMD ["python", "server.py"]
