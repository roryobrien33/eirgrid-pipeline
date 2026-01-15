#!/usr/bin/env bash
set -euo pipefail

echo "[entrypoint] Initialising DB (safe to re-run)..."
python -m ingest.init_db

echo "[entrypoint] Seeding dimension tables (safe to re-run)..."
python -m ingest.seed_dims

echo "[entrypoint] Applying SQL views (safe to re-run)..."
python -m ingest.init_views

echo "[entrypoint] Starting: $*"
exec "$@"
