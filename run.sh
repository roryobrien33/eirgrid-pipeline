#!/usr/bin/env bash
set -euo pipefail

TRAIN_DAYS="${1:-60}"

docker compose build
docker compose run --rm -e TRAIN_DAYS="$TRAIN_DAYS" eirgrid
