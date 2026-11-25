#!/bin/sh
# Destructive only to the running broker process; persistent data is retained.
set -eu
cd "$(dirname "$0")/.."
docker compose up -d --build --wait
trap 'docker compose start kafka-1' EXIT INT TERM
docker compose stop kafka-1
# Do not restart dependencies: verify the real two-survivor cluster.
docker compose --profile test run --no-deps --rm tests
