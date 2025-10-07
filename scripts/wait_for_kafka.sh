#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for Kafka health..."
for i in {1..60}; do
  status=$(docker inspect --format='{{json .State.Health.Status}}' kafka 2>/dev/null || echo '"starting"')
  if [[ "$status" == '"healthy"' ]]; then
    echo "Kafka is healthy"
    exit 0
  fi
  sleep 2
done
echo "Kafka did not become healthy in time" >&2
exit 1


