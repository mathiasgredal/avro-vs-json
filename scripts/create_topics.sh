#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-localhost:9094}"

topics=(
  "bench-json"
  "bench-avro"
)

for t in "${topics[@]}"; do
  echo "Creating topic: $t"
  docker exec kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic $t --partitions 3 --replication-factor 1"
done

echo "Listing topics via $BOOTSTRAP_SERVER"
/usr/bin/env bash -lc "/usr/bin/printf ''" >/dev/null


