#!/usr/bin/env bash
set -euo pipefail

TOPIC_NAME="${1:-ran-telemetry}"
BROKER="${2:-kafka1:9092}"

/usr/bin/kafka-topics.sh --create --bootstrap-server "$BROKER" --replication-factor 3 --partitions 5 --topic "$TOPIC_NAME" --if-not-exists
