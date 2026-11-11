#!/usr/bin/env bash
# Run the bronze job on YARN. Usage: scripts/bronze-stream.sh [--once]
# (--once drains Kafka and stops; no flag runs until you press Ctrl-C)
set -euo pipefail

exec "$(dirname "$0")/spark-yarn.sh" bronze_stream.py "$@"
