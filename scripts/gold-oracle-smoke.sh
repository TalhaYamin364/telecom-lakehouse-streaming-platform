#!/usr/bin/env bash
# Oracle test for the gold layer, from an empty lake (it runs scripts/lake-reset.sh first, so it deletes local data).
# The other smoke tests compare each layer with the one below it. This one compares gold with the raw cdr.raw messages,
# through oracle/oracle.py, which works out the expected KPIs in plain Python without any of the pipeline's code:
# per day, per event type and per cell, including a correction that moves an event to another day, a record with stray
# fields, and a corrupt record that has to be left out.
# Needs both stacks up (see README). Takes about four minutes.
set -euo pipefail

# shellcheck source=/dev/null
source "$(dirname "$0")/smoke-lib.sh"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

echo "1/4 start from an empty lake"
scripts/lake-reset.sh > /dev/null

echo "2/4 generate 1000 events plus a stray-field event, a later correction that moves an event to another day, and a negative-charge record"
total="$(generate 1000 8601)"
produce_raw '{"event_id":"stray-fields","event_type":"DATA","msisdn":"+8801000000004","cell_id":"DHK-0001","event_time":"2026-01-01T00:00:20.000Z","emitted_at":"2026-01-01T00:00:21.000Z","duration_sec":60,"sms_segments":9,"bytes_up":5,"bytes_down":6,"charge_paisa":7}'
fix_time_original
fix_time_correction
produce_raw '{"event_id":"negative-charge","event_type":"SMS","msisdn":"+8801000000005","counterparty_msisdn":"+8801000000006","cell_id":"DHK-0002","event_time":"2026-01-01T00:00:40.000Z","emitted_at":"2026-01-01T00:00:41.000Z","sms_segments":1,"charge_paisa":-50}'
produce_raw 'this is not json'

echo "3/4 build bronze, silver and gold on YARN, and read every message back from Kafka"
ingest
scripts/spark-yarn.sh gold_daily.py 2>&1 | grep -E '^gold:|nothing to do' || fail "gold job failed (run scripts/spark-yarn.sh gold_daily.py to see why)"
read_topic "${work}/raw.jsonl"
[ "$(wc -l < "${work}/raw.jsonl")" -ge "${total}" ] || fail "read only $(wc -l < "${work}/raw.jsonl") messages back from Kafka, fewer than the ${total} events generated"
gold_dump "${work}/gold.json"

echo "4/4 gold equals what the raw events say it should be"
python3 oracle/oracle.py "${work}/raw.jsonl" "${work}/gold.json" | indent

echo "OK: gold matches an independent calculation from the raw events, per day, per event type and per cell"
