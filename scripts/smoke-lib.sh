# shellcheck shell=bash
# Helpers shared by the smoke tests that run the whole pipeline (silver-smoke.sh, gold-smoke.sh). Source it, do not run it.

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit

kafka=(docker compose -f docker/compose.kafka.yml)
fail() { echo "FAIL: $1" >&2; exit 1; }
indent() { sed 's/^/    /'; }

generate() {  # generate <count> <seed> [start]; prints how many distinct events were created
  local out args=(--count "$1" --seed "$2")
  [ -z "${3:-}" ] || args+=(--start "$3")
  out="$("${kafka[@]}" --profile generator run --rm -T generator "${args[@]}" 2>&1)"
  grep -E '^sent' <<< "${out}" >&2 || fail "generator failed: ${out}"
  sed -n 's/^sent to cdr.raw: \([0-9]*\) new.*/\1/p' <<< "${out}"
}

produce_raw() {  # one Kafka message per argument, exactly as given
  printf '%s\n' "$@" | "${kafka[@]}" exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:19092 --topic cdr.raw > /dev/null
}

# An event and a later correction of it that also changes event_time. They arrive in different batches, and the
# correction's day is different from the original's, so the original's day has to be recomputed downstream too.
fix_time_original() {
  produce_raw '{"event_id":"fix-time","event_type":"DATA","msisdn":"+8801000000003","cell_id":"DHK-0001","event_time":"2026-01-01T00:00:50.000Z","emitted_at":"2026-01-01T00:00:51.000Z","bytes_up":1,"bytes_down":2,"charge_paisa":10}'
}
fix_time_correction() {
  produce_raw '{"event_id":"fix-time","event_type":"DATA","msisdn":"+8801000000003","cell_id":"DHK-0001","event_time":"2025-12-29T00:00:30.000Z","emitted_at":"2026-01-01T00:10:00.000Z","bytes_up":1,"bytes_down":2,"charge_paisa":99}'
}

value() {  # value KEY "text with KEY=value pairs"
  grep -oE "(^| )$1=[^ ]*" <<< "$2" | head -1 | sed 's/.*=//'
}

ingest() {  # bronze from Kafka, then silver, both on YARN
  scripts/bronze-stream.sh --once > /dev/null 2>&1 || fail "bronze job failed"
  scripts/spark-yarn.sh silver_cdr.py 2>&1 | grep -E '^silver:|nothing to do' || fail "silver job failed (run scripts/spark-yarn.sh silver_cdr.py to see why)"
}

expect() {  # expect <label> <name> <actual> <wanted>
  [ "$3" = "$4" ] || fail "$1: $2 is '$3', expected '$4'"
}
