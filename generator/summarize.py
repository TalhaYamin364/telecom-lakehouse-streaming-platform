"""Summarise what the Kafka console consumer printed from cdr.raw (one 'Partition:N|key|json' line per message, on stdin)."""
import json
import sys
from collections import defaultdict
from datetime import datetime

LATE_SECONDS = 600  # the watermark the streaming job is expected to use


def seconds_late(record):
    emitted = datetime.fromisoformat(record["emitted_at"].replace("Z", "+00:00"))
    happened = datetime.fromisoformat(record["event_time"].replace("Z", "+00:00"))
    return (emitted - happened).total_seconds()


def summarize(lines):
    rows = []
    for line in lines:
        if line.startswith("Partition:"):
            head, key, value = line.rstrip("\n").split("|", 2)
            rows.append((int(head.split(":")[1]), key, json.loads(value)))

    by_id = defaultdict(list)
    partitions_of = defaultdict(set)
    for partition, key, record in rows:
        by_id[record["event_id"]].append(record)
        partitions_of[key].add(partition)

    # One id can have both kinds: an original, its correction, and a re-send of either. Classify by payload within each id.
    exact, near = [], []
    for copies in by_id.values():
        payloads = {}
        for record in copies:
            payloads.setdefault(json.dumps(record, sort_keys=True), []).append(record)
        repeated = next((same for same in payloads.values() if len(same) > 1), None)
        if repeated:
            exact.append(repeated[:2])
        if len(payloads) > 1:
            near.append([same[0] for same in payloads.values()][:2])
    late = [r for _, _, r in rows if seconds_late(r) > 0]
    return {
        "messages": len(rows),
        "distinct_event_ids": len(by_id),
        "exact_duplicates": exact,
        "near_duplicates": near,
        "late": late,
        "very_late": [r for r in late if seconds_late(r) > LATE_SECONDS],
        "types": {t: sum(1 for _, _, r in rows if r["event_type"] == t) for t in ("VOICE", "SMS", "DATA")},
        "split_subscribers": sum(1 for p in partitions_of.values() if len(p) > 1),
    }


def show(summary):
    print(f"messages read:        {summary['messages']}")
    print(f"distinct event_ids:   {summary['distinct_event_ids']}")
    print(f"by type:              {summary['types']}")
    print(f"exact duplicates:     {len(summary['exact_duplicates'])} event_ids seen again, unchanged")
    print(f"near-duplicates:      {len(summary['near_duplicates'])} event_ids seen again with a corrected charge")
    print(f"late events:          {len(summary['late'])} (of which {len(summary['very_late'])} over {LATE_SECONDS // 60} minutes late)")
    print(f"subscribers split across partitions: {summary['split_subscribers']} (0 means the key keeps each subscriber together)")
    for label, example in (
        ("An exact duplicate (same event_id twice):", summary["exact_duplicates"][:1] and summary["exact_duplicates"][0][:2]),
        ("A near-duplicate (same event_id, corrected charge):", summary["near_duplicates"][:1] and summary["near_duplicates"][0][:2]),
        ("A very late event:" if summary["very_late"] else "A late event:", (summary["very_late"] or summary["late"])[:1]),
    ):
        if example:
            print(f"\n{label}")
            for record in example:
                print("  " + json.dumps(record, separators=(",", ":")))


def main():
    summary = summarize(sys.stdin)
    show(summary)
    missing = [name for name, found in (("duplicates", summary["exact_duplicates"]), ("near-duplicates", summary["near_duplicates"]),
                                        ("late events", summary["late"])) if not found]
    if not summary["messages"] or missing:
        print("FAIL: " + ("no messages in cdr.raw" if not summary["messages"] else "no " + ", ".join(missing) + " found"), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
