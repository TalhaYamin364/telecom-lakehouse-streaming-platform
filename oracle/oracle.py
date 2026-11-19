"""An independent oracle for the gold KPIs: expected values computed in plain Python straight from the raw cdr.raw messages.

Every other check compares one layer of the pipeline with the layer below it, so a rule that is wrong in the code is
wrong in both numbers. This one never touches Spark, Iceberg or the pipeline's code. It reads what is in Kafka, applies
the rules as the README states them, and says what gold should hold, so a rule misread in the pipeline shows up here.

The rules (README, silver and gold sections):
- A record is valid or it is dropped: JSON, an event_id, a known type, +8801 and 9 digits for the number (and for the
  counterparty of voice and SMS), a cell, parseable times with event_time not after emitted_at, and a sensible metric
  for its own type. Validation comes first, so an invalid "correction" never replaces a valid original.
- One row per event_id: the valid copy with the latest emitted_at.
- Days are UTC days of the event_time of that copy, so a correction can move an event to another day.
- Each metric is summed over its own event type only; a stray value on another type is ignored.

Usage: python3 oracle/oracle.py <raw.jsonl> <gold.json>    (exit 1 and the differences if gold is not what the oracle expects)
"""
import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone

MSISDN = re.compile(r"^\+8801[0-9]{9}$")
PHONE_ANYWHERE = re.compile(r"\+8801[0-9]{9}")
TYPES = ("VOICE", "SMS", "DATA")


def moment(text):
    value = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def whole(value):
    return isinstance(value, int) and not isinstance(value, bool)


def parse_valid(line):
    """The record as a dict with parsed times, or None if the record breaks a rule."""
    try:
        r, _ = json.JSONDecoder().raw_decode(line.lstrip())  # text after a complete JSON object is ignored, as in silver
        if not isinstance(r, dict):
            return None
        event_id, kind, cell = r.get("event_id"), r.get("event_type"), r.get("cell_id")
        if not isinstance(event_id, str) or not event_id or kind not in TYPES:
            return None
        if not isinstance(r.get("msisdn"), str) or not MSISDN.match(r["msisdn"]):
            return None
        if kind in ("VOICE", "SMS") and not (isinstance(r.get("counterparty_msisdn"), str) and MSISDN.match(r["counterparty_msisdn"])):
            return None
        if not isinstance(cell, str) or not cell.strip():
            return None
        if PHONE_ANYWHERE.search(event_id) or PHONE_ANYWHERE.search(cell):
            return None
        happened, emitted = moment(r["event_time"]), moment(r["emitted_at"])
        if happened > emitted:
            return None
        if kind == "VOICE" and not (whole(r.get("duration_sec")) and r["duration_sec"] >= 0):
            return None
        if kind == "SMS" and not (whole(r.get("sms_segments")) and r["sms_segments"] >= 1):
            return None
        if kind == "DATA" and not all(whole(r.get(k)) and r[k] >= 0 for k in ("bytes_up", "bytes_down")):
            return None
        if not (whole(r.get("charge_paisa")) and r["charge_paisa"] >= 0):
            return None
    except (ValueError, TypeError, KeyError, AttributeError):
        return None
    return {**r, "_happened": happened, "_emitted": emitted}


def latest_copies(lines):
    """(valid events, dropped records): one valid copy per event_id, the one emitted last."""
    best, dropped = {}, 0
    for line in lines:
        if not line.strip():
            continue
        record = parse_valid(line)
        if record is None:
            dropped += 1
        elif record["event_id"] not in best or record["_emitted"] >= best[record["event_id"]]["_emitted"]:
            best[record["event_id"]] = record
    return list(best.values()), dropped


def expected(lines):
    """What gold should hold, in the same shape as gold_dump.py prints."""
    events, dropped = latest_copies(lines)
    days = defaultdict(lambda: defaultdict(int))
    people = defaultdict(set)
    types = defaultdict(lambda: defaultdict(int))
    cells = defaultdict(lambda: defaultdict(int))
    cell_people = defaultdict(set)
    for e in events:
        day = e["_happened"].astimezone(timezone.utc).date().isoformat()
        kind, charge = e["event_type"], e["charge_paisa"]
        d = days[day]
        d["events"] += 1
        d["charge_paisa"] += charge
        people[day].add(e["msisdn"])
        d[{"VOICE": "voice_calls", "SMS": "sms_count", "DATA": "data_sessions"}[kind]] += 1
        volume = ({"voice_seconds": e["duration_sec"]} if kind == "VOICE" else
                  {"sms_segments": e["sms_segments"]} if kind == "SMS" else
                  {"data_bytes": e["bytes_up"] + e["bytes_down"]})
        t = types[(day, kind)]
        t["events"] += 1
        t["charge_paisa"] += charge
        c = cells[(day, e["cell_id"])]
        c["events"] += 1
        c["charge_paisa"] += charge
        for name, amount in volume.items():
            d[name] += amount
            c[name] += amount
        cell_people[(day, e["cell_id"])].add(e["msisdn"])
    for day, who in people.items():
        days[day]["subscribers"] = len(who)
    for key, who in cell_people.items():
        cells[key]["distinct_subscribers"] = len(who)
    return {
        "events": len(events), "dropped": dropped,
        "days": {day: dict(values) for day, values in days.items()},
        "types": {f"{day}|{kind}": dict(values) for (day, kind), values in types.items()},
        "cells": {f"{day}|{cell}": dict(values) for (day, cell), values in cells.items()},
    }


DAY_FIELDS = ("events", "subscribers", "charge_paisa", "voice_calls", "sms_count", "data_sessions",
              "voice_seconds", "sms_segments", "data_bytes")


def differences(want, got):
    """A list of plain-English differences between the expected KPIs and the gold dump; empty if they agree."""
    found = [f"gold holds more than one row for {key}" for key in got.get("duplicate_keys", [])]
    for section, fields in (("days", DAY_FIELDS), ("types", ("events", "charge_paisa")),
                            ("cells", ("events", "distinct_subscribers", "charge_paisa", "voice_seconds", "sms_segments", "data_bytes"))):
        for key in sorted(set(want[section]) | set(got[section])):
            if key not in got[section]:
                found.append(f"{section} {key}: gold has no row, expected {want[section][key]}")
            elif key not in want[section]:
                found.append(f"{section} {key}: gold has a row {got[section][key]} that the raw events do not explain")
            else:
                for field in fields:
                    a, b = want[section][key].get(field, 0), got[section][key].get(field, 0)
                    if a != b:
                        found.append(f"{section} {key} {field}: expected {a}, gold has {b}")
    return found


def main(argv):
    if len(argv) != 3:
        print(__doc__, file=sys.stderr)
        return 2
    with open(argv[1], encoding="utf-8") as raw:
        want = expected(raw)
    with open(argv[2], encoding="utf-8") as dump:
        got = json.load(dump)
    print(f"oracle: {want['events']} valid events over {len(want['days'])} days, {want['dropped']} invalid or corrupt messages ignored")
    if want["events"] == 0 or len(want["days"]) < 2:
        print("FAIL: too little data for the comparison to mean anything", file=sys.stderr)
        return 1
    found = differences(want, got)
    for line in found[:20]:
        print("  " + line)
    if found:
        print(f"FAIL: gold differs from the raw events in {len(found)} places", file=sys.stderr)
        return 1
    print("oracle: gold matches the raw events on every day, event type and cell")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
