"""Synthetic call, SMS and data records, with deliberate duplicates and late events. No Kafka here, so it is unit-testable."""
import itertools
import random
import uuid
from datetime import timedelta

SUBSCRIBERS = 10_000
CELLS = 200
EVENT_TYPES = ("VOICE", "SMS", "DATA")
EVENT_WEIGHTS = (20, 10, 70)
DUPLICATE_RATE = 0.02       # exact re-send: same event_id and identical payload
NEAR_DUPLICATE_RATE = 0.01  # same event_id, corrected charge and a later emitted_at (latest wins)
LATE_RATE = 0.05
LONG_LATE_SHARE = 0.10      # of late events, the share that is 1-3 hours late instead of 1-10 minutes
MAX_DUPLICATE_GAP = 100     # a duplicate arrives up to this many events after the original


def msisdn(n):
    # 010 is not an allocated Bangladesh mobile prefix (013-019 are), so no number belongs to a real subscriber.
    return f"+88010{n:08d}"


def iso(moment):
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _record(rng, event_id, event_type, now):
    delay = 0
    if rng.random() < LATE_RATE:
        delay = rng.randint(3600, 10800) if rng.random() < LONG_LATE_SHARE else rng.randint(60, 600)
    record = {
        "event_id": event_id,
        "event_type": event_type,
        "msisdn": msisdn(rng.randrange(SUBSCRIBERS)),
        "counterparty_msisdn": None,
        "cell_id": f"DHK-{rng.randrange(CELLS):04d}",
        "event_time": iso(now - timedelta(seconds=delay)),
        "emitted_at": iso(now),
        "duration_sec": None,
        "sms_segments": None,
        "bytes_up": None,
        "bytes_down": None,
    }
    if event_type == "VOICE":
        record["counterparty_msisdn"] = msisdn(rng.randrange(SUBSCRIBERS))
        record["duration_sec"] = rng.randint(5, 1800)
        record["charge_paisa"] = record["duration_sec"] * 2
    elif event_type == "SMS":
        record["counterparty_msisdn"] = msisdn(rng.randrange(SUBSCRIBERS))
        record["sms_segments"] = rng.randint(1, 3)
        record["charge_paisa"] = record["sms_segments"] * 50
    else:
        record["bytes_up"] = rng.randint(1_000, 5_000_000)
        record["bytes_down"] = rng.randint(10_000, 50_000_000)
        record["charge_paisa"] = (record["bytes_up"] + record["bytes_down"]) // 50_000
    return record


def generate(seed, count, start, step):
    """Yield (kind, record) in emission order; kind is "new", "duplicate" or "near_duplicate".

    The same seed, start and step give the identical stream. count=0 runs forever; otherwise
    `count` distinct events are created and every duplicate still pending is flushed at the end.
    """
    rng = random.Random(seed)
    pending = []  # (emit before this index, kind, record)
    for i in itertools.count() if count == 0 else range(count):
        now = start + i * step
        still_pending = []
        for due, kind, record in pending:
            if due <= i:
                yield kind, record
            else:
                still_pending.append((due, kind, record))
        pending = still_pending

        event_id = str(uuid.uuid5(uuid.NAMESPACE_OID, f"{seed}:{start.isoformat()}:{step.total_seconds()}:{i}"))
        record = _record(rng, event_id, rng.choices(EVENT_TYPES, EVENT_WEIGHTS)[0], now)
        yield "new", record

        roll = rng.random()
        if roll < DUPLICATE_RATE:
            pending.append((i + rng.randint(1, MAX_DUPLICATE_GAP), "duplicate", record))
        elif roll < DUPLICATE_RATE + NEAR_DUPLICATE_RATE:
            gap = rng.randint(1, MAX_DUPLICATE_GAP)
            corrected = {**record, "charge_paisa": record["charge_paisa"] + rng.randint(1, 50),
                         "emitted_at": iso(start + (i + gap) * step)}
            pending.append((i + gap, "near_duplicate", corrected))

    for _, kind, record in pending:
        yield kind, record
