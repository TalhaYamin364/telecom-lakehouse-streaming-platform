"""Publish synthetic CDRs to Kafka, keyed by subscriber so one subscriber's events stay in one partition."""
import argparse
import json
import math
import signal
import sys
import threading
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from cdr_events import generate

try:
    from confluent_kafka import KafkaException
except ImportError:  # the unit tests run without the Kafka client; an empty tuple catches nothing
    KafkaException = ()


def publish(producer, topic, events, rate=0, should_stop=lambda: False):
    """Send events until they run out, should_stop() is true, or a delivery fails; then drain the queue.

    Returns (counts by kind, list of distinct delivery problems).
    """
    sent, failures = Counter(), Counter()

    def on_delivery(err, _msg):
        if err:
            failures[str(err)] += 1

    def enqueue(record):
        while not failures and not should_stop():
            try:
                producer.produce(topic, key=record["msisdn"], value=json.dumps(record, separators=(",", ":")),
                                 on_delivery=on_delivery)
                return True
            except BufferError:
                producer.poll(0.5)
            except KafkaException as err:  # raised at once, not via the callback, once the topic is known to be missing
                failures[str(err)] += 1
        return False

    started = time.monotonic()
    try:
        for n, (kind, record) in enumerate(events, 1):
            if not enqueue(record):
                break
            producer.poll(0)
            sent[kind] += 1
            while rate and not failures and not should_stop():
                ahead = n / rate - (time.monotonic() - started)
                if ahead <= 0:
                    break
                # poll, not sleep: delivery callbacks only run inside poll(), and this also ends within half a second
                producer.poll(min(ahead, 0.5))
    finally:
        left = producer.flush(30)  # even if something above raised, do not drop what is queued
    problems = list(failures)
    if left:
        problems.append(f"{left} messages were not delivered")
    return sent, problems


MAX_STEP_MS = 86_400_000  # one day


def non_negative_int(text):
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError("must be 0 or more")
    return value


def non_negative_rate(text):
    value = float(text)
    if not math.isfinite(value) or value < 0:
        raise argparse.ArgumentTypeError("must be a finite number, 0 or more")
    return value


def step_ms(text):
    value = int(text)
    # Zero or negative would make a near-duplicate's later emitted_at not later than the original.
    if not 0 < value <= MAX_STEP_MS:
        raise argparse.ArgumentTypeError(f"must be between 1 and {MAX_STEP_MS}")
    return value


def non_empty(text):
    if not text.strip():
        raise argparse.ArgumentTypeError("must not be empty")
    return text


def parse_start(text):
    if text == "now":
        return datetime.now(timezone.utc).replace(microsecond=0)
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if moment.tzinfo is None:
            raise argparse.ArgumentTypeError("needs a timezone, for example 2026-01-01T00:00:00Z")
        moment = moment.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        raise argparse.ArgumentTypeError("must be an ISO timestamp such as 2026-01-01T00:00:00Z, or 'now'") from None
    if not 1970 <= moment.year <= 2100:
        raise argparse.ArgumentTypeError("must be between the years 1970 and 2100")
    return moment


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", type=non_empty, default="kafka:19092")
    parser.add_argument("--topic", type=non_empty, default="cdr.raw")
    parser.add_argument("--count", type=non_negative_int, default=10_000, help="distinct events to create; 0 runs until stopped")
    parser.add_argument("--rate", type=non_negative_rate, default=0, help="events per second; 0 sends as fast as possible")
    parser.add_argument("--seed", type=int, default=1, help="the same seed, --start and --step-ms give the same events and event_ids")
    parser.add_argument("--start", type=parse_start, default=parse_start("2026-01-01T00:00:00Z"),
                        help="event time of the first event (ISO, with a timezone), or 'now'")
    parser.add_argument("--step-ms", type=step_ms, default=100, help="event time between consecutive events")
    return parser


def main():
    args = build_parser().parse_args()

    from confluent_kafka import Producer  # imported here so the unit tests do not need the Kafka client

    # The handler only sets a flag, so a stop request ends generation but never interrupts the final flush.
    stopping = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping.set())

    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 20})
    events = generate(args.seed, args.count, args.start, timedelta(milliseconds=args.step_ms))
    sent, problems = publish(producer, args.topic, events, args.rate, stopping.is_set)

    label = "sent to" if not problems else "attempted, with failures, for"
    print(f"{label} {args.topic}: " + ", ".join(f"{sent[k]} {k}" for k in ("new", "duplicate", "near_duplicate")))
    if stopping.is_set() and args.count and sent["new"] < args.count:
        print(f"stopped early: {sent['new']} of {args.count} events")
    for problem in problems:
        print(f"FAIL: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
