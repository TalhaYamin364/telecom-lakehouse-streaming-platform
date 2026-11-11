"""Publish synthetic CDRs to Kafka, keyed by subscriber so one subscriber's events stay in one partition."""
import argparse
import json
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
    for n, (kind, record) in enumerate(events, 1):
        if not enqueue(record):
            break
        producer.poll(0)
        sent[kind] += 1
        if rate:
            ahead = n / rate - (time.monotonic() - started)
            if ahead > 0:
                time.sleep(ahead)
    left = producer.flush(30)
    problems = list(failures)
    if left:
        problems.append(f"{left} messages were not delivered")
    return sent, problems


def parse_start(text):
    if text == "now":
        return datetime.now(timezone.utc).replace(microsecond=0)
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default="kafka:19092")
    parser.add_argument("--topic", default="cdr.raw")
    parser.add_argument("--count", type=int, default=10_000, help="distinct events to create; 0 runs until stopped")
    parser.add_argument("--rate", type=float, default=0, help="events per second; 0 sends as fast as possible")
    parser.add_argument("--seed", type=int, default=1, help="the same seed, --start and --step-ms give the same events and event_ids")
    parser.add_argument("--start", default="2026-01-01T00:00:00Z", help="event time of the first event, or 'now'")
    parser.add_argument("--step-ms", type=int, default=100, help="event time between consecutive events")
    args = parser.parse_args()

    from confluent_kafka import Producer  # imported here so the unit tests do not need the Kafka client

    # The handler only sets a flag, so a stop request ends generation but never interrupts the final flush.
    stopping = threading.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, lambda *_: stopping.set())

    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 20})
    events = generate(args.seed, args.count, parse_start(args.start), timedelta(milliseconds=args.step_ms))
    sent, problems = publish(producer, args.topic, events, args.rate, stopping.is_set)

    label = "sent to" if not problems else "attempted, with failures, for"
    print(f"{label} {args.topic}: " + ", ".join(f"{sent[k]} {k}" for k in ("new", "duplicate", "near_duplicate")))
    for problem in problems:
        print(f"FAIL: {problem}", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
