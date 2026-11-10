import json
import unittest
from datetime import datetime, timedelta, timezone

from cdr_events import generate
from producer import parse_start, publish

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
STEP = timedelta(milliseconds=100)


class FakeProducer:
    def __init__(self, full_first=0, fail_delivery=False, undelivered=0):
        self.messages, self.full_left = [], full_first
        self.fail_delivery, self.undelivered = fail_delivery, undelivered

    def produce(self, topic, key, value, on_delivery):
        if self.full_left:
            self.full_left -= 1
            raise BufferError("queue full")
        self.messages.append((topic, key, value))
        on_delivery("boom" if self.fail_delivery else None, None)

    def poll(self, _timeout):
        pass

    def flush(self, _timeout):
        return self.undelivered


class PublishTest(unittest.TestCase):
    def test_key_is_the_subscriber_and_value_is_the_record_json(self):
        producer = FakeProducer()
        sent, problems = publish(producer, "cdr.raw", generate(7, 200, START, STEP))
        self.assertEqual(problems, [])
        self.assertEqual(len(producer.messages), sum(sent.values()))
        for topic, key, value in producer.messages:
            self.assertEqual(topic, "cdr.raw")
            self.assertEqual(key, json.loads(value)["msisdn"])

    def test_a_full_queue_is_retried_without_losing_events(self):
        producer = FakeProducer(full_first=3)
        sent, problems = publish(producer, "cdr.raw", generate(7, 50, START, STEP))
        self.assertEqual(problems, [])
        self.assertEqual(len(producer.messages), sum(sent.values()))

    def test_delivery_errors_and_undelivered_messages_are_reported(self):
        _, problems = publish(FakeProducer(fail_delivery=True), "cdr.raw", generate(7, 10, START, STEP))
        self.assertIn("boom", problems)
        _, problems = publish(FakeProducer(undelivered=4), "cdr.raw", generate(7, 10, START, STEP))
        self.assertEqual(problems, ["4 messages were not delivered"])

    def test_start_accepts_iso_and_now(self):
        self.assertEqual(parse_start("2026-01-01T00:00:00Z"), START)
        self.assertLess((datetime.now(timezone.utc) - parse_start("now")).total_seconds(), 5)


if __name__ == "__main__":
    unittest.main()
