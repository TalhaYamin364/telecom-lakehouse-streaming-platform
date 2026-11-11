import itertools
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import producer as producer_module
from cdr_events import generate
from producer import parse_start, publish

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
STEP = timedelta(milliseconds=100)


class FakeProducer:
    def __init__(self, full_first=0, fail_delivery=False, undelivered=0, produce_error=None):
        self.messages, self.full_left, self.produce_error = [], full_first, produce_error
        self.fail_delivery, self.undelivered = fail_delivery, undelivered
        self.polls, self.flushes = 0, 0

    def produce(self, topic, key, value, on_delivery):
        if self.produce_error:
            raise self.produce_error
        if self.full_left:
            self.full_left -= 1
            raise BufferError("queue full")
        self.messages.append((topic, key, value))
        on_delivery("boom" if self.fail_delivery else None, None)

    def poll(self, _timeout):
        self.polls += 1
        if self.polls > 50_000:
            raise AssertionError("the publish loop never ended")

    def flush(self, _timeout):
        self.flushes += 1
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
        self.assertEqual(problems, ["boom"])
        _, problems = publish(FakeProducer(undelivered=4), "cdr.raw", generate(7, 10, START, STEP))
        self.assertEqual(problems, ["4 messages were not delivered"])

    def test_generation_stops_at_the_first_delivery_failure_even_when_endless(self):
        producer = FakeProducer(fail_delivery=True)
        endless = itertools.islice(generate(7, 0, START, STEP), 10_000)  # the islice only guards the test against hanging
        sent, problems = publish(producer, "cdr.raw", endless)
        self.assertEqual(problems, ["boom"])
        self.assertLess(len(producer.messages), 10)
        self.assertEqual(producer.flushes, 1)

    def test_an_error_raised_by_produce_is_reported_instead_of_crashing(self):
        class FakeKafkaException(Exception):
            pass

        producer = FakeProducer(produce_error=FakeKafkaException("Unknown topic"))
        endless = itertools.islice(generate(7, 0, START, STEP), 10_000)
        with mock.patch.object(producer_module, "KafkaException", FakeKafkaException):
            sent, problems = publish(producer, "cdr.raw", endless)
        self.assertEqual((problems, sum(sent.values()), producer.flushes), (["Unknown topic"], 0, 1))

    def test_a_stop_request_ends_generation_and_still_drains_the_queue(self):
        producer = FakeProducer()
        sent, problems = publish(producer, "cdr.raw", generate(7, 1_000, START, STEP),
                                 should_stop=lambda: len(producer.messages) >= 5)
        self.assertEqual((len(producer.messages), sum(sent.values()), problems), (5, 5, []))
        self.assertEqual(producer.flushes, 1)

    def test_a_stop_request_ends_a_retry_loop_on_a_full_queue(self):
        producer = FakeProducer(full_first=10**9)
        sent, problems = publish(producer, "cdr.raw", generate(7, 100, START, STEP),
                                 should_stop=lambda: producer.polls >= 3)
        self.assertEqual((len(producer.messages), sum(sent.values())), (0, 0))
        self.assertEqual(producer.flushes, 1)

    def test_start_accepts_iso_and_now(self):
        self.assertEqual(parse_start("2026-01-01T00:00:00Z"), START)
        self.assertLess((datetime.now(timezone.utc) - parse_start("now")).total_seconds(), 5)


if __name__ == "__main__":
    unittest.main()
