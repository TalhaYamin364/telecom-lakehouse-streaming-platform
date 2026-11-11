import contextlib
import io
import itertools
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import producer as producer_module
from cdr_events import generate
from producer import build_parser, parse_start, publish

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
STEP = timedelta(milliseconds=100)


class FakeProducer:
    """Like the real client, delivery callbacks do not run in produce(): each one fires on the second poll() after it,
    or in flush(). poll(0) straight after produce() therefore sees nothing yet."""

    def __init__(self, full_first=0, fail_delivery=False, undelivered=0, produce_error=None):
        self.messages, self.full_left, self.produce_error = [], full_first, produce_error
        self.fail_delivery, self.undelivered = fail_delivery, undelivered
        self.polls, self.flushes, self.poll_timeouts, self.pending = 0, 0, [], []

    def produce(self, topic, key, value, on_delivery):
        if self.produce_error:
            raise self.produce_error
        if self.full_left:
            self.full_left -= 1
            raise BufferError("queue full")
        self.messages.append((topic, key, value))
        self.pending.append([0, on_delivery])

    def _deliver(self, everything):
        for item in self.pending:
            item[0] += 1
        due = [item for item in self.pending if everything or item[0] >= 2]
        self.pending = [item for item in self.pending if item not in due]
        for _, callback in due:
            callback("boom" if self.fail_delivery else None, None)

    def poll(self, timeout):
        self.polls += 1
        self.poll_timeouts.append(timeout)
        if self.polls > 50_000:
            raise AssertionError("the publish loop never ended")
        self._deliver(everything=False)

    def flush(self, _timeout):
        self.flushes += 1
        self._deliver(everything=True)
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

    def test_pacing_waits_in_short_polls_so_a_stop_request_is_noticed_quickly(self):
        producer = FakeProducer()
        with mock.patch.object(producer_module.time, "sleep", side_effect=AssertionError("pacing must poll, not sleep")):
            sent, _ = publish(producer, "cdr.raw", generate(7, 1_000, START, STEP), rate=0.01,
                              should_stop=lambda: producer.polls >= 6)
        self.assertEqual(sum(sent.values()), 1)  # one event goes out, then the stop request ends the wait for the next
        self.assertEqual(max(producer.poll_timeouts), 0.5)

    def test_pacing_wait_notices_a_delivery_failure_that_only_poll_can_report(self):
        producer = FakeProducer(fail_delivery=True)
        with mock.patch.object(producer_module.time, "sleep", side_effect=AssertionError("sleeping hides callbacks")):
            _, problems = publish(producer, "cdr.raw", generate(7, 1_000, START, STEP), rate=0.01)
        self.assertEqual((len(producer.messages), problems), (1, ["boom"]))

    def test_the_queue_is_flushed_even_when_the_event_source_raises(self):
        def exploding():
            yield from itertools.islice(generate(7, 0, START, STEP), 3)
            raise RuntimeError("generator failed")

        producer = FakeProducer()
        with self.assertRaises(RuntimeError):
            publish(producer, "cdr.raw", exploding())
        self.assertEqual((len(producer.messages), producer.flushes), (3, 1))

    def test_start_accepts_iso_and_now(self):
        self.assertEqual(parse_start("2026-01-01T00:00:00Z"), START)
        self.assertEqual(parse_start("2026-01-01T06:00:00+06:00"), START)
        self.assertLess((datetime.now(timezone.utc) - parse_start("now")).total_seconds(), 5)


def parse(*argv):
    with contextlib.redirect_stderr(io.StringIO()):
        return build_parser().parse_args(list(argv))


class ArgumentTest(unittest.TestCase):
    def test_defaults_are_valid(self):
        args = parse()
        self.assertEqual((args.count, args.rate, args.step_ms, args.start), (10_000, 0, 100, START))

    def test_bad_values_are_rejected_before_anything_starts(self):
        bad = [
            ("--count", "-1"), ("--count", "abc"),
            ("--rate", "-1"), ("--rate", "nan"), ("--rate", "inf"), ("--rate", "abc"),
            ("--step-ms", "0"), ("--step-ms", "-5"), ("--step-ms", "86400001"), ("--step-ms", "1" + "0" * 100),
            ("--start", ""), ("--start", "NaN"), ("--start", "0"), ("--start", "2026-01-01"),
            ("--start", "2026-01-01T00:00:00"), ("--start", "9999-12-31T23:59:59.900Z"),
            ("--start", "0001-01-01T00:00:00Z"),
            ("--topic", ""), ("--topic", "  "), ("--bootstrap", ""),
        ]
        for flag, value in bad:
            with self.subTest(flag=flag, value=value[:20]), self.assertRaises(SystemExit):
                parse(flag, value)

    def test_good_edge_values_are_accepted(self):
        args = parse("--count", "0", "--rate", "0.01", "--step-ms", "86400000", "--start", "now")
        self.assertEqual((args.count, args.rate, args.step_ms), (0, 0.01, 86_400_000))
        self.assertEqual(parse("--start", "2026-01-01T00:00:00Z").start, START)


if __name__ == "__main__":
    unittest.main()
