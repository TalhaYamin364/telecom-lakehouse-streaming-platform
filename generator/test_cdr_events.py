import re
import unittest
from datetime import datetime, timedelta, timezone
from unittest import mock

import cdr_events
from cdr_events import MAX_DUPLICATE_GAP, generate

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
STEP = timedelta(milliseconds=100)


def run(seed=42, count=20_000):
    return list(generate(seed, count, START, STEP))


def parse(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


class GeneratorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.events = run()
        cls.new = [r for k, r in cls.events if k == "new"]
        cls.duplicates = [r for k, r in cls.events if k == "duplicate"]
        cls.near = [r for k, r in cls.events if k == "near_duplicate"]

    def test_same_seed_and_start_give_the_identical_stream(self):
        self.assertEqual(self.events, run())
        self.assertNotEqual(self.events, run(seed=43))

    def test_count_is_distinct_events_and_ids_are_unique(self):
        self.assertEqual(len(self.new), 20_000)
        self.assertEqual(len({r["event_id"] for r in self.new}), 20_000)

    def test_duplicate_rates(self):
        self.assertAlmostEqual(len(self.duplicates) / 20_000, 0.02, delta=0.005)
        self.assertAlmostEqual(len(self.near) / 20_000, 0.01, delta=0.004)

    def test_exact_duplicates_are_identical_and_arrive_after_the_original(self):
        position = {r["event_id"]: n for n, (k, r) in enumerate(self.events) if k == "new"}
        originals = {r["event_id"]: r for r in self.new}
        for n, (kind, record) in enumerate(self.events):
            if kind == "duplicate":
                self.assertEqual(record, originals[record["event_id"]])
                self.assertGreater(n, position[record["event_id"]])

    def test_near_duplicates_only_change_charge_and_emitted_at(self):
        originals = {r["event_id"]: r for r in self.new}
        for record in self.near:
            original = originals[record["event_id"]]
            changed = {key for key in record if record[key] != original[key]}
            self.assertEqual(changed, {"charge_paisa", "emitted_at"})
            self.assertGreater(record["charge_paisa"], original["charge_paisa"])
            self.assertGreater(parse(record["emitted_at"]), parse(original["emitted_at"]))

    def test_late_events_are_5_percent_and_mostly_inside_ten_minutes(self):
        delays = [(parse(r["emitted_at"]) - parse(r["event_time"])).total_seconds() for r in self.new]
        late = [d for d in delays if d > 0]
        self.assertAlmostEqual(len(late) / 20_000, 0.05, delta=0.01)
        self.assertTrue(all(60 <= d <= 10_800 for d in late))
        self.assertTrue(any(d > 600 for d in late), "some events must arrive past a 10 minute watermark")
        self.assertGreater(sum(1 for d in late if d <= 600), sum(1 for d in late if d > 600))

    def test_event_mix_and_type_specific_fields(self):
        counts = {t: sum(1 for r in self.new if r["event_type"] == t) for t in ("VOICE", "SMS", "DATA")}
        self.assertAlmostEqual(counts["DATA"] / 20_000, 0.70, delta=0.02)
        self.assertAlmostEqual(counts["VOICE"] / 20_000, 0.20, delta=0.02)
        for r in self.new:
            if r["event_type"] == "VOICE":
                self.assertIsNotNone(r["duration_sec"])
            elif r["event_type"] == "SMS":
                self.assertIsNotNone(r["sms_segments"])
            else:
                self.assertIsNotNone(r["bytes_down"])

    def test_numbers_use_only_the_unallocated_010_block(self):
        pattern = re.compile(r"^\+88010\d{8}$")
        for r in self.new:
            self.assertRegex(r["msisdn"], pattern)
            if r["counterparty_msisdn"]:
                self.assertRegex(r["counterparty_msisdn"], pattern)

    def test_endless_mode_streams_without_end(self):
        stream = generate(1, 0, START, STEP)
        self.assertEqual(len([next(stream) for _ in range(500)]), 500)


class CopiesAreNeverLostTest(unittest.TestCase):
    def copies_follow_originals(self, events):
        """Every copy arrives after its original, within MAX_DUPLICATE_GAP new events of it."""
        index_of = {}
        new_so_far = 0
        for kind, record in events:
            if kind == "new":
                index_of[record["event_id"]] = new_so_far
                new_so_far += 1
            else:
                self.assertIn(record["event_id"], index_of, "a copy arrived before its original")
                self.assertTrue(1 <= new_so_far - index_of[record["event_id"]] <= MAX_DUPLICATE_GAP)

    def test_duplicates_still_pending_when_the_stream_ends_are_flushed(self):
        with mock.patch.object(cdr_events, "DUPLICATE_RATE", 1.0):
            events = list(generate(5, 5, START, STEP))
        self.assertEqual([k for k, _ in events].count("duplicate"), 5)
        self.copies_follow_originals(events)

    def test_near_duplicates_still_pending_when_the_stream_ends_are_flushed(self):
        with mock.patch.object(cdr_events, "DUPLICATE_RATE", 0.0), mock.patch.object(cdr_events, "NEAR_DUPLICATE_RATE", 1.0):
            events = list(generate(5, 5, START, STEP))
        self.assertEqual([k for k, _ in events].count("near_duplicate"), 5)
        self.copies_follow_originals(events)

    def test_copies_follow_originals_in_a_normal_run(self):
        self.copies_follow_originals(run(count=5_000))

    def test_changing_the_step_changes_every_event_id(self):
        ids = lambda step: {r["event_id"] for k, r in generate(1, 500, START, step) if k == "new"}
        self.assertFalse(ids(STEP) & ids(timedelta(seconds=1)))


if __name__ == "__main__":
    unittest.main()
