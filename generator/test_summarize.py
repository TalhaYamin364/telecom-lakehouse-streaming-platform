import json
import unittest
from datetime import datetime, timedelta, timezone

from cdr_events import generate
from summarize import summarize

START = datetime(2026, 1, 1, tzinfo=timezone.utc)
STEP = timedelta(milliseconds=100)


class SummarizeTest(unittest.TestCase):
    def test_it_finds_exactly_the_duplicates_and_late_events_the_generator_made(self):
        events = list(generate(3, 5_000, START, STEP))
        lines = [f"Partition:0|{r['msisdn']}|{json.dumps(r)}\n" for _, r in events]
        summary = summarize(lines + ["not a message line\n"])

        kinds = [kind for kind, _ in events]
        self.assertEqual(summary["messages"], len(events))
        self.assertEqual(summary["distinct_event_ids"], kinds.count("new"))
        self.assertEqual(len(summary["exact_duplicates"]), kinds.count("duplicate"))
        self.assertEqual(len(summary["near_duplicates"]), kinds.count("near_duplicate"))
        self.assertTrue(summary["late"] and summary["very_late"])
        self.assertEqual(summary["split_subscribers"], 0)

    def test_a_subscriber_in_two_partitions_is_reported(self):
        events = [r for _, r in generate(3, 4, START, STEP)]
        lines = [f"Partition:{n % 2}|same-key|{json.dumps(r)}\n" for n, r in enumerate(events)]
        self.assertEqual(summarize(lines)["split_subscribers"], 1)


if __name__ == "__main__":
    unittest.main()
