"""Tests for the silver rules and the gold aggregations, on small DataFrames in a local Spark session (no HDFS, Kafka or Iceberg).

Run from the repository root with pyspark installed (and Java 17):
    python3 -m unittest discover -s spark_jobs/tests
"""
import hashlib
import json
import sys
import unittest
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

import gold_daily  # noqa: E402
import silver_cdr  # noqa: E402

SALT = "test-salt"
SUBSCRIBER = "+8801012345678"
COUNTERPARTY = "+8801087654321"


def record(**changes):
    """A valid VOICE record as bronze holds it (one JSON string), with the given fields changed."""
    base = {
        "event_id": "e1", "event_type": "VOICE", "msisdn": SUBSCRIBER, "counterparty_msisdn": COUNTERPARTY,
        "cell_id": "DHK-0001", "event_time": "2026-01-01T10:00:00.000Z", "emitted_at": "2026-01-01T10:00:05.000Z",
        "duration_sec": 60, "sms_segments": None, "bytes_up": None, "bytes_down": None, "charge_paisa": 120,
    }
    base.update(changes)
    return json.dumps(base)


def sms(**changes):
    return record(**{"event_type": "SMS", "duration_sec": None, "sms_segments": 2, "charge_paisa": 100, **changes})


def data(**changes):
    return record(**{"event_type": "DATA", "counterparty_msisdn": None, "duration_sec": None, "bytes_up": 1000,
                     "bytes_down": 5000, "charge_paisa": 30, **changes})


class SparkCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]").appName("unit-tests")
            .config("spark.sql.shuffle.partitions", "2").config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC").config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()


class SilverRulesTest(SparkCase):
    def parsed(self, *raw_values):
        rows = [(0, offset, raw) for offset, raw in enumerate(raw_values)]
        return silver_cdr.parse(self.spark.createDataFrame(rows, "kafka_partition INT, kafka_offset BIGINT, raw_value STRING"))

    def reasons(self, *raw_values):
        return [r.reason for r in self.parsed(*raw_values).orderBy("kafka_offset").collect()]

    def test_valid_records_of_every_type_pass(self):
        self.assertEqual(self.reasons(record(), sms(event_id="e2"), data(event_id="e3")), [None, None, None])

    def test_a_data_record_needs_no_counterparty_but_voice_and_sms_do(self):
        self.assertEqual(self.reasons(data(counterparty_msisdn=None)), [None])
        self.assertEqual(self.reasons(record(counterparty_msisdn=None), sms(counterparty_msisdn=None)),
                         ["invalid counterparty", "invalid counterparty"])

    def test_each_rule_names_the_reason_it_rejects_with(self):
        cases = {
            "invalid json": "this is not json",
            "missing event_id": record(event_id=None),
            "unknown event_type": record(event_type="FAX"),
            "invalid msisdn": record(msisdn="01012345678"),
            "invalid counterparty": record(counterparty_msisdn="+8801"),
            "missing cell_id": record(cell_id="  "),
            "phone number in event_id or cell_id": record(event_id="call-+8801012345678"),
            "invalid event_time": record(event_time="yesterday"),
            "invalid emitted_at": record(emitted_at=None),
            "event_time after emitted_at": record(event_time="2026-01-01T11:00:00.000Z"),
            "invalid duration": record(duration_sec=-1),
            "invalid sms_segments": sms(sms_segments=0),
            "invalid data volume": data(bytes_down=-5),
            "invalid charge": record(charge_paisa=-1),
        }
        for expected, raw in cases.items():
            with self.subTest(expected):
                self.assertEqual(self.reasons(raw), [expected])

    def test_a_null_value_is_invalid_json_too(self):
        df = self.spark.createDataFrame([(0, 0, None)], "kafka_partition INT, kafka_offset BIGINT, raw_value STRING")
        self.assertEqual([r.reason for r in silver_cdr.parse(df).collect()], ["invalid json"])

    def test_an_empty_json_object_is_missing_its_event_id(self):
        self.assertEqual(self.reasons("{}"), ["missing event_id"])

    def test_an_empty_event_id_and_a_phone_number_in_cell_id_are_rejected(self):
        self.assertEqual(self.reasons(record(event_id="")), ["missing event_id"])
        self.assertEqual(self.reasons(record(cell_id="x+8801012345678y")), ["phone number in event_id or cell_id"])

    def test_a_missing_metric_for_its_own_type_is_rejected(self):
        self.assertEqual(self.reasons(record(duration_sec=None), sms(sms_segments=None), data(bytes_up=None)),
                         ["invalid duration", "invalid sms_segments", "invalid data volume"])

    def test_a_stray_metric_on_another_type_is_kept_not_rejected(self):
        self.assertEqual(self.reasons(data(duration_sec=999)), [None])

    def test_a_record_that_breaks_several_rules_gets_the_first_one(self):
        self.assertEqual(self.reasons(record(msisdn="bad", cell_id=None, charge_paisa=-1)), ["invalid msisdn"])

    def test_a_bad_timestamp_rejects_the_record_without_failing_the_job(self):
        self.assertEqual(self.reasons(record(event_time="2026-13-45T99:99:99Z"), record(event_id="e2")),
                         ["invalid event_time", None])


class SilverShapeTest(SparkCase):
    def shaped(self, *raw_values, salt=SALT):
        rows = [(0, offset, raw) for offset, raw in enumerate(raw_values)]
        raw_df = self.spark.createDataFrame(rows, "kafka_partition INT, kafka_offset BIGINT, raw_value STRING")
        return silver_cdr.mask_and_shape(silver_cdr.parse(raw_df).where("reason IS NULL"), salt)

    def test_numbers_become_a_salted_sha256_and_a_masked_form(self):
        row = self.shaped(record()).collect()[0]
        self.assertEqual(row.subscriber_hash, hashlib.sha256(f"{SALT}|{SUBSCRIBER}".encode()).hexdigest())
        self.assertEqual(row.counterparty_hash, hashlib.sha256(f"{SALT}|{COUNTERPARTY}".encode()).hexdigest())
        self.assertEqual(row.subscriber_masked, "+88010******78")
        self.assertEqual(row.counterparty_masked, "+88010******21")

    def test_no_raw_phone_number_reaches_any_column(self):
        for row in self.shaped(record(), sms(event_id="e2"), data(event_id="e3")).collect():
            for value in row:
                self.assertNotIn("+8801012345678", str(value))
                self.assertNotIn("+8801087654321", str(value))

    def test_the_same_number_always_gets_the_same_hash_and_another_salt_a_different_one(self):
        a, b = (r.subscriber_hash for r in self.shaped(record(event_id="x"), record(event_id="y")).orderBy("event_id").collect())
        self.assertEqual(a, b)
        other = self.shaped(record(), salt="another-salt").collect()[0].subscriber_hash
        self.assertNotEqual(a, other)

    def test_a_data_record_has_no_counterparty_hash(self):
        row = self.shaped(data()).collect()[0]
        self.assertIsNone(row.counterparty_hash)

    def test_columns_match_the_silver_table(self):
        names = self.shaped(record()).columns
        self.assertEqual(names, ["event_id", "event_type", "subscriber_hash", "subscriber_masked", "counterparty_hash",
                                 "counterparty_masked", "cell_id", "event_time", "emitted_at", "duration_sec",
                                 "sms_segments", "bytes_up", "bytes_down", "charge_paisa", "kafka_partition",
                                 "kafka_offset", "processed_at"])


class LatestCopyTest(SparkCase):
    def latest(self, rows):
        df = self.spark.createDataFrame(rows, "event_id STRING, emitted_at TIMESTAMP, kafka_offset BIGINT, charge_paisa BIGINT")
        return {r.event_id: r.charge_paisa for r in silver_cdr.latest_per_event(df).collect()}

    def test_the_later_emitted_copy_wins_whatever_the_arrival_order(self):
        early, late = datetime(2026, 1, 1, 10), datetime(2026, 1, 1, 11)
        self.assertEqual(self.latest([("a", early, 1, 100), ("a", late, 2, 150)]), {"a": 150})
        self.assertEqual(self.latest([("a", late, 1, 150), ("a", early, 2, 100)]), {"a": 150})

    def test_an_exact_resend_keeps_one_row(self):
        moment = datetime(2026, 1, 1, 10)
        self.assertEqual(self.latest([("a", moment, 1, 100), ("a", moment, 2, 100)]), {"a": 100})

    def test_different_events_are_kept_apart(self):
        moment = datetime(2026, 1, 1, 10)
        self.assertEqual(self.latest([("a", moment, 1, 100), ("b", moment, 2, 200)]), {"a": 100, "b": 200})


SILVER_SCHEMA = T.StructType([
    T.StructField("event_time", T.TimestampType()), T.StructField("event_type", T.StringType()),
    T.StructField("subscriber_hash", T.StringType()), T.StructField("subscriber_masked", T.StringType()),
    T.StructField("cell_id", T.StringType()), T.StructField("duration_sec", T.LongType()),
    T.StructField("sms_segments", T.LongType()), T.StructField("bytes_up", T.LongType()),
    T.StructField("bytes_down", T.LongType()), T.StructField("charge_paisa", T.LongType()),
])


def silver_row(when, kind, who="h1", cell="C1", duration=None, segments=None, up=None, down=None, charge=0):
    return (when, kind, who, "+88010******78", cell, duration, segments, up, down, charge)


class GoldAggregatesTest(SparkCase):
    DAY1, DAY2 = datetime(2026, 1, 1, 10), datetime(2026, 1, 2, 10)

    def build(self, rows):
        frames = gold_daily.aggregates(self.spark.createDataFrame(rows, SILVER_SCHEMA))
        return {name: [r.asDict() for r in df.collect()] for name, df in frames.items()}

    def usage(self, rows, who="h1"):
        found = [r for r in self.build(rows)[gold_daily.USAGE] if r["subscriber_hash"] == who]
        self.assertEqual(len(found), 1)
        return found[0]

    def test_each_metric_is_summed_over_its_own_event_type_only(self):
        day = self.usage([
            silver_row(self.DAY1, "VOICE", duration=60, charge=120),
            silver_row(self.DAY1, "VOICE", duration=30, charge=60),
            silver_row(self.DAY1, "SMS", segments=2, charge=100),
            silver_row(self.DAY1, "DATA", up=1000, down=5000, charge=30),
        ])
        self.assertEqual((day["voice_calls"], day["voice_seconds"]), (2, 90))
        self.assertEqual((day["sms_count"], day["sms_segments"]), (1, 2))
        self.assertEqual((day["data_sessions"], day["data_bytes_up"], day["data_bytes_down"]), (1, 1000, 5000))
        self.assertEqual((day["events"], day["charge_paisa"]), (4, 310))

    def test_a_stray_value_on_the_wrong_type_is_not_counted(self):
        day = self.usage([
            silver_row(self.DAY1, "DATA", duration=999, segments=9, up=10, down=20),
            silver_row(self.DAY1, "VOICE", duration=5, up=777, down=888),
            silver_row(self.DAY1, "SMS", segments=1, duration=444),
        ])
        self.assertEqual(day["voice_seconds"], 5)
        self.assertEqual(day["sms_segments"], 1)
        self.assertEqual((day["data_bytes_up"], day["data_bytes_down"]), (10, 20))

    def test_a_day_without_a_type_reports_zero_not_null(self):
        day = self.usage([silver_row(self.DAY1, "VOICE", duration=10, charge=5)])
        for column in ("sms_count", "sms_segments", "data_sessions", "data_bytes_up", "data_bytes_down"):
            self.assertEqual(day[column], 0, column)

    def test_events_fall_on_their_utc_day_at_the_midnight_boundary(self):
        rows = [silver_row(datetime(2026, 1, 1, 23, 59, 59), "VOICE", duration=1),
                silver_row(datetime(2026, 1, 2, 0, 0, 0), "VOICE", duration=2)]
        by_day = {str(r["usage_date"]): r["voice_seconds"] for r in self.build(rows)[gold_daily.USAGE]}
        self.assertEqual(by_day, {"2026-01-01": 1, "2026-01-02": 2})

    def test_a_subscriber_gets_one_row_per_day(self):
        rows = [silver_row(self.DAY1, "VOICE", duration=1), silver_row(self.DAY2, "VOICE", duration=2),
                silver_row(self.DAY1, "VOICE", who="h2", duration=4)]
        found = sorted((str(r["usage_date"]), r["subscriber_hash"], r["voice_seconds"]) for r in self.build(rows)[gold_daily.USAGE])
        self.assertEqual(found, [("2026-01-01", "h1", 1), ("2026-01-01", "h2", 4), ("2026-01-02", "h1", 2)])

    def test_revenue_is_summed_per_event_type_and_day(self):
        rows = [silver_row(self.DAY1, "VOICE", duration=1, charge=100), silver_row(self.DAY1, "VOICE", duration=1, charge=50),
                silver_row(self.DAY1, "SMS", segments=1, charge=40), silver_row(self.DAY2, "VOICE", duration=1, charge=7)]
        found = {(str(r["usage_date"]), r["event_type"]): (r["events"], r["charge_paisa"]) for r in self.build(rows)[gold_daily.REVENUE]}
        self.assertEqual(found, {("2026-01-01", "VOICE"): (2, 150), ("2026-01-01", "SMS"): (1, 40), ("2026-01-02", "VOICE"): (1, 7)})

    def test_cell_traffic_counts_distinct_subscribers_and_adds_data_volume_both_ways(self):
        rows = [silver_row(self.DAY1, "VOICE", who="h1", cell="C1", duration=10, charge=1),
                silver_row(self.DAY1, "SMS", who="h1", cell="C1", segments=3, charge=2),
                silver_row(self.DAY1, "DATA", who="h2", cell="C1", up=100, down=900, charge=4),
                silver_row(self.DAY1, "VOICE", who="h3", cell="C2", duration=5, charge=8)]
        cells = {r["cell_id"]: r for r in self.build(rows)[gold_daily.CELLS]}
        self.assertEqual((cells["C1"]["events"], cells["C1"]["distinct_subscribers"]), (3, 2))
        self.assertEqual((cells["C1"]["voice_seconds"], cells["C1"]["sms_segments"], cells["C1"]["data_bytes"]), (10, 3, 1000))
        self.assertEqual(cells["C1"]["charge_paisa"], 7)
        self.assertEqual((cells["C2"]["events"], cells["C2"]["distinct_subscribers"], cells["C2"]["charge_paisa"]), (1, 1, 8))

    def test_the_three_tables_agree_on_the_total_charge(self):
        rows = [silver_row(self.DAY1, "VOICE", duration=1, charge=100), silver_row(self.DAY1, "SMS", who="h2", segments=1, charge=40),
                silver_row(self.DAY2, "DATA", who="h3", cell="C2", up=1, down=1, charge=9)]
        tables = self.build(rows)
        totals = {name: sum(r["charge_paisa"] for r in found) for name, found in tables.items()}
        self.assertEqual(set(totals.values()), {149})

    def test_no_silver_rows_give_no_gold_rows(self):
        self.assertEqual({name: len(found) for name, found in self.build([]).items()},
                         {gold_daily.USAGE: 0, gold_daily.REVENUE: 0, gold_daily.CELLS: 0})


if __name__ == "__main__":
    unittest.main()
