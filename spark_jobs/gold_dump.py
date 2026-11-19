"""Print the gold tables as one JSON line (GOLD_JSON=...), in the shape oracle/oracle.py compares with. Runs locally in the driver."""
import json

from pyspark.sql import functions as F

from gold_daily import CELLS, REVENUE, USAGE
from silver_cdr import build_session

spark = build_session()
spark.sparkContext.setLogLevel("ERROR")

usage, revenue, cell_traffic = spark.table(USAGE), spark.table(REVENUE), spark.table(CELLS)

days = usage.groupBy("usage_date").agg(
    F.sum("events").alias("events"), F.count("*").alias("subscribers"), F.sum("charge_paisa").alias("charge_paisa"),
    F.sum("voice_calls").alias("voice_calls"), F.sum("sms_count").alias("sms_count"), F.sum("data_sessions").alias("data_sessions"),
    F.sum("voice_seconds").alias("voice_seconds"), F.sum("sms_segments").alias("sms_segments"),
    F.sum("data_bytes_up").alias("data_bytes_up"), F.sum("data_bytes_down").alias("data_bytes_down"))
types = revenue.select("usage_date", "event_type", "events", "charge_paisa")
cells = cell_traffic.select("usage_date", "cell_id", "events", "distinct_subscribers", "charge_paisa",
                            "voice_seconds", "sms_segments", "data_bytes")


def duplicate_keys(label, table, *key):
    """The keys a table holds more than once. This has to look at the table itself: once it is grouped or turned into a
    dict, a repeated row is merged away."""
    repeated = table.groupBy(*key).count().where("count > 1").collect()
    return [f"{label} " + "|".join(str(r[k]) for k in key) for r in repeated]


def rows(df, *key):
    return {"|".join(str(r[k]) for k in key): {c: r[c] for c in df.columns if c not in key} for r in df.collect()}


print("GOLD_JSON=" + json.dumps({
    "days": rows(days, "usage_date"), "types": rows(types, "usage_date", "event_type"), "cells": rows(cells, "usage_date", "cell_id"),
    "duplicate_keys": (duplicate_keys("daily_usage_per_subscriber", usage, "usage_date", "subscriber_hash")
                       + duplicate_keys("revenue_by_event_type", revenue, "usage_date", "event_type")
                       + duplicate_keys("cell_traffic", cell_traffic, "usage_date", "cell_id")),
}))
