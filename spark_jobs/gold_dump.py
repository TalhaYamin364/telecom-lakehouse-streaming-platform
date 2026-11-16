"""Print the gold tables as one JSON line (GOLD_JSON=...), in the shape oracle/oracle.py compares with. Runs locally in the driver."""
import json

from pyspark.sql import functions as F

from gold_daily import CELLS, REVENUE, USAGE
from silver_cdr import build_session

spark = build_session()
spark.sparkContext.setLogLevel("ERROR")

days = spark.table(USAGE).groupBy("usage_date").agg(
    F.sum("events").alias("events"), F.count("*").alias("subscribers"), F.sum("charge_paisa").alias("charge_paisa"),
    F.sum("voice_calls").alias("voice_calls"), F.sum("sms_count").alias("sms_count"), F.sum("data_sessions").alias("data_sessions"),
    F.sum("voice_seconds").alias("voice_seconds"), F.sum("sms_segments").alias("sms_segments"),
    F.sum(F.col("data_bytes_up") + F.col("data_bytes_down")).alias("data_bytes"))
types = spark.table(REVENUE).select("usage_date", "event_type", "events", "charge_paisa")
cells = spark.table(CELLS).select("usage_date", "cell_id", "events", "distinct_subscribers", "charge_paisa")


def rows(df, *key):
    return {"|".join(str(r[k]) for k in key): {c: r[c] for c in df.columns if c not in key} for r in df.collect()}


print("GOLD_JSON=" + json.dumps({
    "days": rows(days, "usage_date"), "types": rows(types, "usage_date", "event_type"), "cells": rows(cells, "usage_date", "cell_id"),
}))
