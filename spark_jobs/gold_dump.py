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
    F.sum("data_bytes_up").alias("data_bytes_up"), F.sum("data_bytes_down").alias("data_bytes_down"))
types = spark.table(REVENUE).select("usage_date", "event_type", "events", "charge_paisa")
cells = spark.table(CELLS).select("usage_date", "cell_id", "events", "distinct_subscribers", "charge_paisa",
                                  "voice_seconds", "sms_segments", "data_bytes")


def rows(label, df, *key):
    """The rows by key, and the keys that occur more than once, which a dict would silently overwrite."""
    found, duplicates = {}, []
    for r in df.collect():
        name = "|".join(str(r[k]) for k in key)
        if name in found:
            duplicates.append(f"{label} {name}")
        found[name] = {c: r[c] for c in df.columns if c not in key}
    return found, duplicates


days_rows, days_dups = rows("daily_usage_per_subscriber", days, "usage_date")
types_rows, types_dups = rows("revenue_by_event_type", types, "usage_date", "event_type")
cells_rows, cells_dups = rows("cell_traffic", cells, "usage_date", "cell_id")
print("GOLD_JSON=" + json.dumps({
    "days": days_rows, "types": types_rows, "cells": cells_rows, "duplicate_keys": days_dups + types_dups + cells_dups,
}))
