"""Reconcile the gold tables with silver and print KEY=value lines for the smoke test. Runs locally in the driver (no YARN)."""
from pyspark.sql import functions as F

from gold_daily import CELLS, REVENUE, USAGE
from silver_cdr import SILVER, build_session

spark = build_session()
spark.sparkContext.setLogLevel("ERROR")


def totals(table, **columns):
    """One row of sums; every name maps to a SQL expression over the table."""
    return table.agg(*[F.expr(e).alias(n) for n, e in columns.items()]).first()


def line(prefix, row):
    return " ".join(f"{prefix}_{name.upper()}={value if value is not None else 0}" for name, value in row.asDict().items())


silver = spark.table(SILVER)
# Each metric counts for its own event type only; silver keeps stray values on other types, which gold must ignore.
s = totals(silver, rows="count(*)", charge="sum(charge_paisa)",
           voice="sum(case when event_type = 'VOICE' then duration_sec else 0 end)",
           sms="sum(case when event_type = 'SMS' then sms_segments else 0 end)",
           bytes="sum(case when event_type = 'DATA' then bytes_up + bytes_down else 0 end)",
           calls="sum(case when event_type = 'VOICE' then 1 else 0 end)",
           texts="sum(case when event_type = 'SMS' then 1 else 0 end)", sessions="sum(case when event_type = 'DATA' then 1 else 0 end)",
           stray_voice="sum(case when event_type <> 'VOICE' then coalesce(duration_sec, 0) else 0 end)",
           stray_sms="sum(case when event_type <> 'SMS' then coalesce(sms_segments, 0) else 0 end)")
usage = spark.table(USAGE)
u = totals(usage, rows="sum(events)", charge="sum(charge_paisa)", voice="sum(voice_seconds)", sms="sum(sms_segments)",
           bytes="sum(data_bytes_up + data_bytes_down)", calls="sum(voice_calls)", texts="sum(sms_count)", sessions="sum(data_sessions)")
revenue = spark.table(REVENUE)
r = totals(revenue, rows="sum(events)", charge="sum(charge_paisa)")
cells = spark.table(CELLS)
c = totals(cells, rows="sum(events)", charge="sum(charge_paisa)", voice="sum(voice_seconds)", sms="sum(sms_segments)", bytes="sum(data_bytes)")

# Each table must have one row per key.
duplicate_keys = sum(t.count() - t.select(*k).distinct().count() for t, k in (
    (usage, ["usage_date", "subscriber_hash"]), (revenue, ["usage_date", "event_type"]), (cells, ["usage_date", "cell_id"])))

# Per day and event type, gold must agree with a fresh count over silver.
by_type = silver.groupBy(F.to_date("event_time").alias("usage_date"), "event_type").agg(F.count("*").alias("n"))
type_mismatch = by_type.join(revenue, ["usage_date", "event_type"], "full_outer").where("n IS NULL OR events IS NULL OR n <> events").count()

# Per day, the number of subscriber rows must equal the distinct subscribers silver has.
subs = silver.groupBy(F.to_date("event_time").alias("usage_date")).agg(F.countDistinct("subscriber_hash").alias("n"))
gold_subs = usage.groupBy("usage_date").agg(F.count("*").alias("g"))
subscriber_mismatch = subs.join(gold_subs, "usage_date", "full_outer").where("n IS NULL OR g IS NULL OR n <> g").count()

# When each day was last computed, to show which days a run touched.
computed = usage.groupBy("usage_date").agg(F.expr("unix_micros(min(computed_at))").alias("t")).orderBy("usage_date").collect()

print(line("SILVER", s))
print(line("USAGE", u))
print(line("REVENUE", r))
print(line("CELL", c))
print(f"DUPLICATE_KEYS={duplicate_keys} TYPE_MISMATCH={type_mismatch} SUBSCRIBER_MISMATCH={subscriber_mismatch}")
print("DAYS=" + ",".join(f"{row.usage_date}:{row.t}" for row in computed))
