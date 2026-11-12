"""Inspect lake.silver.cdr and print KEY=value lines for the smoke test. Runs locally in the driver (no YARN)."""
from pyspark.sql import functions as F

from silver_cdr import BRONZE, REJECTED, SILVER, build_session, latest_per_event, parse

spark = build_session()
spark.sparkContext.setLogLevel("ERROR")

silver = spark.table(SILVER)
rows = silver.count()
distinct_ids = silver.select("event_id").distinct().count()

# Any raw phone number in any string column of silver or the quarantine table would be a privacy leak.
# This is the format silver accepts (+8801 and nine digits), not just the generator's prefix, and it is not anchored,
# so a number embedded in a longer string is found too.
raw_number = "[+]8801[0-9]{9}"


def leaked(table):
    string_columns = [c for c, t in table.dtypes if t == "string"]
    return table.where(" OR ".join(f"{c} RLIKE '{raw_number}'" for c in string_columns)).count() if string_columns else 0


rejected_table = spark.table(REJECTED)
leaks = leaked(silver) + leaked(rejected_table)

# The copy with the latest emitted_at in bronze must be what silver holds.
bronze = parse(spark.table(BRONZE)).where("reason IS NULL")
expected = latest_per_event(bronze.select("event_id", F.col("emitted_ts").alias("emitted_at"), "charge_paisa", "kafka_offset"))
mismatch = (
    expected.alias("e").join(silver.alias("s"), "event_id", "full_outer")
    .where("e.event_id IS NULL OR s.event_id IS NULL OR e.charge_paisa <> s.charge_paisa OR e.emitted_at <> s.emitted_at")
    .count()
)
corrected = bronze.groupBy("event_id").agg(F.countDistinct("charge_paisa").alias("c")).where("c > 1").count()

days = [r.d for r in silver.select(F.date_format("event_time", "yyyy-MM-dd").alias("d")).distinct().orderBy("d").collect()]
reasons = {r.reason: r["count"] for r in rejected_table.groupBy("reason").count().collect()}
fix = silver.where("event_id = 'fix-time'")  # the smoke test sends this event twice, the second time with a different event_time
fix_row = fix.select("charge_paisa", F.date_format("event_time", "yyyy-MM-dd").alias("d")).first()
print(f"SILVER_ROWS={rows} DISTINCT_IDS={distinct_ids} LEAKED_NUMBERS={leaks} LATEST_MISMATCH={mismatch} CORRECTED_IDS={corrected}")
print(f"FIXTIME_ROWS={fix.count()} FIXTIME_CHARGE={fix_row.charge_paisa if fix_row else 'none'} FIXTIME_DAY={fix_row.d if fix_row else 'none'}")
print(f"SUBSCRIBERS={silver.select('subscriber_hash').distinct().count()} DAYS={','.join(days)}")
print("REJECTED=" + str(sum(reasons.values())) + " REASONS=" + ",".join(f"{k}:{v}" for k, v in sorted(reasons.items())))
