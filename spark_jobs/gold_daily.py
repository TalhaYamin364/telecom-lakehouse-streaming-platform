"""Build the gold tables from silver, one event day at a time.

Tables (all partitioned by usage_date):
  lake.gold.daily_usage_per_subscriber  one row per subscriber per day: calls, minutes, SMS, data volume, charge
  lake.gold.revenue_by_event_type       one row per event type per day
  lake.gold.cell_traffic                one row per cell per day: events, subscribers, volumes, charge

How it stays correct and safe to rerun:
- A day is always recomputed from silver in full, never added to. The result replaces that day's rows in one atomic
  Iceberg overwrite, so a rerun, a replay or a crash halfway gives the same numbers and never duplicates a row.
- By default it recomputes the days silver changed since the last run (lake.silver.changed_days), including the day a
  corrected event moved away from, so late and corrected events reach gold. It only acts on markers from a silver run
  that has finished. On its very first run it builds every day silver has, so it can be added to, or rebuilt over, a
  lake that already holds silver data.
- Each metric is summed over its own event type only: call seconds over VOICE, SMS segments over SMS, bytes over DATA.
- --day recomputes exactly the days you name and leaves every other day alone: the safe backfill.
"""
import argparse
from datetime import date, timedelta

from pyspark.sql import functions as F

from silver_cdr import CHANGED_DAYS, SILVER, build_session
from silver_cdr import PROGRESS as SILVER_PROGRESS

USAGE = "lake.gold.daily_usage_per_subscriber"
REVENUE = "lake.gold.revenue_by_event_type"
CELLS = "lake.gold.cell_traffic"
PROGRESS = "lake.gold.progress"


def create_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.gold")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {USAGE} (
            usage_date DATE, subscriber_hash STRING, subscriber_masked STRING, events BIGINT,
            voice_calls BIGINT, voice_seconds BIGINT, sms_count BIGINT, sms_segments BIGINT,
            data_sessions BIGINT, data_bytes_up BIGINT, data_bytes_down BIGINT, charge_paisa BIGINT, computed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (usage_date)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REVENUE} (
            usage_date DATE, event_type STRING, events BIGINT, charge_paisa BIGINT, computed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (usage_date)
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CELLS} (
            usage_date DATE, cell_id STRING, events BIGINT, distinct_subscribers BIGINT, voice_seconds BIGINT,
            sms_segments BIGINT, data_bytes BIGINT, charge_paisa BIGINT, computed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (usage_date)
    """)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {PROGRESS} (source STRING, last_changed_at TIMESTAMP) USING iceberg")


def of_type(kind):
    return F.col("event_type") == kind


def count_if(condition):
    return F.sum(F.when(condition, 1).otherwise(0)).cast("bigint")


def total(column):
    return F.coalesce(F.sum(column), F.lit(0)).cast("bigint")


def total_for(kind, column):
    """Sum a metric over one event type only. Silver validates a metric for its own type but keeps stray values on
    other types (a DATA record with a duration_sec), and those must not count as call seconds."""
    return F.coalesce(F.sum(F.when(of_type(kind), F.col(column))), F.lit(0)).cast("bigint")


def aggregates(silver_days):
    d = silver_days.withColumn("usage_date", F.to_date("event_time"))
    now = F.current_timestamp().alias("computed_at")
    usage = d.groupBy("usage_date", "subscriber_hash").agg(
        F.max("subscriber_masked").alias("subscriber_masked"), F.count("*").alias("events"),
        count_if(of_type("VOICE")).alias("voice_calls"), total_for("VOICE", "duration_sec").alias("voice_seconds"),
        count_if(of_type("SMS")).alias("sms_count"), total_for("SMS", "sms_segments").alias("sms_segments"),
        count_if(of_type("DATA")).alias("data_sessions"), total_for("DATA", "bytes_up").alias("data_bytes_up"),
        total_for("DATA", "bytes_down").alias("data_bytes_down"), total("charge_paisa").alias("charge_paisa"),
    ).select("usage_date", "subscriber_hash", "subscriber_masked", "events", "voice_calls", "voice_seconds", "sms_count",
             "sms_segments", "data_sessions", "data_bytes_up", "data_bytes_down", "charge_paisa", now)
    revenue = d.groupBy("usage_date", "event_type").agg(
        F.count("*").alias("events"), total("charge_paisa").alias("charge_paisa"),
    ).select("usage_date", "event_type", "events", "charge_paisa", now)
    cells = d.groupBy("usage_date", "cell_id").agg(
        F.count("*").alias("events"), F.countDistinct("subscriber_hash").alias("distinct_subscribers"),
        total_for("VOICE", "duration_sec").alias("voice_seconds"), total_for("SMS", "sms_segments").alias("sms_segments"),
        (total_for("DATA", "bytes_up") + total_for("DATA", "bytes_down")).alias("data_bytes"), total("charge_paisa").alias("charge_paisa"),
    ).select("usage_date", "cell_id", "events", "distinct_subscribers", "voice_seconds", "sms_segments",
             "data_bytes", "charge_paisa", now)
    return {USAGE: usage, REVENUE: revenue, CELLS: cells}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--day", action="append", metavar="YYYY-MM-DD",
                        help="recompute only this day (repeat for several); other days and the progress record are left alone")
    args = parser.parse_args()

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    create_tables(spark)

    newest_change = None
    if args.day:
        days = sorted({date.fromisoformat(d) for d in args.day})
    else:
        # Only the markers of a silver run that has finished: silver records its progress last, after it has merged, so
        # a marker newer than that belongs to a run still in flight and gold must not act on it yet.
        # Compared inside Spark, so no timestamp passes through Python and its time zone.
        first_run = spark.sql(f"SELECT COUNT(*) FROM {PROGRESS} WHERE source = 'silver'").first()[0] == 0
        completed = spark.sql(f"""
            SELECT day, changed_at FROM {CHANGED_DAYS}
            WHERE changed_at <= (SELECT MAX(updated_at) FROM {SILVER_PROGRESS} WHERE source = 'bronze')
              AND changed_at > COALESCE((SELECT MAX(last_changed_at) FROM {PROGRESS} WHERE source = 'silver'),
                                        TIMESTAMP '1970-01-01 00:00:00')
        """)
        summary = completed.agg(
            F.collect_set("day").alias("days"),
            F.date_format(F.max("changed_at"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("newest"),
        ).first()
        days, newest_change = sorted(summary.days or []), summary.newest
        if first_run:
            # Gold has never run, so silver may hold days that were never recorded as changed (silver built first,
            # or gold wiped to rebuild it): build every day silver has.
            every_day = spark.table(SILVER).agg(F.collect_set(F.to_date("event_time")).alias("days")).first().days
            days = sorted(set(days) | set(every_day or []))
    if not days:
        print("no changed days; nothing to do")
        return

    low, high = days[0], days[-1] + timedelta(days=1)
    silver_days = spark.table(SILVER).where((F.col("event_time") >= F.lit(low)) & (F.col("event_time") < F.lit(high))
                                            & F.to_date("event_time").isin([F.lit(d) for d in days]))
    for table, frame in aggregates(silver_days).items():
        # One atomic commit per table: the days' old rows go and the recomputed rows arrive together, even if the
        # day is now empty (for example, all of its events moved to another day).
        frame.writeTo(table).overwrite(F.col("usage_date").isin([F.lit(d) for d in days]))

    if not args.day:
        # Save the boundary even when there were no markers (the first run on an old lake), so the next run is not "first".
        spark.sql(f"""
            MERGE INTO {PROGRESS} t
            USING (SELECT 'silver' AS source, TIMESTAMP '{newest_change or "1970-01-01 00:00:00"}' AS last_changed_at) s
            ON t.source = s.source
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    print("gold: recomputed " + ", ".join(str(d) for d in days))


if __name__ == "__main__":
    main()
