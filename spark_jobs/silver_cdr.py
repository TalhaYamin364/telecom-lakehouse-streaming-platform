"""Build lake.silver.cdr from lake.bronze.cdr_raw: parse, validate, mask phone numbers and deduplicate on event_id.

- Incremental: reads only the bronze snapshots added since the last run (progress is kept in lake.silver.progress).
  --full reads all of bronze again.
- Idempotent: rows are MERGEd on event_id and a row is only replaced by a copy with a later emitted_at, so running it
  twice, or replaying the same events, changes nothing. If the job dies before it records its progress, the next run
  reads the same snapshots again and reaches the same result.
- Late events need no special case: a row lands in the partition of its event_time, whenever it arrives.
- Bad records go to lake.silver.cdr_rejected with a reason instead of breaking the job or being dropped.
- Phone numbers never reach silver: each becomes a salted SHA-256 hash (to count per subscriber) and a masked form.
"""
import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

BRONZE = "lake.bronze.cdr_raw"
SILVER = "lake.silver.cdr"
REJECTED = "lake.silver.cdr_rejected"
PROGRESS = "lake.silver.progress"
CHANGED_DAYS = "lake.silver.changed_days"  # which event days a run changed; the gold layer recomputes exactly those days

MSISDN = r"^\+8801[0-9]{9}$"  # Bangladesh mobile number in international form
PHONE_ANYWHERE = r"\+8801[0-9]{9}"  # the same format, found anywhere inside a longer string
EVENT_TYPES = ("VOICE", "SMS", "DATA")

SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("msisdn", T.StringType()),
    T.StructField("counterparty_msisdn", T.StringType()),
    T.StructField("cell_id", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("emitted_at", T.StringType()),
    T.StructField("duration_sec", T.LongType()),
    T.StructField("sms_segments", T.LongType()),
    T.StructField("bytes_up", T.LongType()),
    T.StructField("bytes_down", T.LongType()),
    T.StructField("charge_paisa", T.LongType()),
])


def build_session():
    return (
        SparkSession.builder.appName("silver-cdr")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", "hdfs://namenode:8020/warehouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def create_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.silver")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER} (
            event_id STRING, event_type STRING,
            subscriber_hash STRING, subscriber_masked STRING,
            counterparty_hash STRING, counterparty_masked STRING,
            cell_id STRING, event_time TIMESTAMP, emitted_at TIMESTAMP,
            duration_sec BIGINT, sms_segments BIGINT, bytes_up BIGINT, bytes_down BIGINT, charge_paisa BIGINT,
            kafka_partition INT, kafka_offset BIGINT, processed_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (days(event_time))
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {REJECTED} (
            kafka_partition INT, kafka_offset BIGINT, reason STRING, processed_at TIMESTAMP
        ) USING iceberg
    """)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {PROGRESS} (source STRING, snapshot_id BIGINT, updated_at TIMESTAMP) USING iceberg")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {CHANGED_DAYS} (day DATE, changed_at TIMESTAMP) USING iceberg")


def rejection_reason(row):
    """The first rule a record breaks, or NULL if it is valid. A NULL input counts as breaking the rule."""
    checks = [
        ("invalid json", row.isNull()),
        ("missing event_id", F.col("event_id").isNull() | (F.col("event_id") == "")),
        ("unknown event_type", ~F.col("event_type").isin(*EVENT_TYPES)),
        ("invalid msisdn", ~F.col("msisdn").rlike(MSISDN)),
        ("invalid counterparty", F.col("event_type").isin("VOICE", "SMS") & ~F.col("counterparty_msisdn").rlike(MSISDN)),
        ("missing cell_id", F.col("cell_id").isNull() | (F.trim(F.col("cell_id")) == "")),
        # event_id and cell_id are copied to silver as they are, so a phone number in either would leak.
        ("phone number in event_id or cell_id", F.col("event_id").rlike(PHONE_ANYWHERE) | F.col("cell_id").rlike(PHONE_ANYWHERE)),
        ("invalid event_time", F.col("event_ts").isNull()),
        ("invalid emitted_at", F.col("emitted_ts").isNull()),
        ("event_time after emitted_at", F.col("event_ts") > F.col("emitted_ts")),
        ("invalid duration", (F.col("event_type") == "VOICE") & ~(F.col("duration_sec") >= 0)),
        ("invalid sms_segments", (F.col("event_type") == "SMS") & ~(F.col("sms_segments") >= 1)),
        ("invalid data volume", (F.col("event_type") == "DATA") & ~((F.col("bytes_up") >= 0) & (F.col("bytes_down") >= 0))),
        ("invalid charge", ~(F.col("charge_paisa") >= 0)),
    ]
    return F.coalesce(*[F.when(F.coalesce(broken, F.lit(True)), F.lit(reason)) for reason, broken in checks])


def parse(raw):
    parsed = raw.withColumn("p", F.from_json("raw_value", SCHEMA))
    flat = parsed.select("kafka_partition", "kafka_offset", "raw_value", "p", "p.*")
    # try_to_timestamp gives NULL for a bad value; Spark 4 would otherwise fail the whole job on it.
    flat = flat.withColumn("event_ts", F.expr("try_to_timestamp(event_time)")).withColumn("emitted_ts", F.expr("try_to_timestamp(emitted_at)"))
    return flat.withColumn("reason", rejection_reason(F.col("p")))


def mask_and_shape(valid, salt):
    def hashed(col):
        return F.when(F.col(col).isNotNull(), F.sha2(F.concat_ws("|", F.lit(salt), F.col(col)), 256))

    def masked(col):  # +88010******34: country code and prefix, then only the last two digits
        return F.concat(F.substring(F.col(col), 1, 6), F.lit("******"), F.substring(F.col(col), -2, 2))

    return valid.select(
        "event_id", "event_type",
        hashed("msisdn").alias("subscriber_hash"), masked("msisdn").alias("subscriber_masked"),
        hashed("counterparty_msisdn").alias("counterparty_hash"), masked("counterparty_msisdn").alias("counterparty_masked"),
        "cell_id", F.col("event_ts").alias("event_time"), F.col("emitted_ts").alias("emitted_at"),
        "duration_sec", "sms_segments", "bytes_up", "bytes_down", "charge_paisa",
        "kafka_partition", "kafka_offset", F.current_timestamp().alias("processed_at"),
    )


def latest_per_event(df):
    """One row per event_id: the copy with the latest emitted_at (the correction beats the original)."""
    w = Window.partitionBy("event_id").orderBy(F.col("emitted_at").desc(), F.col("kafka_offset").desc())
    return df.withColumn("rn", F.row_number().over(w)).where("rn = 1").drop("rn")


def record_changed_days(spark, batch):
    """Note which event days this batch will change, before the merge changes them.

    A day is changed by a new event, by a correction (later emitted_at), and also by the day a corrected event
    moved away from. An exact re-send changes nothing and is not recorded. Recording first means a crash can only
    cause an unnecessary recompute, never a missed one: after the merge the rows would no longer look new.
    """
    existing = spark.table(SILVER).select(
        "event_id", F.col("event_time").alias("old_time"), F.col("emitted_at").alias("old_emitted_at"))
    changes = batch.join(existing, "event_id", "left").where("old_emitted_at IS NULL OR emitted_at > old_emitted_at")
    days = (
        changes.select(F.to_date("event_time").alias("day"))
        .union(changes.where("old_time IS NOT NULL").select(F.to_date("old_time").alias("day")))
        .distinct()
        .withColumn("changed_at", F.current_timestamp())
    )
    days.writeTo(CHANGED_DAYS).append()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--full", action="store_true", help="read all of bronze again instead of only what is new")
    args = parser.parse_args()
    # A local default so the stack runs; a real deployment would take the salt from a secret store.
    salt = os.environ.get("MSISDN_SALT", "dev-only-salt")

    spark = build_session()
    spark.sparkContext.setLogLevel("WARN")
    create_tables(spark)

    head = spark.sql(f"SELECT snapshot_id FROM {BRONZE}.refs WHERE name = 'main'").first()
    if head is None:
        print("bronze is empty; nothing to do")
        return
    last = spark.sql(f"SELECT snapshot_id FROM {PROGRESS} WHERE source = 'bronze'").first()
    reader = spark.read.format("iceberg")
    if last is not None and not args.full:
        if last.snapshot_id == head.snapshot_id:
            print("no new bronze data; nothing to do")
            return
        reader = reader.option("start-snapshot-id", last.snapshot_id).option("end-snapshot-id", head.snapshot_id)

    flat = parse(reader.load(BRONZE)).cache()

    # The payload is not copied here: it holds raw phone numbers, and the original stays available in bronze by partition and offset.
    flat.where("reason IS NOT NULL").select(
        "kafka_partition", "kafka_offset", "reason", F.current_timestamp().alias("processed_at")
    ).createOrReplaceTempView("rejected_batch")
    spark.sql(f"""
        MERGE INTO {REJECTED} t USING rejected_batch s
        ON t.kafka_partition = s.kafka_partition AND t.kafka_offset = s.kafka_offset
        WHEN NOT MATCHED THEN INSERT *
    """)

    batch = latest_per_event(mask_and_shape(flat.where("reason IS NULL"), salt))
    record_changed_days(spark, batch)
    batch.createOrReplaceTempView("silver_batch")
    # Match on event_id alone: a correction may change event_time, so the match must not be limited to a time range.
    spark.sql(f"""
        MERGE INTO {SILVER} t USING silver_batch s
        ON t.event_id = s.event_id
        WHEN MATCHED AND s.emitted_at > t.emitted_at THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    spark.sql(f"""
        MERGE INTO {PROGRESS} t
        USING (SELECT 'bronze' AS source, CAST({head.snapshot_id} AS BIGINT) AS snapshot_id, current_timestamp() AS updated_at) s
        ON t.source = s.source
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print(f"silver: read {flat.count()} bronze rows, {flat.where('reason IS NOT NULL').count()} rejected")


if __name__ == "__main__":
    main()
