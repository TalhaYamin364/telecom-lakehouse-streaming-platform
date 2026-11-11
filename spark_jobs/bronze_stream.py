"""Stream cdr.raw from Kafka into the Iceberg table lake.bronze.cdr_raw, keeping every message exactly as received.

Bronze is append-only: no parsing, no dedup. Deduplication and late-data handling belong to silver.
The checkpoint on HDFS records which Kafka offsets are already committed to Iceberg, so a restart
continues from there: no message is lost and none is written twice.
"""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLE = "lake.bronze.cdr_raw"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default="kafka:19092")
    parser.add_argument("--topic", default="cdr.raw")
    parser.add_argument("--checkpoint", default="hdfs://namenode:8020/checkpoints/bronze_cdr_raw")
    parser.add_argument("--once", action="store_true", help="process what is in Kafka now, then stop (otherwise run until stopped)")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("bronze-cdr-raw")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", "hdfs://namenode:8020/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            kafka_topic     STRING,
            kafka_partition INT,
            kafka_offset    BIGINT,
            kafka_timestamp TIMESTAMP,
            msg_key         STRING,
            raw_value       STRING,
            ingested_at     TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (days(kafka_timestamp))
    """)

    messages = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "earliest")  # only used on the very first run; later runs resume from the checkpoint
        .option("failOnDataLoss", "true")
        .load()
        .select(
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("msg_key"),
            F.col("value").cast("string").alias("raw_value"),
            F.current_timestamp().alias("ingested_at"),
        )
    )

    trigger = {"availableNow": True} if args.once else {"processingTime": "10 seconds"}
    query = (
        messages.writeStream.outputMode("append")
        .option("checkpointLocation", args.checkpoint)
        .option("fanout-enabled", "true")
        .trigger(**trigger)
        .toTable(TABLE)
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
