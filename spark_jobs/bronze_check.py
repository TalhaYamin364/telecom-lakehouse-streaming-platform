"""Print how many rows lake.bronze.cdr_raw holds and how many distinct Kafka (partition, offset) pairs they cover.

Runs locally in the driver (no YARN) so it is quick. Output: ROWS=<n> DISTINCT_OFFSETS=<n> MAX_OFFSETS=<partition:offset,...>
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.master("local[1]").appName("bronze-check")
    .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lake.type", "hadoop")
    .config("spark.sql.catalog.lake.warehouse", "hdfs://namenode:8020/warehouse")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

table = spark.table("lake.bronze.cdr_raw")
rows = table.count()
distinct = table.select("kafka_partition", "kafka_offset").distinct().count()
maxima = table.groupBy("kafka_partition").agg(F.max("kafka_offset").alias("m")).orderBy("kafka_partition").collect()
print(f"ROWS={rows} DISTINCT_OFFSETS={distinct} MAX_OFFSETS=" + ",".join(f"{r.kafka_partition}:{r.m}" for r in maxima))
