#!/usr/bin/env python
# coding: utf-8
#
# AWS S3 equivalent of 06_spark_sql_big_query.py
# Reads parquet data from S3, runs revenue aggregations, writes results back to S3.
#
# Usage (Spark 4.x on Windows):
#   spark-submit \
#     --packages org.apache.hadoop:hadoop-aws:3.4.2,com.amazonaws:aws-java-sdk-bundle:1.12.787 \
#     --conf spark.driver.host=localhost \
#     --conf spark.driver.bindAddress=127.0.0.1 \
#     --conf spark.hadoop.fs.s3a.analytics.accelerator.enabled=false \
#     06_spark_sql_s3.py \
#       --input_green=s3a://dezoomcamp-data-lake-ym/green/ \
#       --input_yellow=s3a://dezoomcamp-data-lake-ym/yellow/ \
#       --output=s3a://dezoomcamp-data-lake-ym/reports/revenue-2019

import argparse
import boto3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    LongType, DoubleType, StringType, TimestampType,
)

parser = argparse.ArgumentParser()
parser.add_argument("--input_green",  required=True)
parser.add_argument("--input_yellow", required=True)
parser.add_argument("--output",       required=True)
args = parser.parse_args()

# ── Spark session ─────────────────────────────────────────────────────────────
spark = SparkSession.builder.appName("nyc-taxi-revenue-s3").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# ── S3A config — credentials from ~/.aws/credentials (never hardcoded) ────────
session = boto3.Session()
creds   = session.get_credentials().get_frozen_credentials()

hc = spark._jsc.hadoopConfiguration()
hc.set("fs.s3a.impl",             "org.apache.hadoop.fs.s3a.S3AFileSystem")
hc.set("fs.s3a.access.key",       creds.access_key)
hc.set("fs.s3a.secret.key",       creds.secret_key)
hc.set("fs.s3a.endpoint",         "s3.amazonaws.com")
hc.set("fs.s3a.aws.credentials.provider",
       "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
hc.set("fs.s3a.analytics.accelerator.enabled", "false")
hc.set("fs.s3a.connection.maximum",            "50")
hc.set("fs.s3a.connection.timeout",            "600000")
hc.set("fs.s3a.socket.timeout",                "600000")
hc.set("fs.s3a.attempts.maximum",              "10")

# ── Schemas — pandas writes Int64 as Parquet INT64; use LongType to match ─────
# Timestamp columns are written as TIMESTAMP(NANOS) by pandas; we read them as
# Long (nanoseconds since epoch) and convert manually to avoid Spark 4.x error.
green_schema = StructType([
    StructField("VendorID",              LongType(),   True),
    StructField("lpep_pickup_datetime",  LongType(),   True),
    StructField("lpep_dropoff_datetime", LongType(),   True),
    StructField("store_and_fwd_flag",    StringType(), True),
    StructField("RatecodeID",            LongType(),   True),
    StructField("PULocationID",          LongType(),   True),
    StructField("DOLocationID",          LongType(),   True),
    StructField("passenger_count",       LongType(),   True),
    StructField("trip_distance",         DoubleType(), True),
    StructField("fare_amount",           DoubleType(), True),
    StructField("extra",                 DoubleType(), True),
    StructField("mta_tax",               DoubleType(), True),
    StructField("tip_amount",            DoubleType(), True),
    StructField("tolls_amount",          DoubleType(), True),
    StructField("ehail_fee",             DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount",          DoubleType(), True),
    StructField("payment_type",          LongType(),   True),
    StructField("trip_type",             LongType(),   True),
    StructField("congestion_surcharge",  DoubleType(), True),
])

yellow_schema = StructType([
    StructField("VendorID",              LongType(),   True),
    StructField("tpep_pickup_datetime",  LongType(),   True),
    StructField("tpep_dropoff_datetime", LongType(),   True),
    StructField("passenger_count",       LongType(),   True),
    StructField("trip_distance",         DoubleType(), True),
    StructField("RatecodeID",            LongType(),   True),
    StructField("store_and_fwd_flag",    StringType(), True),
    StructField("PULocationID",          LongType(),   True),
    StructField("DOLocationID",          LongType(),   True),
    StructField("payment_type",          LongType(),   True),
    StructField("fare_amount",           DoubleType(), True),
    StructField("extra",                 DoubleType(), True),
    StructField("mta_tax",               DoubleType(), True),
    StructField("tip_amount",            DoubleType(), True),
    StructField("tolls_amount",          DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount",          DoubleType(), True),
    StructField("congestion_surcharge",  DoubleType(), True),
])


def ns_to_ts(df, *cols):
    """Convert nanosecond-epoch Long columns to Spark TimestampType."""
    for c in cols:
        if c in df.columns:
            df = df.withColumn(c, (F.col(c) / 1_000_000_000).cast(TimestampType()))
    return df


# ── Read ──────────────────────────────────────────────────────────────────────
print(f"\n>>> Reading green:  {args.input_green}")
df_green = ns_to_ts(
    spark.read.schema(green_schema).parquet(args.input_green),
    "lpep_pickup_datetime", "lpep_dropoff_datetime",
).withColumnRenamed("lpep_pickup_datetime",  "pickup_datetime") \
 .withColumnRenamed("lpep_dropoff_datetime", "dropoff_datetime")

print(f">>> Reading yellow: {args.input_yellow}")
df_yellow = ns_to_ts(
    spark.read.schema(yellow_schema).parquet(args.input_yellow),
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
).withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime") \
 .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

# ── Transform ─────────────────────────────────────────────────────────────────
common_columns = [
    "VendorID", "pickup_datetime", "dropoff_datetime",
    "store_and_fwd_flag", "RatecodeID", "PULocationID", "DOLocationID",
    "passenger_count", "trip_distance", "fare_amount", "extra", "mta_tax",
    "tip_amount", "tolls_amount", "improvement_surcharge",
    "total_amount", "payment_type", "congestion_surcharge",
]

df_trips = (
    df_green.select(common_columns).withColumn("service_type", F.lit("green"))
    .unionAll(
    df_yellow.select(common_columns).withColumn("service_type", F.lit("yellow")))
)
df_trips.createOrReplaceTempView("trips_data")

df_result = spark.sql("""
SELECT
    PULocationID                         AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month,
    service_type,
    SUM(fare_amount)                     AS revenue_monthly_fare,
    SUM(extra)                           AS revenue_monthly_extra,
    SUM(mta_tax)                         AS revenue_monthly_mta_tax,
    SUM(tip_amount)                      AS revenue_monthly_tip_amount,
    SUM(tolls_amount)                    AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge)           AS revenue_monthly_improvement_surcharge,
    SUM(total_amount)                    AS revenue_monthly_total_amount,
    SUM(congestion_surcharge)            AS revenue_monthly_congestion_surcharge,
    AVG(passenger_count)                 AS avg_monthly_passenger_count,
    AVG(trip_distance)                   AS avg_monthly_trip_distance
FROM trips_data
GROUP BY 1, 2, 3
""")

# ── Write ─────────────────────────────────────────────────────────────────────
print(f">>> Writing to: {args.output}")
df_result.write.mode("overwrite").format("parquet").save(args.output)
print(f"\n=== Spark job complete! Results at {args.output} ===")

spark.stop()
