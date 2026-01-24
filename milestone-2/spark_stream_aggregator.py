# spark_batch_aggregator.py
# Part B – Batch job:
#   - Read all messages from Kafka topic "drone_telemetry"
#   - Compute 10-second window averages for height, battery, pitch, roll, yaw
#   - Save a single aggregated CSV folder: aggregated_telemetry/

import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    avg,
    window,
    from_unixtime,
)
from pyspark.sql.types import StructType, StructField, DoubleType

# Make Spark use this Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


def main():

    # Creating spark application named: DroneTelemetryBatchAggregation
    # Adding kafka connector so spark can read from kafka
    # Keep output readable by setting log as warn.
    spark = (
        SparkSession.builder
        .appName("DroneTelemetryBatchAggregation")
        # Match your streaming job's connector version:
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # JSON schema (same structure as kafka's output)
    schema = StructType([
        StructField("timestamp", DoubleType()),
        StructField("height",   DoubleType()),
        StructField("pitch",    DoubleType()),
        StructField("roll",     DoubleType()),
        StructField("yaw",      DoubleType()),
        StructField("battery",  DoubleType()),
    ])

    # Read all Kafka messages as an one time pull, not streaming.
    df_kafka = (
        spark.read
             .format("kafka")
             .option("kafka.bootstrap.servers", "localhost:9092")
             .option("subscribe", "drone_telemetry")
             .option("startingOffsets", "earliest")  # from beginning
             .option("endingOffsets", "latest")      # up to latest now
             .load()
    )

    # Exit early if there is nothing already in kafka topic
    # This means drone did not flew before, thus no data to input into spark
    if df_kafka.rdd.isEmpty():
        print("No messages found in Kafka topic 'drone_telemetry'.")
        print("Did you run kafka_producer.py and fly the drone?")
        spark.stop()
        return

    # Parse JSON
    json_df = df_kafka.select(
        from_json(col("value").cast("string"), schema).alias("data")
    )
    telemetry_df = json_df.select("data.*")

    # Drop any rows without a timestamp.
    telemetry_df = telemetry_df.dropna(subset=["timestamp"])

    # Convert epoch seconds to proper timestamp column
    telemetry_ts_df = telemetry_df.withColumn(
        "event_time",
        from_unixtime(col("timestamp")).cast("timestamp"),
    )

    # 10-second window aggregation
    # Read the data in 10 seconds window.
    # Group by the 10 seconds and find average of height, battery, pitch, roll, and yaw.
    # Flatten the window to start and end time
    # Sort the order rows according to start time.
    agg_df = (
        telemetry_ts_df
        .groupBy(window(col("event_time"), "10 seconds"))
        .agg(
            avg("height").alias("avg_height"),
            avg("battery").alias("avg_battery"),
            avg("pitch").alias("avg_pitch"),
            avg("roll").alias("avg_roll"),
            avg("yaw").alias("avg_yaw"),
        )
        .select(
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            "avg_height",
            "avg_battery",
            "avg_pitch",
            "avg_roll",
            "avg_yaw",
        )
        .orderBy("start_time")
    )

    # Write this into a single CSV file
    print("Aggregated windowed results (first few rows):")
    agg_df.show(truncate=False)

    # ----- Write one nice CSV folder -----
    (
        agg_df
        .coalesce(1) # single CSV file (plus _SUCCESS) this is easier for Pandas which we will use for visualization
        .write
        .mode("overwrite") # if ran again, then replace with the previous folder.
        .option("header", "true")
        .csv("aggregated_telemetry")
    )

    # Write that CSV file into aggregated_telemetry folder
    print("\nWrote aggregated CSV to ./aggregated_telemetry/")
    spark.stop()


if __name__ == "__main__":
    main()
