# spark_batch_stats.py
# Batch stats from local CSV or S3 (use "s3a://bucket/key" with proper Hadoop AWS jars)
#
# pip install pyspark

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min as spark_min, max as spark_max, variance, col

# Local by default; you can point to s3a://bucket/prefix/flight_*.csv
INPUT_PATH = "logs/flight_*.csv"

spark = (
    SparkSession.builder
    .appName("DroneBatchStats")
    .getOrCreate()
)

# Read & harden types so variance works
df = (
    spark.read.option("header", True).option("inferSchema", True).csv(INPUT_PATH)
      .withColumn("height_cm", col("height_cm").cast("double"))
      .withColumn("speed_cms", col("speed_cms").cast("double"))
      .withColumn("angle_yaw_deg", col("angle_yaw_deg").cast("double"))
      .dropna(subset=["height_cm", "speed_cms", "angle_yaw_deg"])
)

stats = df.select(
    avg(col("height_cm")).alias("avg_height_cm"),
    spark_min(col("height_cm")).alias("min_height_cm"),
    spark_max(col("height_cm")).alias("max_height_cm"),
    avg(col("speed_cms")).alias("avg_speed_cms"),
    spark_min(col("speed_cms")).alias("min_speed_cms"),
    spark_max(col("speed_cms")).alias("max_speed_cms"),
    variance(col("angle_yaw_deg")).alias("yaw_variance_deg2"),
)

stats.show(truncate=False)

spark.stop()
