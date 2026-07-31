"""
Day 2 — TLC trip data transformation job for EMR Serverless.

Usage:
  spark-submit day2_transform.py <input_glob_or_paths> <output_s3_path>

Example entryPointArguments for a few months at once:
  [
    "s3://nyc-tlc/trip data/yellow_tripdata_2024-01.parquet,s3://nyc-tlc/trip data/yellow_tripdata_2024-02.parquet,s3://nyc-tlc/trip data/yellow_tripdata_2024-03.parquet",
    "s3://atharva-emr-tlc-output/day2-aggregates/"
  ]

What this does:
  1. Reads one or more months of yellow taxi parquet files
  2. Cleans malformed records (bad durations, distances, fares)
  3. Handles schema drift across months (column sets differ over the years)
  4. Computes trip-level features: duration, avg speed, anomaly flags
  5. Aggregates by pickup date/hour/zone
  6. Writes partitioned Parquet output, partitioned by pickup_date
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def build_spark():
    return (
        SparkSession.builder
        .appName("tlc-day2-transform")
        .getOrCreate()
    )

def read_source(spark, input_paths):
    # input_paths is a comma-separated string of one or more S3 parquet paths.
    paths = [p.strip() for p in input_paths.split(",")]

    # mergeSchema handles the fact that TLC has added/renamed/dropped columns
    # across years (e.g. airport_fee showing up in some months, not others).
    # This is the schema-evolution issue called out in the project notes.
    df = (
        spark.read
        .option("mergeSchema", "true")
        .parquet(*paths)
    )
    return df

def standardize_columns(df):
    # Column names have varied historically (VendorID vs vendor_id, etc).
    # Normalize to a consistent lowercase snake_case set for the columns we need.
    rename_map = {
        "tpep_pickup_datetime": "pickup_ts",
        "tpep_dropoff_datetime": "dropoff_ts",
        "PULocationID": "pu_location_id",
        "DOLocationID": "do_location_id",
        "trip_distance": "trip_distance",
        "fare_amount": "fare_amount",
        "passenger_count": "passenger_count",
    }
    for old, new in rename_map.items():
        if old in df.columns and old != new:
            df = df.withColumnRenamed(old, new)

    required = ["pickup_ts", "dropoff_ts", "pu_location_id",
                "do_location_id", "trip_distance", "fare_amount"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Input is missing expected columns after standardization: {missing}")

    return df

def clean_and_engineer(df):
    # Note: Spark's ANSI mode (default from Spark 4.x / emr-spark-8.0.0 onward)
    # disallows a direct TIMESTAMP -> BIGINT cast. Use unix_timestamp() instead,
    # which is the explicit, supported way to get epoch seconds from a timestamp.
    df = df.withColumn(
        "trip_duration_min",
        (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0
    )

    # Drop clearly malformed records:
    #  - non-positive or absurd durations (data entry errors, clock issues)
    #  - negative distance/fare
    #  - trips longer than 6 hours (almost certainly bad data, not a real fare trip)
    df = df.filter(
        (F.col("trip_duration_min") > 0) &
        (F.col("trip_duration_min") < 360) &
        (F.col("trip_distance") >= 0) &
        (F.col("fare_amount") >= 0)
    )

    # avg_speed_mph — guard against divide-by-zero for very short/zero-distance trips
    df = df.withColumn(
        "avg_speed_mph",
        F.when(F.col("trip_duration_min") > 0,
               F.col("trip_distance") / (F.col("trip_duration_min") / 60.0))
        .otherwise(F.lit(None).cast(DoubleType()))
    )

    # Anomaly flag: unrealistic speed (e.g. > 80 mph average in NYC traffic)
    # or fare/distance ratio that's way outside normal range.
    df = df.withColumn(
        "is_anomalous_speed",
        F.when(F.col("avg_speed_mph") > 80, F.lit(True)).otherwise(F.lit(False))
    )

    df = df.withColumn("pickup_date", F.to_date("pickup_ts"))
    df = df.withColumn("pickup_hour", F.hour("pickup_ts"))

    return df

def aggregate(df):
    agg = (
        df.groupBy("pickup_date", "pickup_hour", "pu_location_id")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("trip_distance").alias("avg_distance_miles"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.avg("avg_speed_mph").alias("avg_speed_mph"),
            F.sum(F.col("is_anomalous_speed").cast("int")).alias("anomalous_trip_count"),
        )
    )
    return agg

def main():
    if len(sys.argv) != 3:
        print("Usage: day2_transform.py <input_paths_comma_separated> <output_s3_path>")
        sys.exit(1)

    input_paths, output_path = sys.argv[1], sys.argv[2]

    spark = build_spark()

    raw_count = None
    try:
        df = read_source(spark, input_paths)
        raw_count = df.count()

        df = standardize_columns(df)
        df = clean_and_engineer(df)
        clean_count = df.count()

        agg = aggregate(df)

        (
            agg.write
            .mode("overwrite")
            .partitionBy("pickup_date")
            .parquet(output_path)
        )

        print(f"Raw records read: {raw_count}")
        print(f"Clean records after filtering: {clean_count}")
        print(f"Dropped as malformed: {raw_count - clean_count}")
        print(f"Aggregated output written to: {output_path}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
