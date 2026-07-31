"""
Day 1 smoke test — proves the EMR Serverless + PySpark plumbing works
before writing real transformation logic.

Usage:
  spark-submit day1_smoke_test.py <input_s3_path> <output_s3_path>

What this does:
  1. Reads one month of TLC yellow taxi parquet data
  2. Counts the rows
  3. Writes the raw data straight back out to S3 (proves read + write both work)
"""

import sys
from pyspark.sql import SparkSession

def main():
    if len(sys.argv) != 3:
        print("Usage: day1_smoke_test.py <input_s3_path> <output_s3_path>")
        sys.exit(1)

    input_path, output_path = sys.argv[1], sys.argv[2]

    spark = SparkSession.builder.appName("tlc-day1-smoke-test").getOrCreate()

    try:
        df = spark.read.parquet(input_path)

        row_count = df.count()
        col_count = len(df.columns)

        print(f"Smoke test: read {row_count} rows, {col_count} columns from {input_path}")
        print(f"Columns: {df.columns}")

        df.write.mode("overwrite").parquet(output_path)

        print(f"Smoke test: wrote output to {output_path}")
        print("Day 1 smoke test PASSED")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
