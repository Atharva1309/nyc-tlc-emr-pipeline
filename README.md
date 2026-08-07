# NYC TLC Trip Data Pipeline — EMR Serverless + PySpark

A batch data pipeline built solo over a weekend to process NYC Taxi & Limousine Commission (TLC) trip data using AWS EMR Serverless and PySpark, with results cataloged in AWS Glue and queried via Amazon Athena.

## Architecture

```
NYC TLC (CloudFront) → S3 (raw) → EMR Serverless (PySpark) → S3 (partitioned Parquet)
                                                                      ↓
                                                              AWS Glue Crawler
                                                                      ↓
                                                              Amazon Athena (SQL)
```

- **Compute:** EMR Serverless — chosen over a persistent cluster to avoid idle compute cost; billed per-second with no capacity planning required for this workload
- **Storage:** Amazon S3, output written as Parquet partitioned by `pickup_date`
- **Cataloging:** AWS Glue Crawler auto-infers schema and registers the output as a queryable table
- **Query layer:** Amazon Athena, run directly against the partitioned output

### Why EMR Serverless + Glue/Athena over a persistent Airflow-orchestrated setup
This was a solo, weekend-scoped batch job with no recurring schedule requirement,
so I optimized for zero ops overhead: EMR Serverless removes cluster sizing/idle-cost
decisions, and Glue Crawler + Athena gives a queryable output with no separate
orchestration layer to maintain. For a recurring production pipeline I'd add Airflow
for scheduling/retries and consider Delta Lake if downstream consumers needed ACID
guarantees on incremental updates.

## What the pipeline does

1. **Ingest** — pulls monthly NYC yellow taxi trip records (Parquet) from NYC TLC's public distribution
2. **Schema handling** — merges schema across files and standardizes column names, since TLC's schema and naming conventions have drifted across years (e.g. a `cbd_congestion_fee` column added in Jan 2025)
3. **Cleaning** — filters out malformed records: non-positive/absurd trip durations, negative distance or fare values
4. **Feature engineering** — computes trip duration, average speed, and a basic speed-based anomaly flag
5. **Aggregation** — groups by pickup date, hour, and pickup zone
6. **Output** — writes partitioned Parquet, designed so downstream queries only scan relevant date partitions

## Results

Run against 3 months of data (Jan–Mar 2024):

| Metric | Value |
|---|---|
| Raw records read | 9,554,778 |
| Clean records after filtering | 9,410,003 |
| Dropped as malformed | 144,775 (~1.5%) |
| Output partitions | 97 (by `pickup_date`) |
| Sample Athena query runtime | 1.378 sec, 5.76 MB scanned |

The low data-scan volume on the sample query confirms the date-partitioning strategy works as intended — queries don't need to scan the full dataset.

## Real issues hit & resolved

- **Broken documented data source:** the commonly-referenced `s3://nyc-tlc` public bucket path returns `AccessDenied` (has been inaccessible since ~2022 despite still being referenced as canonical). Resolved by sourcing from NYC TLC's actual current CloudFront distribution instead.
- **Spark ANSI-mode cast failure:** a direct `TIMESTAMP → BIGINT` cast (previously valid) fails under Spark's ANSI SQL mode, the default from Spark 4.x onward. Resolved using `unix_timestamp()` instead of an implicit cast.

## Known limitation

A handful of output partitions contain clearly invalid pickup dates (e.g. years 2002, 2008) that pass the duration/fare validation because those checks validate the trip itself, not the calendar date. Explicit date-range validation would be the next improvement.

## Files

- `day1_smoke_test.py` — minimal read/count/write job used to validate the EMR Serverless + S3 plumbing before building real logic
- `day2_transform.py` — the full transformation: schema handling, cleaning, feature engineering, aggregation, and partitioned write

## Tech stack

AWS EMR Serverless · PySpark · Amazon S3 · AWS Glue · Amazon Athena
