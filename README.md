# NYC TLC Trip Data Pipeline — EMR Serverless + PySpark

A batch data pipeline built solo over a weekend to process NYC Taxi & Limousine Commission (TLC) trip data using AWS EMR Serverless and PySpark, with results cataloged in AWS Glue and queried via Amazon Athena plus a natural-language query layer and LLM-assisted data quality triage built on top.

## Architecture

```
NYC TLC (CloudFront) → S3 (raw) → EMR Serverless (PySpark) → S3 (partitioned Parquet)
                                                                      ↓
                                                              AWS Glue Crawler
                                                                      ↓
                                                              Amazon Athena (SQL)
                                                                      ↓
                                                    Claude (nl_to_sql.py / streamlit_app.py)
```

- **Compute:** EMR Serverless — chosen over a persistent cluster to avoid idle compute cost; billed per-second with no capacity planning required for this workload
- **Storage:** Amazon S3, output written as Parquet partitioned by `pickup_date`
- **Cataloging:** AWS Glue Crawler auto-infers schema and registers the output as a queryable table
- **Query layer:** Amazon Athena, run directly against the partitioned output
- **AI layer:** Claude (Anthropic API) for natural-language querying and anomaly triage, see [AI Layer](#ai-layer) below

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

A handful of output partitions contain clearly invalid pickup dates (e.g. years 2002, 2008) that pass the duration/fare validation because those checks validate the trip itself, not the calendar date. Explicit date-range validation would be the next improvement to the core Spark job.

**Update:** this gap is now caught by a secondary LLM-based triage pass rather than the original Spark validation — see [AI Layer](#ai-layer) below. The underlying pipeline still doesn't filter these at write time; the triage script is a detection layer on top, not a fix to `day2_transform.py` itself. Baking the check into the Spark job directly would be the cleaner long-term fix.

## AI Layer

The core pipeline produces a queryable table, but querying it still requires knowing SQL and the exact schema. This layer adds three small, focused pieces on top of that output, each one solves a specific gap rather than being AI added for its own sake.

### 1. Natural-language querying (`nl_to_sql.py`, `streamlit_app.py`)

Translates a plain-English question into Athena SQL, runs it, and returns the result. Scoped deliberately narrow: one known table, one schema passed explicitly to the model, and a hard safety check before anything executes.

**Design choices:**
- The model is given the exact `day2_aggregates` schema up front rather than being allowed to guess column names. This is what makes the generated SQL reliable instead of a coin flip.
- Every generated query is checked to confirm it's a single read-only `SELECT` before it's allowed to run. This isn't just a prompt instruction, it's enforced in code as a second layer, independent of what the model decides to generate.
- Ambiguous or out-of-scope questions return `NOT_ANSWERABLE` rather than a best-effort guess.

**Example queries (all run against the live table):**

| Question | Generated SQL | Result |
|---|---|---|
| "What was the total trip count for pickup_hour 8?" | `SELECT SUM(trip_count) AS total_trip_count FROM day2_aggregates WHERE pickup_hour = 8 LIMIT 1` | `367298` |
| "What was the average fare amount on 2024-01-15?" | `SELECT AVG(avg_fare_amount) AS overall_avg_fare_amount FROM day2_aggregates WHERE pickup_date = '2024-01-15' LIMIT 1` | `$25.23` |
| "Delete all records where anomalous_trip_count is greater than 0" | *(refused)* | `NOT_ANSWERABLE` — no query executed |

That last row is the important one: the model correctly identified a destructive request as out of scope for a read-only interface and declined, rather than generating a `DELETE` statement that the safety check would then have had to catch.

A minimal Streamlit UI (`streamlit_app.py`) wraps the same `ask()` function with a browser interface — text box in, generated SQL and results rendered out. No logic is duplicated between the CLI and the UI.

### 2. Anomaly triage (`day3_anomaly_triage.py`)

Closes the gap described in [Known limitation](#known-limitation) above: `day2_transform.py` validates trip-level fields (duration, fare) but not calendar sanity, so a handful of aggregate rows with clearly invalid pickup dates pass through undetected.

This script adds a second pass:
1. **Statistical pre-filter** (cheap, no LLM calls) — flags rows with out-of-range dates, fare/duration outliers, or a high share of already-anomalous trips within the bucket.
2. **LLM triage** on only the flagged subset — classifies each as `likely_data_entry_error`, `legitimate_edge_case`, or `needs_manual_review`, with a one-sentence reason grounded in that row's actual values.

**Confirmed working on the exact rows it was built for** — pointing the script directly at the `pickup_date=2002-12-31` and `pickup_date=2008-12-31` partitions:

```
pickup_date=2002-12-31 → likely_data_entry_error
"pickup_date of 2002-12-31 is well outside the valid TLC data range
(2009-present), making this almost certainly a data entry or system error
regardless of the otherwise plausible fare and duration values."

pickup_date=2008-12-31 → likely_data_entry_error
"The pickup_date of 2008-12-31 predates the TLC public dataset's valid
range (which begins in 2009), indicating a likely data entry or system
error despite the otherwise plausible fare and duration values."
```

Both classifications correctly separate the *date* signal from the *fare/duration* signal instead of just pattern-matching on "this row looks weird" the model explicitly notes the other values were plausible and the date alone was disqualifying.

### AI layer bugs hit & resolved

Documented here rather than smoothed over, in the same spirit as the [Real issues](#real-issues-hit--resolved) section above:

- **`TYPE_MISMATCH: varchar = date`** — the Glue Crawler inferred `pickup_date` as a string, not a `DATE` type. Fixed by giving the model explicit type information in the schema description instead of assuming Athena's native date type.
- **Missing `glue:GetPartitions` permission** — Athena needs this to read partition metadata on a partitioned table; it's easy to grant `GetTable`/`GetDatabase` and miss this one until a partitioned-column query actually runs.
- **Unordered Categorical comparison error** — pandas/pyarrow reads Hive-partitioned columns (`pickup_date=.../`) back as `Categorical` dtype, which can't be compared with `<`/`>` directly. Fixed by casting to string before parsing as a date.
- **`row_id` merge type mismatch** — the LLM's JSON response parses bare numeric IDs as `int64`, while the source dataframe's IDs are strings. Fixed by forcing both sides to string before merging.
- **Partition column not reconstructed when reading a single partition folder directly** — pyarrow only infers Hive partition columns when reading from a recognized dataset root; pointing it at one partition folder as the root loses that column. Fixed by recovering the value from the input path when the column is absent.

## Productionizing this

Both AI components currently run locally against the deployed AWS resources (Athena, Glue, S3) the AWS-side pipeline is real, the AI layer's compute is the only piece still local. If this were moving toward production, here's the path:

- **`day3_anomaly_triage.py` → AWS Lambda, triggered by an S3 event.** An `ObjectCreated` notification on the `day2-aggregates` prefix would invoke the triage automatically every time `day2_transform.py` writes a new partition, turning triage from something run manually into something that runs itself. The Anthropic API key would move from a local env var to AWS Secrets Manager, read at invocation time.
- **`streamlit_app.py` → containerized on AWS App Runner.** A Dockerfile wrapping the existing app, pushed to Amazon ECR, deployed behind App Runner's managed HTTPS endpoint, no cluster to provision or manage for a single-container UI like this.
- **Both** would move off long-lived local credentials and onto IAM roles scoped to exactly the actions each component needs (the same principle behind the IAM policy already in place for local runs), rather than a single broad user.

Scoped out of this weekend build to stay within time constraints, but the design already anticipates it: neither script assumes a local execution context, and IAM permissions are already broken out by action rather than granted broadly.

## Files

- `day1_smoke_test.py` — minimal read/count/write job used to validate the EMR Serverless + S3 plumbing before building real logic
- `day2_transform.py` — the full transformation: schema handling, cleaning, feature engineering, aggregation, and partitioned write
- `nl_to_sql.py` — natural-language → Athena SQL query layer (CLI)
- `streamlit_app.py` — browser UI wrapper around `nl_to_sql.py`
- `day3_anomaly_triage.py` — LLM-based triage of statistically flagged aggregate rows

## Tech stack

AWS EMR Serverless · PySpark · Amazon S3 · AWS Glue · Amazon Athena · Anthropic Claude API · Streamlit
