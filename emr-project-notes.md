# EMR Side Project — Working Notes

Carried over from a prior chat. Use this as the running reference for the project and for building the interview story once it's done.

## Goal
Build a small, real, defensible EMR/PySpark project over a weekend to have a genuine story for interviews (mapped to Amazon LPs, especially **Learn & Be Curious** and **Dive Deep**).

## Starting point
- Time budget: a weekend (2–3 days)
- AWS: no existing account
- Spark: new; strong in Python/SQL already
- Dataset: chosen for me — public, EMR-appropriate, no download step needed

## Dataset & Architecture
**Dataset:** NYC TLC Trip Record Data — public, sits in a public S3 bucket already, large enough (millions of rows/month) that EMR is a legitimate choice rather than overkill.

**Architecture:**
- Source: public NYC TLC S3 bucket (Parquet files)
- Compute: **EMR Serverless** (not a persistent cluster — bills per second, no idle cluster to forget about)
- Transform: PySpark job — cleaning, feature engineering, aggregation
- Target: my own S3 bucket, partitioned Parquet
- Query layer: Athena on top, for a finished, queryable result

## Day-by-day plan

### Day 1 — Setup + first working job
- [x] Create AWS account
- [x] Set a billing budget + alert (Budgets → $5/month, 80% actual-cost alert to my email) — **done, saved**
- [x] Create an S3 output bucket — `atharva-emr-tlc-output` (us-east-1), general purpose, ACLs disabled, all public access blocked — **done**
- [x] Create IAM role for EMR Serverless — used **custom trust policy** (trusting `emr-serverless.amazonaws.com`), not the "AWS service" service-linked-role flow (that one has a fixed policy and no "Add permissions" step). Attached `AmazonS3FullAccess` + `AWSGlueServiceRole`. Role name `emr-serverless-tlc-role` — **done, ARN saved**
- [ ] Create the EMR Serverless application (EMR Studio → Applications → Create → Spark → `tlc-pipeline`) — **blocked**, see below
- [ ] Run the Day 1 smoke test script — pending on the above

**Blocker (as of July 13):** AWS account shows "Complete your account setup" when trying to open EMR Studio — account not fully activated yet (>24h since signup). Filed a Basic Support case: "Unable to open EMR studio" / Account and Billing / Account Activation, severity low, Web communication preference. Waiting on response — resume Day 1's last two steps once account activates.

### Day 2 — Real logic + numbers
- [x] Write the actual transformation script (`day2_transform.py`, drafted ahead of schedule while waiting on account activation) — handles:
  - schema drift across months via `mergeSchema` + column standardization
  - malformed-record filtering (bad duration/distance/fare)
  - trip duration, avg speed, and a basic anomaly flag (avg speed > 80mph)
  - aggregation by pickup date/hour/zone
  - partitioned Parquet write, partitioned by `pickup_date`
- [ ] **Not yet run** — needs the EMR Serverless application to exist first
- [ ] Run across a few months of data to get a real "X million records processed in Y minutes" number
- [ ] Catalog in Glue, query from Athena to prove the output is usable

**How to run once the app exists:**
```bash
aws s3 cp day2_transform.py s3://atharva-emr-tlc-output/scripts/

aws emr-serverless start-job-run \
  --application-id <your-app-id> \
  --execution-role-arn <your-iam-role-arn> \
  --job-driver '{
    "sparkSubmit": {
      "entryPoint": "s3://atharva-emr-tlc-output/scripts/day2_transform.py",
      "entryPointArguments": [
        "s3://nyc-tlc/trip data/yellow_tripdata_2024-01.parquet,s3://nyc-tlc/trip data/yellow_tripdata_2024-02.parquet,s3://nyc-tlc/trip data/yellow_tripdata_2024-03.parquet",
        "s3://atharva-emr-tlc-output/day2-aggregates/"
      ]
    }
  }'
```

### Day 3 / evening — Document it
- [ ] Screenshot: EMR Studio run, S3 output, Athena query result
- [ ] Turn it into a STAR+ story with real numbers

## Learn & Be Curious framing — track these as they happen
Things a Python/SQL background doesn't give you for free. Log the *first real instance* of each — that's the reflection material:
- **Lazy evaluation** — nothing runs until an action (`.write()`, `.count()`, `.show()`); errors can show up several cells after the actual bad line.
- **Partitioning strategy** — how output is partitioned (date/zone) directly determines whether Athena scans 2GB or 200GB. A real design decision, not a default.
- **Distributed debugging** — failures are often layers removed from the bad row (executor OOM, not "line 47 crashed").
- **Schema evolution** — TLC Parquet files have changed columns across years; handling that gracefully is a real "caught it before it broke something" moment (Dive Deep material).

## Status as of this note
S3 bucket, billing alert, and IAM role are done. Blocked on AWS account activation before the EMR Serverless application can be created — support case filed and pending. Day 2 script is written and ready to run the moment the app exists; not yet executed against real data, so no real numbers yet.

## STAR+ story skeleton (fill in once Day 2/3 are done)
- **Situation:** Wanted hands-on EMR/Spark experience beyond Python/SQL for [interview/role context].
- **Task:** Build a real pipeline over a weekend — ingest, transform, and query a non-trivial public dataset using EMR Serverless.
- **Action:** [fill in once built — architecture choices, the specific bug/schema/partitioning decision, how it was debugged]
- **Result:** [fill in real numbers — records processed, runtime, query performance via Athena]
- **Reflection / Learn & Be Curious:** [the specific Spark concept that tripped you up and how you resolved it]
