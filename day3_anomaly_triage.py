"""
day3_anomaly_triage.py

Closes the known limitation documented in the README:
  "A handful of output partitions contain clearly invalid pickup dates
  (e.g. years 2002, 2008) that pass the duration/fare validation because
  those checks validate the trip itself, not the calendar date."

IMPORTANT: day2_aggregates is pre-aggregated data (grouped by
pickup_date / pickup_hour / pu_location_id), not per-trip records — so
this script triages anomalous AGGREGATE ROWS (e.g. an hour/zone bucket
with an implausible avg_fare_amount, or a nonsensical pickup_date), not
individual trips. day2_transform.py already computes anomalous_trip_count
per bucket; this script adds a second pass that pulls buckets flagged as
statistical outliers and uses an LLM to make a structured judgment call
on each — is this a data entry error, a legitimate rare edge case, or a
batch/system glitch? — with a short reason attached. This is deliberately
NOT "run the LLM on every row"; it's a targeted triage step for the small
number of aggregate rows that already look suspicious after the
statistical pre-filter.

Usage:
    python day3_anomaly_triage.py \
        --input s3://your-bucket/output/day2_aggregates/ \
        --output s3://your-bucket/output/anomaly_triage/ \
        --sample-size 200

Requires:
    pip install anthropic pandas pyarrow boto3
    export ANTHROPIC_API_KEY=...
"""

import argparse
import json
import os
import re
from datetime import datetime

import pandas as pd
import anthropic

# NYC TLC has published data since 2009; anything outside a generous
# window is almost certainly a data entry error, but we still let the
# model reason about it rather than hard-coding a cutoff, since the point
# of this step is judgment on ambiguous cases, not another hard filter.
VALID_DATE_FLOOR = datetime(2009, 1, 1)
VALID_DATE_CEILING = datetime.now()

SYSTEM_PROMPT = """You are a data quality analyst reviewing flagged rows from \
an aggregated NYC taxi table (day2_aggregates). Each row summarizes trips for \
one pickup_date / pickup_hour / pu_location_id bucket. Each row already \
passed basic validation, but has been flagged for a statistically unusual \
pickup_date, avg_fare_amount, avg_duration_min, or anomalous_trip_count \
relative to trip_count. Your job is to classify each one.

Classify each row into exactly one of:
- "likely_data_entry_error": e.g. a pickup_date far outside the valid TLC \
publication range (2009-present), or an avg_fare_amount/avg_duration_min \
that is almost certainly a typo/system default (e.g. exactly 0, or an \
implausible outlier by orders of magnitude vs. the rest of the bucket's stats).
- "legitimate_edge_case": e.g. a bucket with genuinely high avg_fare_amount \
or avg_duration_min that is plausible (airport zone, holiday surge pricing, \
low trip_count making the average noisy but not wrong).
- "needs_manual_review": genuinely ambiguous — you cannot confidently place \
it in either bucket above.

Return ONLY a JSON array, one object per input row, in the same order, \
with fields: "row_id", "classification", "reason" (one sentence, specific \
to that row's actual values, not generic)."""


def build_batch_prompt(records: list[dict]) -> str:
    """Serialize a batch of flagged aggregate rows for the model."""
    lines = []
    for r in records:
        lines.append(
            f"row_id={r['row_id']} | pickup_date={r['pickup_date']} | "
            f"pickup_hour={r.get('pickup_hour', 'n/a')} | "
            f"pu_location_id={r.get('pu_location_id', 'n/a')} | "
            f"trip_count={r.get('trip_count', 'n/a')} | "
            f"avg_fare_amount={r['avg_fare_amount']} | "
            f"avg_duration_min={r['avg_duration_min']} | "
            f"anomalous_trip_count={r.get('anomalous_trip_count', 'n/a')}"
        )
    return (
        "Classify the following flagged aggregate rows:\n\n"
        + "\n".join(lines)
        + "\n\nReturn the JSON array now."
    )


def triage_batch(client: anthropic.Anthropic, records: list[dict]) -> list[dict]:
    """Send one batch to the model and parse the structured response."""
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": build_batch_prompt(records)}],
    )
    raw_text = "".join(
        block.text for block in response.content if block.type == "text"
    )
    # Model is instructed to return only JSON, but strip code fences
    # defensively in case it wraps the array anyway.
    cleaned = raw_text.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
    return json.loads(cleaned)


def flag_candidates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select the subset of aggregate rows worth sending to the model: this
    is a cheap statistical pre-filter so we're not paying for LLM calls
    on the ~99% of rows that are unambiguously fine.
    """
    # Partitioned datasets (folders like pickup_date=2024-03-31/) get read
    # back with the partition column as a pandas Categorical, which can't
    # be compared with < / > directly — cast to plain string first.
    df["pickup_date_parsed"] = pd.to_datetime(
        df["pickup_date"].astype(str), errors="coerce"
    )

    date_out_of_range = (
        (df["pickup_date_parsed"] < VALID_DATE_FLOOR)
        | (df["pickup_date_parsed"] > VALID_DATE_CEILING)
        | df["pickup_date_parsed"].isna()
    )

    fare_p01, fare_p99 = df["avg_fare_amount"].quantile([0.01, 0.99])
    fare_outlier = (
        (df["avg_fare_amount"] < fare_p01) | (df["avg_fare_amount"] > fare_p99)
    )

    dur_p01, dur_p99 = df["avg_duration_min"].quantile([0.01, 0.99])
    duration_outlier = (
        (df["avg_duration_min"] < dur_p01) | (df["avg_duration_min"] > dur_p99)
    )

    # Buckets where a large share of trips were already flagged anomalous
    # by day2_transform.py are also worth a second look.
    high_anomaly_share = (
        df["trip_count"] > 0
    ) & (
        (df["anomalous_trip_count"] / df["trip_count"]) > 0.5
    )

    return df[date_out_of_range | fare_outlier | duration_outlier | high_anomaly_share].copy()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to partitioned Parquet output from day2_transform.py")
    parser.add_argument("--output", required=True, help="Where to write triage results (CSV or Parquet)")
    parser.add_argument("--sample-size", type=int, default=200, help="Max flagged records to send to the LLM (cost control)")
    parser.add_argument("--batch-size", type=int, default=20, help="Records per API call")
    args = parser.parse_args()

    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    df = pd.read_parquet(args.input)

    # If --input points directly at a single partition folder (e.g.
    # .../pickup_date=2002-12-31/), pyarrow doesn't reconstruct the
    # partition column from the path — it only does that when the
    # partition folder is a subdirectory under a recognized dataset root.
    # Recover it from the path directly in that case.
    if "pickup_date" not in df.columns:
        match = re.search(r"pickup_date=([\d-]+)", args.input)
        if match:
            df["pickup_date"] = match.group(1)
            print(f"Recovered pickup_date={match.group(1)} from --input path (not present as a column).")
        else:
            raise ValueError(
                "Input has no 'pickup_date' column and none could be inferred "
                "from --input path. Point --input at the day2-aggregates root "
                "(so pyarrow can reconstruct partitions) or a pickup_date=... folder."
            )

    df["row_id"] = df.index.astype(str)

    candidates = flag_candidates(df)
    if len(candidates) > args.sample_size:
        candidates = candidates.sample(args.sample_size, random_state=42)

    print(f"Flagged {len(candidates)} of {len(df)} records for LLM triage.")

    results = []
    records = candidates.to_dict("records")
    for i in range(0, len(records), args.batch_size):
        batch = records[i : i + args.batch_size]
        batch_results = triage_batch(client, batch)
        results.extend(batch_results)
        print(f"Triaged {min(i + args.batch_size, len(records))}/{len(records)}")

    results_df = pd.DataFrame(results)
    # The LLM's JSON response parses bare numeric row_ids as int64, while
    # our own row_id column is a string — force both to match before
    # merging, or pandas raises rather than silently coercing.
    results_df["row_id"] = results_df["row_id"].astype(str)
    merged = candidates.merge(results_df, on="row_id", how="left")

    if args.output.endswith(".csv"):
        merged.to_csv(args.output, index=False)
    else:
        merged.to_parquet(args.output, index=False)

    summary = merged["classification"].value_counts()
    print("\nTriage summary:")
    print(summary.to_string())


if __name__ == "__main__":
    main()
