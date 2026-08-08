"""
nl_to_sql.py

Natural-language query layer over the Athena table this pipeline already
produces (via the Glue Crawler in day2_transform.py's output). Ask a
question in plain English, get back a pandas DataFrame.

This deliberately does NOT let the model write arbitrary SQL against
arbitrary tables — it's scoped to one known table/schema, the generated
SQL is validated as read-only before execution, and query results are
capped, since this is a demo/analysis layer, not a general text-to-SQL
service.

Usage:
    python nl_to_sql.py "What were the busiest pickup zones in February 2024?"

Requires:
    pip install anthropic boto3
    export ANTHROPIC_API_KEY=...
    AWS credentials configured (for Athena + S3 access)
"""

import argparse
import os
import re
import sys
import time

import boto3
import anthropic

# --- Fill these in for your environment ---
ATHENA_DATABASE = "tlc_project"
ATHENA_TABLE = "day2_aggregates"
ATHENA_OUTPUT_S3 = "s3://atharva-emr-tlc-output/athena-results/"
AWS_REGION = "us-east-1"
# --------------------------------------------

# Keep this in sync with the actual Glue-crawled schema. Giving the model
# the real column names/types up front is what makes this reliable —
# without it, it guesses at column names and the query fails.
TABLE_SCHEMA = """
Table: day2_aggregates
Columns:
  pickup_date            (string, format 'YYYY-MM-DD')   -- partition column
  pickup_hour             (int)
  pu_location_id           (int)     -- NYC TLC pickup zone location ID, not a zone name
  trip_count               (bigint)
  avg_duration_min          (double)
  avg_distance_miles         (double)
  avg_fare_amount            (double)
  avg_speed_mph               (double)
  anomalous_trip_count         (bigint)  -- count of anomalous trips in this aggregate group
"""

SYSTEM_PROMPT = f"""You translate natural-language analytics questions into \
a single Amazon Athena (Presto/Trino SQL dialect) SELECT query against this \
table:

{TABLE_SCHEMA}

Rules:
- Output ONLY the SQL query, no explanation, no markdown code fences.
- SELECT statements only. Never generate INSERT, UPDATE, DELETE, DROP, \
ALTER, CREATE, or any DDL/DML.
- Always filter or aggregate using the pickup_date partition column when \
the question implies a date range, to keep the query cheap to scan.
- Always include a LIMIT clause (default 100 if the question doesn't imply \
a specific row count).
- If the question can't be answered from this schema, output exactly: \
NOT_ANSWERABLE"""


def generate_sql(client: anthropic.Anthropic, question: str) -> str:
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=500,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": question}],
    )
    sql = "".join(b.text for b in response.content if b.type == "text").strip()
    return sql.removeprefix("```sql").removeprefix("```").removesuffix("```").strip()


def is_safe_select(sql: str) -> bool:
    """Defense in depth: only ever allow a single read-only SELECT, even
    though the prompt already constrains the model to this."""
    normalized = sql.strip().rstrip(";").upper()
    if not normalized.startswith("SELECT"):
        return False
    forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", ";"]
    # allow exactly one statement: no semicolon-separated stacking
    return not any(re.search(rf"\b{kw}\b", normalized) for kw in forbidden[:-1]) and ";" not in sql.rstrip(";")


def run_athena_query(sql: str) -> list[dict]:
    athena = boto3.client("athena", region_name=AWS_REGION)

    exec_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT_S3},
    )["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=exec_id)
        state = status["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        reason = status["QueryExecution"]["Status"].get("StateChangeReason", "unknown error")
        raise RuntimeError(f"Athena query {state}: {reason}")

    result = athena.get_query_results(QueryExecutionId=exec_id)
    columns = [c["Label"] for c in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = []
    # first row is the header row Athena echoes back
    for row in result["ResultSet"]["Rows"][1:]:
        values = [field.get("VarCharValue") for field in row["Data"]]
        rows.append(dict(zip(columns, values)))
    return rows


def ask(question: str) -> list[dict]:
    client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

    sql = generate_sql(client, question)
    if sql == "NOT_ANSWERABLE":
        raise ValueError("This question can't be answered from the day2_aggregates table's schema.")

    print(f"Generated SQL:\n{sql}\n")

    if not is_safe_select(sql):
        raise ValueError(f"Refusing to run generated query — failed safety check:\n{sql}")

    return run_athena_query(sql)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("question", help="Natural language question, e.g. 'busiest pickup zones in February 2024'")
    args = parser.parse_args()

    try:
        rows = ask(args.question)
    except (ValueError, RuntimeError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)

    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
