"""
streamlit_app.py

Minimal UI wrapper around nl_to_sql.py's ask() function. All the actual
logic (SQL generation, safety check, Athena execution) already lives in
nl_to_sql.py — this file just gives it a demo-able interface instead of
reading terminal output.

Usage:
    streamlit run streamlit_app.py

Requires:
    pip install streamlit
    (plus everything nl_to_sql.py already requires: anthropic, boto3)
    ANTHROPIC_API_KEY set, AWS credentials configured
"""

import streamlit as st
import pandas as pd

from nl_to_sql import ask, generate_sql, is_safe_select
import anthropic
import os

st.set_page_config(page_title="NYC TLC — Ask Your Data", page_icon="🚕")

st.title("🚕 NYC TLC Pipeline — Ask Your Data")
st.caption(
    "Natural-language questions over the day2_aggregates Athena table. "
    "Read-only — the query is validated before it runs."
)

question = st.text_input(
    "Ask a question about the data",
    placeholder="e.g. What was the average fare amount on 2024-01-15?",
)

col1, col2 = st.columns([1, 4])
run_clicked = col1.button("Run", type="primary")

if run_clicked and question.strip():
    with st.spinner("Generating SQL and querying Athena..."):
        try:
            client = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])
            sql = generate_sql(client, question)

            st.subheader("Generated SQL")
            st.code(sql, language="sql")

            if sql == "NOT_ANSWERABLE":
                st.warning(
                    "This question can't be answered from the day2_aggregates "
                    "table's schema."
                )
            elif not is_safe_select(sql):
                st.error(
                    "Generated query failed the read-only safety check and "
                    "was not executed."
                )
            else:
                rows = ask(question)
                st.subheader("Result")
                if rows:
                    st.dataframe(pd.DataFrame(rows), use_container_width=True)
                else:
                    st.info("Query ran successfully but returned no rows.")

        except Exception as e:
            st.error(f"Error: {e}")

elif run_clicked:
    st.warning("Enter a question first.")

with st.expander("Example questions to try"):
    st.markdown(
        """
        - What was the total trip count for pickup_hour 8?
        - What was the average fare amount on 2024-01-15?
        - Delete all records where anomalous_trip_count is greater than 0
          *(try this one to see the safety guardrail reject it)*
        """
    )
