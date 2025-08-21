# modules/query_lab.py
import streamlit as st
import pandas as pd
import plotly.express as px
from utils.db import execute_query_sync

def run(conn):
    st.markdown("<h2>SQL++ Query Lab</h2>", unsafe_allow_html=True)
    st.markdown("Experiment with SQL++ queries directly in this interactive lab.")

    # Example queries
    example_queries = {
        "Simple Business Query": """
            USE YelpDataverse;
            SELECT b.business_id, b.name, b.city, b.state, b.stars, b.review_count
            FROM Businesses b
            WHERE b.stars >= 4.5
            ORDER BY b.stars DESC, b.review_count DESC
            LIMIT 10;
        """,
        "Join Businesses and Reviews": """
            USE YelpDataverse;
            SELECT b.name AS business_name, b.city, b.state, r.stars AS review_stars, 
                   r.text AS review_text, r.date AS review_date
            FROM Businesses b, Reviews r
            WHERE r.business_id = b.business_id
                  AND b.stars >= 4.0
                  AND r.stars >= 4.0
            LIMIT 10;
        """
    }

    selected_example = st.selectbox("Load example query", [""] + list(example_queries.keys()))
    if selected_example:
        query = st.text_area("SQL++ Query", value=example_queries[selected_example], height=250)
    else:
        query = st.text_area("SQL++ Query", 
            "-- Enter your SQL++ query here\n\nUSE YelpDataverse;\nSELECT * FROM Businesses b LIMIT 10;", height=250)

    display_mode = st.radio("Display Mode", ["Table", "JSON", "Raw"])
    limit_results = st.checkbox("Limit Results Size", value=True)
    max_rows = st.number_input("Max Rows", min_value=1, max_value=1000, value=100) if limit_results else None

    if st.button("Run Query"):
        with st.spinner("Executing query..."):
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                if cursor.description:
                    df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
                else:
                    df = pd.DataFrame(results)
                if limit_results and len(df) > max_rows:
                    st.warning(f"Results limited to {max_rows} rows out of {len(df)} total rows.")
                    df = df.head(max_rows)
                if not df.empty:
                    st.markdown("### Query Results")
                    if display_mode == "Table":
                        st.dataframe(df)
                    elif display_mode == "JSON":
                        st.json(df.to_dict(orient="records"))
                    else:
                        st.code(df.to_string())
                    csv = df.to_csv(index=False)
                    st.download_button("Download CSV", csv, "query_results.csv", "text/csv")
                else:
                    st.info("Query returned no results.")
            except Exception as e:
                st.error(f"Error executing query: {e}")
