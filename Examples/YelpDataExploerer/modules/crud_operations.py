# modules/crud_operations.py
import streamlit as st
import json
import time
import pandas as pd
from utils.db import execute_query_sync, execute_query_async

def run(conn):
    st.markdown("<h2>CRUD Operations Demo</h2>", unsafe_allow_html=True)
    st.markdown("Demonstrate Create, Read, and Delete operations using PyAsterix.")

    operation = st.radio("Select Operation", ["Create (Insert)", "Read (Query)", "Delete"])

    if operation == "Create (Insert)":
        st.markdown("#### Insert New Business")
        with st.form("insert_form"):
            business_id = st.text_input("Business ID (required)", value=f"new_business_{int(time.time())}")
            name = st.text_input("Name (required)", "New Test Business")
            address = st.text_input("Address", "123 Test Street")
            city = st.text_input("City", "Test City")
            state = st.text_input("State", "TS")
            postal_code = st.text_input("Postal Code", "12345")
            latitude = st.number_input("Latitude", value=37.7749)
            longitude = st.number_input("Longitude", value=-122.4194)
            stars = st.slider("Stars", 1.0, 5.0, 4.0, 0.5)
            review_count = st.number_input("Review Count", value=0)
            categories_input = st.text_input("Categories (comma separated)", "Test, Demo, PyAsterixDB")
            categories = [cat.strip() for cat in categories_input.split(",")]
            hours = {
                "Monday": "9:00-17:00",
                "Tuesday": "9:00-17:00",
                "Wednesday": "9:00-17:00",
                "Thursday": "9:00-17:00",
                "Friday": "9:00-17:00",
                "Saturday": "10:00-15:00",
                "Sunday": "Closed"
            }
            submitted = st.form_submit_button("Insert Business")
            if submitted:
                try:
                    business_data = {
                        "business_id": business_id,
                        "name": name,
                        "address": address,
                        "city": city,
                        "state": state,
                        "postal_code": postal_code,
                        "latitude": latitude,
                        "longitude": longitude,
                        "stars": stars,
                        "review_count": review_count,
                        "is_open": 1,
                        "categories": categories,
                        "hours": hours
                    }
                    business_json = json.dumps(business_data)
                    insert_query = f"""
                        USE YelpDataverse;
                        INSERT INTO Businesses([{business_json}]);
                    """
                    cursor = conn.cursor()
                    cursor.execute(insert_query)
                    st.success(f"Successfully inserted business with ID: {business_id}")
                    st.json(business_data)
                except Exception as e:
                    st.error(f"Error inserting business: {e}")

    elif operation == "Read (Query)":
        st.markdown("#### Query Builder")
        dataset = st.selectbox("Select dataset", ["Businesses", "Reviews", "Users", "Tips"])
        field_options = {
            "Businesses": ["business_id", "name", "address", "city", "state", "postal_code", "stars", "review_count", "categories"],
            "Reviews": ["review_id", "user_id", "business_id", "stars", "date", "text"],
            "Users": ["user_id", "name", "review_count", "yelping_since", "fans", "average_stars"],
            "Tips": ["user_id", "business_id", "text", "date", "compliment_count"]
        }
        selected_fields = st.multiselect("Select fields to return",
                                         options=field_options[dataset],
                                         default=field_options[dataset][:3])
        st.markdown("##### Add filter conditions")
        filter_col1, filter_col2, filter_col3 = st.columns([2, 1, 2])
        with filter_col1:
            filter_field = st.selectbox("Field", field_options[dataset])
        with filter_col2:
            filter_operator = st.selectbox("Operator", ["=", ">", "<", ">=", "<=", "!=", "LIKE", "IN", "CONTAINS"])
        with filter_col3:
            filter_value = st.text_input("Value")
        where_clause = ""
        if filter_field and filter_operator and filter_value:
            if filter_operator == "LIKE":
                where_clause = f"WHERE b.{filter_field} LIKE '%{filter_value}%'"
            elif filter_operator == "IN":
                values = [v.strip() for v in filter_value.split(",")]
                values_str = "', '".join(values)
                where_clause = f"WHERE b.{filter_field} IN ['{values_str}']"
            elif filter_operator == "CONTAINS":
                where_clause = f"WHERE CONTAINS(b.{filter_field}, '{filter_value}')"
            else:
                where_clause = f"WHERE b.{filter_field} {filter_operator} '{filter_value}'"
        st.markdown("##### Sort options")
        order_col1, order_col2 = st.columns([2, 1])
        with order_col1:
            order_by_field = st.selectbox("Order by", ["None"] + field_options[dataset])
        with order_col2:
            order_direction = st.selectbox("Direction", ["ASC", "DESC"])
        order_clause = ""
        if order_by_field != "None":
            order_clause = f"ORDER BY b.{order_by_field} {order_direction}"
        limit = st.number_input("Limit results (ignored in async mode)", min_value=1, max_value=1000, value=10)
        fields_str = ", ".join([f"b.{field}" for field in selected_fields]) if selected_fields else "*"

        # Option to select query execution mode: Synchronous (default) or Asynchronous
        query_mode = st.radio("Query Execution Mode", ["Synchronous", "Asynchronous"], index=0)
        mode = "sync" if query_mode == "Synchronous" else "async"

        # Build the query: if async, remove the LIMIT clause to fetch all data.
        if mode == "sync":
            query = f"""
                USE YelpDataverse;
                SELECT {fields_str}
                FROM {dataset} b
                {where_clause}
                {order_clause}
                LIMIT {limit};
            """
        else:
            query = f"""
                USE YelpDataverse;
                SELECT {fields_str}
                FROM {dataset} b
                {where_clause}
                {order_clause};
            """

        st.markdown("##### Generated Query")
        st.code(query, language="sql")

        if st.button("Execute Query"):
            with st.spinner("Executing query..."):
                try:
                    if mode == "sync":
                        result = execute_query_sync(conn, query)
                    else:
                        # Create progress UI elements
                        progress_bar = st.progress(0)
                        progress_text = st.empty()
                        def update_progress(fetched, total):
                            if total:
                                progress = min(fetched / total, 1.0)
                                progress_bar.progress(progress)
                                progress_text.text(f"Fetched {fetched} of {total} rows...")
                            else:
                                progress_text.text(f"Fetched {fetched} rows so far...")
                        result = execute_query_async(conn, query, _progress_callback=update_progress)
                    st.markdown("##### Query Results")
                    if not result.empty:
                        st.dataframe(result)
                        csv = result.to_csv(index=False)
                        st.download_button("Download CSV", csv, "query_results.csv", "text/csv")
                    else:
                        st.info("No results found for your query.")
                except Exception as e:
                    st.error(f"Error executing query: {e}")

    elif operation == "Delete":
        st.markdown("#### Delete Business")
        business_id = st.text_input("Enter a business ID to delete")
        if business_id:
            try:
                business_data = execute_query_sync(conn, f"""
                    SELECT VALUE b FROM YelpDataverse.Businesses b
                    WHERE b.business_id = '{business_id}';
                """)
                if not business_data.empty:
                    business = business_data.iloc[0]
                    st.markdown("##### Business to Delete")
                    st.markdown(f"**Name:** {business.get('name', 'N/A')}")
                    st.markdown(f"**Address:** {business.get('address', 'N/A')}, {business.get('city', 'N/A')}, {business.get('state', 'N/A')}")
                    st.markdown(f"**Rating:** {'⭐' * int(business.get('stars', 0))} ({business.get('stars', 0)})")
                    st.warning("⚠️ Are you sure you want to delete this business? This action cannot be undone.")
                    confirm_delete = st.checkbox("I confirm I want to delete this business")
                    if st.button("Delete Business", disabled=not confirm_delete):
                        try:
                            delete_query = f"""
                                USE YelpDataverse;
                                DELETE FROM Businesses b
                                WHERE b.business_id = '{business_id}';
                            """
                            cursor = conn.cursor()
                            cursor.execute(delete_query)
                            st.success(f"Successfully deleted business with ID: {business_id}")
                        except Exception as e:
                            st.error(f"Error deleting business: {e}")
                else:
                    st.warning(f"No business found with ID: {business_id}")
            except Exception as e:
                st.error(f"Error loading business data: {e}")
