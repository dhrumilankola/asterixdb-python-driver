# utils/db.py
import streamlit as st
import pandas as pd
import sys
import time
sys.path.append('../..')
from src.pyasterix.connection import Connection

@st.cache_resource
def connect_to_asterixdb():
    try:
        conn = Connection(base_url="http://localhost:19002")
        return conn
    except Exception as e:
        st.error(f"Failed to connect to AsterixDB: {str(e)}")
        return None

def normalize_results(cursor, results):
    if results is None:
        return []
    if isinstance(results, dict):
        return [results]
    return results

@st.cache_data(show_spinner=False)
def execute_query_sync(_conn, query):
    try:
        cursor = _conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        results = normalize_results(cursor, results)
        if cursor.description is not None and not (results and isinstance(results[0], dict)):
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(results, columns=columns)
        else:
            return pd.DataFrame(results)
    except Exception as e:
        st.error(f"Query execution failed: {str(e)}")
        return pd.DataFrame()

def execute_query_async(_conn, query, max_attempts=10, poll_interval=1, _progress_callback=None):
    try:
        cursor = _conn.cursor()
        # Execute query in async mode
        cursor.execute(query, mode="async")
        attempt = 1
        results = None
        while attempt <= max_attempts:
            # Update progress based solely on attempt count
            if _progress_callback:
                _progress_callback(attempt, max_attempts)
            status_result = cursor._get_query_status(cursor.results['handle'])
            if status_result.get("status") == "success":
                results = cursor._get_query_result(status_result['handle'])
                if isinstance(results, dict) and "results" in results:
                    results = results["results"]
                if _progress_callback:
                    _progress_callback(max_attempts, max_attempts)
                break
            elif status_result.get("status") in ("FAILED", "FATAL"):
                raise Exception(f"Async query failed with status: {status_result.get('status')}")
            attempt += 1
            time.sleep(poll_interval)
        if results is None:
            raise Exception("Async query did not complete within maximum attempts.")
        results = normalize_results(cursor, results)
        if cursor.description is not None and not (results and isinstance(results[0], dict)):
            columns = [desc[0] for desc in cursor.description]
            return pd.DataFrame(results, columns=columns)
        else:
            return pd.DataFrame(results)
    except Exception as e:
        st.error(f"Query execution failed: {str(e)}")
        return pd.DataFrame()
