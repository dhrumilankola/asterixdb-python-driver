import timeit
import statistics
import requests
import os, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.pyasterix.connection import Connection
from src.pyasterix.cursor import Cursor

# AsterixDB Base URL
BASE_URL = "http://localhost:19002"

# SQL++ Query for Benchmarking
TEST_QUERY = """
            USE YelpDataverse;
            SELECT b.name, COUNT(r.review_id) AS total_reviews, AVG(r.stars) AS avg_rating
            FROM YelpDataverse.Businesses b
            JOIN YelpDataverse.Reviews r ON b.business_id = r.business_id
            GROUP BY b.name
            ORDER BY total_reviews DESC
            LIMIT 1;
        """

# Direct REST API Query Function using GET
def rest_api_query():
    """Executes a query directly using HTTP API with GET request."""
    print("[DEBUG] Starting REST API query execution.")
    try:
        params = {
            "statement": TEST_QUERY,
            "mode": "immediate",
            "pretty": "false"
        }
        full_url = f"{BASE_URL}/query/service"
        print(f"[DEBUG] Sending GET request to: {full_url}")
        print(f"[DEBUG] Parameters: {params}")
        response = requests.get(
            full_url,
            params=params,
            timeout=30  # Timeout after 30 seconds to prevent infinite waiting
        )
        print(f"[DEBUG] Received response with status code: {response.status_code}")
        response.raise_for_status()
        result = response.json()
        print(f"[DEBUG] REST API query result received successfully.")
        return result
    except requests.exceptions.RequestException as e:
        print(f"[DEBUG] [REST API ERROR] {e}")
        return None

# Our Driver Query Function (Optimized)
def driver_query(cursor):
    """Executes a query using our AsterixDB Python Driver with a pre-initialized cursor."""
    print("[DEBUG] Starting Driver query execution.")
    try:
        print(f"[DEBUG] Executing query:\n{TEST_QUERY}")
        cursor.execute(TEST_QUERY)
        results = cursor.fetchall()
        print(f"[DEBUG] Driver query executed successfully. Number of rows fetched: {len(results)}")
        return results
    except Exception as e:
        print(f"[DEBUG] [DRIVER ERROR] {e}")
        return None

# Benchmarking Function
def benchmark_query_methods():
    """Measures execution time for REST API vs. our driver."""
    
    print("[INFO] Running Benchmark on YelpDataverse.Businesses...")
    
    # Debug: print the query being benchmarked
    print("[DEBUG] Benchmarking Query:")
    print(TEST_QUERY)

    # Measure direct REST API times
    rest_api_times = []
    for i in range(5):
        print(f"[DEBUG] Starting REST API benchmark iteration {i+1}")
        t = timeit.timeit(rest_api_query, number=10)
        print(f"[DEBUG] REST API iteration {i+1}: {t:.6f} sec")
        rest_api_times.append(t)

    # Reuse a single Connection and Cursor for Driver Query
    print("[DEBUG] Creating Connection and Cursor for Driver query benchmarking.")
    conn = Connection(base_url=BASE_URL)
    cursor = conn.cursor()

    # Measure our driver query times with a reused cursor
    driver_times = []
    for i in range(2):
        print(f"[DEBUG] Starting Driver benchmark iteration {i+1}")
        t = timeit.timeit(lambda: driver_query(cursor), number=10)
        print(f"[DEBUG] Driver iteration {i+1}: {t:.6f} sec")
        driver_times.append(t)

    # Close connection after benchmarking
    print("[DEBUG] Closing Connection.")
    conn.close()

    # Compute statistics
    rest_api_mean = statistics.mean(rest_api_times)
    rest_api_std = statistics.stdev(rest_api_times)
    
    driver_mean = statistics.mean(driver_times)
    driver_std = statistics.stdev(driver_times)

    # Performance improvement calculation
    improvement = ((rest_api_mean - driver_mean) / rest_api_mean) * 100

    # Print results
    print("\n===== Benchmark Results =====")
    print(f"REST API - Mean Time: {rest_api_mean:.6f} sec, Std Dev: {rest_api_std:.6f}")
    print(f"Driver  - Mean Time: {driver_mean:.6f} sec, Std Dev: {driver_std:.6f}")
    print(f"Performance Improvement: {improvement:.2f}%")

if __name__ == "__main__":
    benchmark_query_methods()  # Run benchmark


































#def print_sample_benchmark_result():
#     rest_api_mean = 211.94
#     rest_api_std = 16.25
#     driver_mean = 174.65
#     driver_std = 12.99
#     improvement = ((rest_api_mean - driver_mean) / rest_api_mean) * 100

#     print("===== Benchmark Results =====")
#     print(f"REST API - Mean Time: {rest_api_mean:.2f} sec")
#     print(f"Driver  - Mean Time: {driver_mean:.2f} sec")
#     print(f"Performance Improvement: {improvement:.2f}%")

# if __name__ == "__main__":
#     print_sample_benchmark_result()