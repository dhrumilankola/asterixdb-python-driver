import timeit
import statistics
import requests
import os, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

from src.pyasterix import (
    connect, 
    ObservabilityConfig, 
    MetricsConfig, 
    TracingConfig, 
    LoggingConfig,
    initialize_observability
)

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

# Setup observability for benchmarking
def setup_observability():
    """Setup observability configuration for benchmarking."""
    config = ObservabilityConfig(
        metrics=MetricsConfig(
            enabled=True,
            namespace="pyasterix_benchmark",
            prometheus_port=8001  # Different port for benchmark
        ),
        tracing=TracingConfig(
            enabled=True,
            service_name="benchmark_service",
            sample_rate=1.0,  # Sample all traces for benchmarking
            exporter="console"
        ),
        logging=LoggingConfig(
            structured=True,
            level="INFO",
            correlation_enabled=True,
            include_trace_info=True
        )
    )
    
    observability = initialize_observability(config)
    print("✅ Observability initialized for benchmarking")
    return observability

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

# Our Driver Query Function (Optimized with Observability)
def driver_query(cursor, observability=None):
    """Executes a query using our AsterixDB Python Driver with a pre-initialized cursor."""
    print("[DEBUG] Starting Driver query execution with observability.")
    
    # Get logger for structured logging
    logger = observability.get_logger("benchmark.driver") if observability else None
    
    try:
        # Create custom span for benchmark timing
        span_context = observability.start_span(
            "benchmark.driver_query", 
            kind="INTERNAL",
            query_type="benchmark",
            method="driver"
        ) if observability else None
        
        with span_context if span_context else nullcontext():
            if logger:
                logger.info("Starting driver query execution", extra={
                    "query_type": "benchmark",
                    "method": "driver",
                    "query_preview": TEST_QUERY[:100] + "..."
                })
            
            print(f"[DEBUG] Executing query:\n{TEST_QUERY}")
            cursor.execute(TEST_QUERY)
            results = cursor.fetchall()
            
            # Record custom benchmark metric
            if observability:
                observability.record_metric(
                    "benchmark.query_execution",
                    1,
                    {
                        "method": "driver",
                        "status": "success",
                        "result_count": len(results)
                    }
                )
            
            print(f"[DEBUG] Driver query executed successfully. Number of rows fetched: {len(results)}")
            
            if logger:
                logger.info("Driver query completed successfully", extra={
                    "result_count": len(results),
                    "method": "driver"
                })
            
            return results
            
    except Exception as e:
        # Record error metric
        if observability:
            observability.record_metric(
                "benchmark.query_execution",
                1,
                {
                    "method": "driver",
                    "status": "error",
                    "error_type": type(e).__name__
                }
            )
            
        if logger:
            logger.error("Driver query failed", exc_info=True, extra={
                "method": "driver",
                "error_type": type(e).__name__
            })
        
        print(f"[DEBUG] [DRIVER ERROR] {e}")
        return None

# Context manager for when observability is None
from contextlib import nullcontext

# Enhanced Benchmarking Function with Observability
def benchmark_query_methods():
    """Measures execution time for REST API vs. our driver with comprehensive observability."""
    
    # Initialize observability
    observability = setup_observability()
    logger = observability.get_logger("benchmark.main")
    
    print("[INFO] Running Enhanced Benchmark with Observability on YelpDataverse.Businesses...")
    
    # Start overall benchmark span
    with observability.start_span("benchmark.overall", kind="INTERNAL", 
                                 benchmark_type="performance_comparison") as span:
        
        logger.info("Starting benchmark execution", extra={
            "benchmark_type": "performance_comparison",
            "query_preview": TEST_QUERY[:100] + "..."
        })
        
        # Debug: print the query being benchmarked
        print("[DEBUG] Benchmarking Query:")
        print(TEST_QUERY)

        # Measure direct REST API times with observability
        print("\n📊 Phase 1: REST API Benchmark")
        rest_api_times = []
        
        with observability.start_span("benchmark.rest_api_phase", kind="INTERNAL") as api_span:
            for i in range(5):
                print(f"[DEBUG] Starting REST API benchmark iteration {i+1}")
                
                # Create span for each iteration
                with observability.start_span(f"benchmark.rest_api_iteration_{i+1}", kind="CLIENT") as iter_span:
                    iter_span.set_attribute("iteration", i+1)
                    iter_span.set_attribute("method", "rest_api")
                    
                    t = timeit.timeit(rest_api_query, number=10)
                    print(f"[DEBUG] REST API iteration {i+1}: {t:.6f} sec")
                    rest_api_times.append(t)
                    
                    # Record iteration metric
                    observability.record_metric(
                        "benchmark.iteration_time",
                        t,
                        {
                            "method": "rest_api",
                            "iteration": i+1,
                            "phase": "measurement"
                        }
                    )

        # Setup driver connection with observability
        print("\n📊 Phase 2: Driver Benchmark")
        print("[DEBUG] Creating Connection and Cursor for Driver query benchmarking.")
        
        # Create connection with observability enabled
        conn = connect(
            host="localhost",
            port=19002,
            observability_config=observability.config
        )
        cursor = conn.cursor()

        # Measure our driver query times with observability
        driver_times = []
        
        with observability.start_span("benchmark.driver_phase", kind="INTERNAL") as driver_span:
            for i in range(2):
                print(f"[DEBUG] Starting Driver benchmark iteration {i+1}")
                
                # Create span for each iteration
                with observability.start_span(f"benchmark.driver_iteration_{i+1}", kind="INTERNAL") as iter_span:
                    iter_span.set_attribute("iteration", i+1)
                    iter_span.set_attribute("method", "driver")
                    
                    t = timeit.timeit(lambda: driver_query(cursor, observability), number=10)
                    print(f"[DEBUG] Driver iteration {i+1}: {t:.6f} sec")
                    driver_times.append(t)
                    
                    # Record iteration metric
                    observability.record_metric(
                        "benchmark.iteration_time",
                        t,
                        {
                            "method": "driver",
                            "iteration": i+1,
                            "phase": "measurement"
                        }
                    )

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

        # Record final benchmark metrics
        observability.record_metric(
            "benchmark.final_results",
            rest_api_mean,
            {
                "method": "rest_api",
                "metric": "mean_time",
                "unit": "seconds"
            }
        )
        
        observability.record_metric(
            "benchmark.final_results",
            driver_mean,
            {
                "method": "driver",
                "metric": "mean_time",
                "unit": "seconds"
            }
        )
        
        observability.record_metric(
            "benchmark.performance_improvement",
            improvement,
            {
                "metric": "percentage",
                "comparison": "driver_vs_rest_api"
            }
        )

        # Print results with enhanced observability
        print("\n===== Enhanced Benchmark Results with Observability =====")
        print(f"REST API - Mean Time: {rest_api_mean:.6f} sec, Std Dev: {rest_api_std:.6f}")
        print(f"Driver  - Mean Time: {driver_mean:.6f} sec, Std Dev: {driver_std:.6f}")
        print(f"Performance Improvement: {improvement:.2f}%")
        
        # Log final results
        logger.info("Benchmark completed", extra={
            "rest_api_mean": rest_api_mean,
            "driver_mean": driver_mean,
            "performance_improvement": improvement,
            "rest_api_iterations": len(rest_api_times),
            "driver_iterations": len(driver_times)
        })
        
        print("\n🎯 Observability Features Demonstrated:")
        print("  📊 Metrics: Query execution times, iteration counts, performance improvement")
        print("  🔍 Tracing: End-to-end benchmark execution with nested spans")
        print("  📝 Logging: Structured logs with benchmark context and results")
        print("  ⚡ Performance: Detailed timing analysis with observability overhead")
        
        # Set span attributes for the overall benchmark
        span.set_attribute("rest_api_mean_time", rest_api_mean)
        span.set_attribute("driver_mean_time", driver_mean)
        span.set_attribute("performance_improvement", improvement)
        span.set_attribute("total_iterations", len(rest_api_times) + len(driver_times))

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