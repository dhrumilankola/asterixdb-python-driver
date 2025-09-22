import time
import os
import sys

root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, root_path)

from src.pyasterix import (
    connect, 
    ObservabilityConfig, 
    MetricsConfig, 
    TracingConfig, 
    LoggingConfig,
    initialize_observability
)

def setup_observability():
    """Setup observability for async queries testing."""
    config = ObservabilityConfig(
        metrics=MetricsConfig(
            enabled=True,
            namespace="pyasterix_async_queries",
            prometheus_port=8003
        ),
        tracing=TracingConfig(
            enabled=True,
            service_name="async_queries_service",
            sample_rate=1.0,
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
    print("✅ Observability initialized for async queries")
    return observability

def test_async_queries():
    try:
        # Setup observability
        observability = setup_observability()
        logger = observability.get_logger("async_queries")
        
        # Initialize connection with observability
        with connect(
            host="localhost",
            port=19002,
            observability_config=observability.config
        ) as conn:
            
            # Start overall async test span
            with observability.start_span("async_queries.test_suite", kind="INTERNAL") as test_span:
                logger.info("Starting async queries test suite", extra={
                    "test_type": "async_queries",
                    "connection_type": "observability_enabled"
                })
                
                print("\nConnection initialized with observability.")

                # Create a cursor
                cursor = conn.cursor()
                print("Cursor created.")

            # Setup: Create necessary dataverse and dataset
            print("\nSetup: Creating dataverse and dataset for async test")
            cursor.execute("""
                DROP DATAVERSE TinySocial IF EXISTS;
                CREATE DATAVERSE TinySocial;
                USE TinySocial;

                CREATE TYPE GleambookUserType AS {
                    id: int,
                    alias: string,
                    name: string,
                    userSince: datetime,
                    friendIds: {{ int }},
                    employment: [ { organizationName: string, startDate: date, endDate: date? } ],
                    nickname: string?
                };

                CREATE DATASET GleambookUsers(GleambookUserType)
                    PRIMARY KEY id;
            """)
            print("Dataverse and dataset created.")

            # Insert sample data using immediate mode (since it's a DML operation)
            print("\nInserting sample data")
            cursor.execute("""
                USE TinySocial;

                INSERT INTO GleambookUsers([
                    { "id": 1, "alias": "Willis", "name": "WillisWynne", "userSince": datetime("2005-01-17T10:10:00.000Z"), "friendIds": {{ 1, 3, 7 }}, "employment": [ { "organizationName": "jaydax", "startDate": date("2009-05-15") } ] },
                    { "id": 2, "alias": "Isbel", "name": "IsbelDull", "userSince": datetime("2011-01-22T10:10:00.000Z"), "friendIds": {{ 1, 4 }}, "employment": [ { "organizationName": "Hexviafind", "startDate": date("2010-04-27") } ] }
                ]);
            """)
            print("Sample data inserted.")

            # Test 1: Asynchronous select query
            print("\nTest 1: Asynchronous select query")
            cursor.execute("""
                USE TinySocial;
                SELECT u.id, u.name, u.alias 
                FROM GleambookUsers u 
                WHERE u.id >= 1 
                ORDER BY u.id;
            """, mode="async")
            print("Async Query Submitted. Awaiting completion...")

            # Polling for completion
            attempt = 1
            max_attempts = 10
            while attempt <= max_attempts:
                status_result = cursor._get_query_status(cursor.results['handle'])
                print(f"Status check {attempt}: {status_result}")

                if status_result.get("status") == "success":
                    print("Async query completed successfully.")
                    final_result = cursor._get_query_result(status_result['handle'])
                    print("Final result:", final_result)
                    break
                elif status_result.get("status") in ("FAILED", "FATAL"):
                    print(f"Async query failed with status: {status_result.get('status')}")
                    break

                print(f"Attempt {attempt}/{max_attempts}: Query still running...")
                attempt += 1
                time.sleep(1)

            if attempt > max_attempts:
                print("Async query did not complete within the maximum number of attempts.")


            # Test 2: Another async query with aggregation
            print("\nTest 2: Another async query with aggregation")
            cursor.execute("""
                USE TinySocial;
                SELECT 
                    u.employment[0].organizationName as org,
                    COUNT(*) as emp_count
                FROM GleambookUsers u
                GROUP BY u.employment[0].organizationName
                ORDER BY emp_count DESC;
            """, mode="async")

            print("Second async query submitted. Awaiting completion...")
            attempt = 1
            while attempt <= max_attempts:
                status_result = cursor._get_query_status(cursor.results['handle'])
                print(f"Status check {attempt}: {status_result}")

                if status_result.get("status") == "success":
                    print("Async aggregation query completed successfully.")
                    final_result = cursor._get_query_result(status_result['handle'])
                    print("Aggregation result:", final_result)
                    break
                elif status_result.get("status") in ("FAILED", "FATAL"):
                    print(f"Async query failed with status: {status_result.get('status')}")
                    break

                print(f"Attempt {attempt}/{max_attempts}: Query still running...")
                attempt += 1
                time.sleep(1)
                
            if attempt > max_attempts:
                print("Async query did not complete within the maximum number of attempts.")

            # Cleanup: Dropping the dataverse
            print("\nCleanup: Dropping dataverse")
            cursor.execute("DROP DATAVERSE TinySocial IF EXISTS;")
            print("Cleanup completed.")

    except Exception as e:
        print(f"Error occurred during async test: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_async_queries()
