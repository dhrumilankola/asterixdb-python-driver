#!/usr/bin/env python3
"""
Simple Observability Demo for High-Level DataFrame Operations
This script demonstrates the observability features with working examples.
"""

import os
import sys
import time

# Add the project root to the path
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
    """Setup observability for DataFrame demo."""
    config = ObservabilityConfig(
        metrics=MetricsConfig(
            enabled=True,
            namespace="dataframe_demo",
            prometheus_port=8007
        ),
        tracing=TracingConfig(
            enabled=True,
            service_name="dataframe_demo_service",
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
    print("✅ Observability initialized for DataFrame demo")
    return observability

def demo_with_observability():
    """Demonstrate observability features with working examples."""
    
    # Initialize observability
    observability = setup_observability()
    logger = observability.get_logger("dataframe_demo")
    
    # Connect with observability
    conn = connect(
        host="localhost",
        port=19002,
        observability_config=observability.config
    )
    
    print("\n🎯 Demonstrating Observability Features in High-Level DataFrame Operations")
    print("=" * 80)
    
    with observability.start_span("demo.setup_test_data", kind="INTERNAL") as setup_span:
        logger.info("Setting up test environment", extra={
            "operation": "setup",
            "demo_type": "dataframe_observability"
        })
        
        cursor = conn.cursor()
        
        # Create simple test dataset
        print("\n📝 Step 1: Setting up test data with observability...")
        try:
            cursor.execute("""
                DROP DATAVERSE ObservabilityDemo IF EXISTS;
                CREATE DATAVERSE ObservabilityDemo;
                USE ObservabilityDemo;
                
                CREATE TYPE UserType AS {
                    id: int,
                    name: string,
                    age: int,
                    city: string
                };
                
                CREATE DATASET Users(UserType) PRIMARY KEY id;
            """)
            
            cursor.execute("""
                USE ObservabilityDemo;
                INSERT INTO Users([
                    {"id": 1, "name": "Alice", "age": 30, "city": "New York"},
                    {"id": 2, "name": "Bob", "age": 25, "city": "San Francisco"},
                    {"id": 3, "name": "Charlie", "age": 35, "city": "New York"},
                    {"id": 4, "name": "Diana", "age": 28, "city": "Chicago"},
                    {"id": 5, "name": "Eve", "age": 32, "city": "San Francisco"}
                ]);
            """)
            
            setup_span.set_attribute("setup_status", "success")
            setup_span.set_attribute("records_inserted", 5)
            
            logger.info("Test data setup completed", extra={
                "records_inserted": 5,
                "datasets_created": 1
            })
            
            print("✅ Test data created successfully")
            
        except Exception as e:
            setup_span.set_attribute("setup_status", "error")
            setup_span.record_exception(e)
            logger.error("Setup failed", exc_info=True, extra={
                "error_type": type(e).__name__
            })
            print(f"❌ Setup failed: {e}")
            return
    
    # Demo 1: Simple Query with Observability
    print("\n📊 Step 2: Simple Query with Observability Tracking...")
    with observability.start_span("demo.simple_query", kind="INTERNAL") as query_span:
        try:
            start_time = time.time()
            
            cursor.execute("""
                USE ObservabilityDemo;
                SELECT u.name, u.age, u.city 
                FROM Users u 
                WHERE u.age > 28 
                ORDER BY u.age DESC;
            """)
            
            results = cursor.fetchall()
            end_time = time.time()
            
            execution_time = end_time - start_time
            
            # Record metrics using the correct method
            observability.record_query_duration(
                execution_time,
                query_type="simple_select",
                result_count=len(results)
            )
            
            # Set span attributes
            query_span.set_attribute("query_type", "simple_select")
            query_span.set_attribute("execution_time", execution_time)
            query_span.set_attribute("result_count", len(results))
            query_span.set_attribute("status", "success")
            
            logger.info("Simple query completed", extra={
                "query_type": "simple_select",
                "execution_time": execution_time,
                "result_count": len(results)
            })
            
            print(f"✅ Query executed in {execution_time:.4f} seconds")
            print(f"📋 Results ({len(results)} rows):")
            for result in results:
                print(f"   {result}")
                
        except Exception as e:
            query_span.set_attribute("status", "error")
            query_span.record_exception(e)
            logger.error("Simple query failed", exc_info=True)
            print(f"❌ Query failed: {e}")
    
    # Demo 2: Aggregation Query with Performance Monitoring
    print("\n📈 Step 3: Aggregation Query with Performance Monitoring...")
    with observability.start_span("demo.aggregation_query", kind="INTERNAL") as agg_span:
        try:
            start_time = time.time()
            
            cursor.execute("""
                USE ObservabilityDemo;
                SELECT u.city, COUNT(*) as user_count, AVG(u.age) as avg_age
                FROM Users u 
                GROUP BY u.city 
                ORDER BY user_count DESC;
            """)
            
            results = cursor.fetchall()
            end_time = time.time()
            
            execution_time = end_time - start_time
            
            # Record performance metrics using the correct method
            observability.record_query_duration(
                execution_time,
                query_type="aggregation",
                result_count=len(results)
            )
            
            # Set span attributes
            agg_span.set_attribute("query_type", "aggregation")
            agg_span.set_attribute("execution_time", execution_time)
            agg_span.set_attribute("result_count", len(results))
            agg_span.set_attribute("aggregation_functions", "COUNT,AVG")
            agg_span.set_attribute("status", "success")
            
            logger.info("Aggregation query completed", extra={
                "query_type": "aggregation",
                "execution_time": execution_time,
                "result_count": len(results),
                "aggregation_functions": ["COUNT", "AVG"]
            })
            
            print(f"✅ Aggregation query executed in {execution_time:.4f} seconds")
            print(f"📊 City Statistics ({len(results)} cities):")
            for result in results:
                print(f"   {result}")
                
        except Exception as e:
            agg_span.set_attribute("status", "error")
            agg_span.record_exception(e)
            logger.error("Aggregation query failed", exc_info=True)
            print(f"❌ Aggregation query failed: {e}")
    
    # Demo 3: Error Handling with Observability
    print("\n⚠️  Step 4: Error Handling with Observability...")
    with observability.start_span("demo.error_handling", kind="INTERNAL") as error_span:
        try:
            start_time = time.time()
            
            # Intentionally cause an error
            cursor.execute("""
                USE ObservabilityDemo;
                SELECT u.nonexistent_field 
                FROM Users u;
            """)
            
            results = cursor.fetchall()
            end_time = time.time()
            
        except Exception as e:
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Record error metrics using the correct method
            observability.record_connection_error(
                error_type=type(e).__name__,
                query_type="invalid_field"
            )
            
            # Set span attributes for error
            error_span.set_attribute("status", "error")
            error_span.set_attribute("error_type", type(e).__name__)
            error_span.set_attribute("execution_time", execution_time)
            error_span.record_exception(e)
            
            logger.error("Intentional error for demo", exc_info=True, extra={
                "query_type": "invalid_field",
                "execution_time": execution_time,
                "demo_purpose": "error_handling"
            })
            
            print(f"✅ Error correctly captured and tracked in {execution_time:.4f} seconds")
            print(f"🔍 Error details logged with correlation ID for debugging")
    
    # Demo 4: Performance Comparison
    print("\n⚡ Step 5: Performance Comparison with Metrics...")
    
    queries = [
        ("single_user", "SELECT * FROM Users u WHERE u.id = 1"),
        ("age_filter", "SELECT * FROM Users u WHERE u.age > 25"),
        ("city_group", "SELECT city, COUNT(*) FROM Users u GROUP BY city")
    ]
    
    for query_name, query in queries:
        with observability.start_span(f"demo.performance.{query_name}", kind="INTERNAL") as perf_span:
            try:
                start_time = time.time()
                
                cursor.execute(f"USE ObservabilityDemo; {query}")
                results = cursor.fetchall()
                
                end_time = time.time()
                execution_time = end_time - start_time
                
                # Record detailed performance metrics
                observability.record_query_duration(
                    execution_time,
                    query_name=query_name,
                    result_count=len(results),
                    query_complexity="low" if "WHERE" not in query else "medium" if "GROUP BY" not in query else "high"
                )
                
                perf_span.set_attribute("query_name", query_name)
                perf_span.set_attribute("execution_time", execution_time)
                perf_span.set_attribute("result_count", len(results))
                
                logger.info(f"Performance test: {query_name}", extra={
                    "query_name": query_name,
                    "execution_time": execution_time,
                    "result_count": len(results)
                })
                
                print(f"   📊 {query_name}: {execution_time:.4f}s ({len(results)} rows)")
                
            except Exception as e:
                perf_span.record_exception(e)
                logger.error(f"Performance test failed: {query_name}", exc_info=True)
                print(f"   ❌ {query_name}: failed")
    
    # Cleanup
    print("\n🧹 Step 6: Cleanup...")
    with observability.start_span("demo.cleanup", kind="INTERNAL"):
        try:
            cursor.execute("DROP DATAVERSE ObservabilityDemo IF EXISTS;")
            logger.info("Cleanup completed")
            print("✅ Test data cleaned up")
        except Exception as e:
            logger.error("Cleanup failed", exc_info=True)
            print(f"⚠️  Cleanup failed: {e}")
    
    print("\n🎉 Observability Demo Completed!")
    print("=" * 80)
    print("📋 Summary of Observability Features Demonstrated:")
    print("   🔍 Distributed Tracing: End-to-end request tracking")
    print("   📊 Metrics Collection: Query performance and error rates")
    print("   📝 Structured Logging: Contextual logs with correlation IDs")
    print("   ⚠️  Error Tracking: Exception capture and analysis")
    print("   ⚡ Performance Monitoring: Query timing and optimization")
    print("   🔗 Trace Correlation: Linking logs, metrics, and traces")
    print("=" * 80)
    print(f"🌐 Metrics available at: http://localhost:8007/metrics")
    print(f"📊 Prometheus format for integration with monitoring stacks")

if __name__ == "__main__":
    print("🚀 High-Level DataFrame Observability Demo")
    print("=" * 80)
    demo_with_observability()
