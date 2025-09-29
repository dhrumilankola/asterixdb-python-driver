#!/usr/bin/env python3
"""
Example demonstrating the new AsterixDB Connection Pool functionality.

This example shows how to use the advanced connection pooling features
including basic usage, async queries, health checks, and observability.
"""

import time
from datetime import timedelta

# Import the new pool functionality
from src.pyasterix import (
    create_pool, 
    PoolConfig, 
    ObservabilityConfig,
    MetricsConfig,
    TracingConfig,
    LoggingConfig
)


def basic_pool_example():
    """Basic connection pool usage example."""
    print("🏊 Basic Connection Pool Example")
    print("=" * 40)
    
    # Configure pool
    pool_config = PoolConfig(
        max_pool_size=5,
        min_pool_size=2,
        max_lifetime=timedelta(minutes=30),
        idle_timeout=timedelta(minutes=5),
        validate_on_borrow=True
    )
    
    # Configure observability (optional)
    observability_config = ObservabilityConfig(
        enabled=True,
        metrics=MetricsConfig(enabled=True, prometheus_port=9091),
        tracing=TracingConfig(enabled=True, exporter="console"),
        logging=LoggingConfig(structured=True, level="INFO")
    )
    
    # Create pool with context manager
    with create_pool(
        host="localhost",
        port=19002,
        pool_config=pool_config,
        observability_config=observability_config
    ) as pool:
        
        print(f"📊 Initial pool stats: {pool.get_pool_stats()}")
        
        # Execute queries through the pool
        try:
            result = pool.execute_query(
                "SELECT VALUE 1",
                mode="immediate",
                readonly=True
            )
            print(f"✅ Query result: {result}")
            
        except Exception as e:
            print(f"⚠️  Query failed (AsterixDB may not be running): {e}")
        
        print(f"📊 After query stats: {pool.get_pool_stats()}")


def async_query_example():
    """Async query execution example."""
    print("\n🚀 Async Query Example")
    print("=" * 40)
    
    pool_config = PoolConfig(
        max_pool_size=3,
        async_poll_interval=0.1,  # Fast polling for demo
        async_max_polls=50
    )
    
    with create_pool(
        host="localhost", 
        port=19002,
        pool_config=pool_config
    ) as pool:
        
        try:
            # Execute async query
            print("🔄 Starting async query...")
            result = pool.execute_query(
                "SELECT VALUE 42",
                mode="async",
                readonly=True
            )
            print(f"✅ Async result: {result}")
            
        except Exception as e:
            print(f"⚠️  Async query failed: {e}")


def concurrent_usage_example():
    """Concurrent connection usage example."""
    print("\n👥 Concurrent Usage Example")
    print("=" * 40)
    
    import threading
    
    pool_config = PoolConfig(
        max_pool_size=3,
        min_pool_size=1,
        pool_wait_timeout=10
    )
    
    with create_pool(
        host="localhost",
        port=19002, 
        pool_config=pool_config
    ) as pool:
        
        results = []
        errors = []
        
        def worker(worker_id):
            try:
                # Get connection from pool
                with pool.get_connection(timeout=5.0) as conn:
                    # Simulate some work
                    time.sleep(0.2)
                    results.append(f"Worker {worker_id} completed")
                    print(f"   ✅ Worker {worker_id} finished")
                    
            except Exception as e:
                errors.append(f"Worker {worker_id}: {e}")
                print(f"   ❌ Worker {worker_id} failed: {e}")
        
        # Start multiple workers
        threads = []
        for i in range(5):
            thread = threading.Thread(target=worker, args=(i,))
            threads.append(thread)
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        print(f"📈 Results: {len(results)} succeeded, {len(errors)} failed")
        print(f"📊 Final pool stats: {pool.get_pool_stats()}")


def health_check_example():
    """Health check functionality example."""
    print("\n🏥 Health Check Example")
    print("=" * 40)
    
    pool_config = PoolConfig(
        max_pool_size=2,
        validate_on_borrow=True,
        health_check_query="SELECT VALUE 'health_check'"
    )
    
    with create_pool(
        host="localhost",
        port=19002,
        pool_config=pool_config
    ) as pool:
        
        # Shallow health check
        health = pool.health_check(deep=False)
        print(f"🔍 Shallow health check: {health['healthy']}")
        print(f"   Pool stats: {health['pool_stats']}")
        
        # Deep health check (validates connections)
        try:
            deep_health = pool.health_check(deep=True)
            print(f"🏥 Deep health check: {deep_health['healthy']}")
            if 'valid_connections' in deep_health:
                print(f"   Valid connections: {deep_health['valid_connections']}")
        except Exception as e:
            print(f"⚠️  Deep health check failed: {e}")


def timeout_configuration_example():
    """Demonstrate different timeout configurations."""
    print("\n⏱️ Timeout Configuration Example")
    print("=" * 40)
    
    # Configure different timeouts
    pool_config = PoolConfig(
        max_pool_size=2,
        connection_timeout=5,      # TCP connection
        query_timeout=30,          # Query execution
        pool_wait_timeout=10,      # Pool acquisition
        health_check_timeout=3     # Health validation
    )
    
    with create_pool(
        host="localhost",
        port=19002,
        pool_config=pool_config
    ) as pool:
        
        try:
            # Test connection acquisition timeout
            with pool.get_connection(timeout=2.0) as conn:
                print("✅ Connection acquired within timeout")
                
        except Exception as e:
            print(f"⏱️ Connection timeout: {e}")


def main():
    """Run all examples."""
    print("🎯 AsterixDB Connection Pool Examples")
    print("=" * 50)
    print("ℹ️  Note: These examples work even if AsterixDB is not running")
    print("ℹ️  Pool functionality will be demonstrated regardless")
    
    try:
        basic_pool_example()
        async_query_example()
        concurrent_usage_example()
        health_check_example()
        timeout_configuration_example()
        
        print("\n🎉 All examples completed successfully!")
        print("\n🚀 Phase 1 Implementation Features:")
        print("  ✅ Intelligent connection lifecycle management")
        print("  ✅ Enhanced async query support with optimized polling")
        print("  ✅ Comprehensive observability integration") 
        print("  ✅ Production-ready health checking and validation")
        print("  ✅ Background cleanup and maintenance")
        print("  ✅ Granular timeout configuration")
        print("  ✅ Thread-safe concurrent connection usage")
        print("  ✅ Context manager support for easy cleanup")
        
    except Exception as e:
        print(f"❌ Example failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
