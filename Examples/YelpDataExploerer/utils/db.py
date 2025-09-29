# utils/db.py
import streamlit as st
import pandas as pd
import sys
import time
sys.path.append('../..')
from src.pyasterix import (
    connect, 
    ObservabilityConfig, 
    MetricsConfig, 
    TracingConfig, 
    LoggingConfig,
    initialize_observability
)

@st.cache_resource
def setup_observability():
    """Setup observability for Yelp Data Explorer (no external server)."""
    config = ObservabilityConfig(
        metrics=MetricsConfig(
            enabled=True,
            namespace="yelp_data_explorer",
            prometheus_port=8010  # Disable external Prometheus server
        ),
        tracing=TracingConfig(
            enabled=True,
            service_name="yelp_explorer_service",
            sample_rate=0.3,  # Sample 30% for web app
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
    st.success("✅ Observability configured (embedded mode)")
    return observability

@st.cache_resource
def connect_to_asterixdb():
    try:
        # Setup observability
        observability = setup_observability()
        
        # Connect with observability enabled
        conn = connect(
            host="localhost",
            port=19002,
            observability_config=observability.config
        )
        
        # Store observability instance for later use
        conn._observability = observability
        
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
        # Get observability instance if available
        observability = getattr(_conn, '_observability', None)
        logger = observability.get_logger("yelp_explorer.sync_query") if observability else None
        
        # Create span for query execution
        span_context = observability.start_span(
            "yelp_explorer.execute_query_sync",
            kind="INTERNAL",
            query_type="sync",
            interface="streamlit"
        ) if observability else None
        
        with span_context if span_context else nullcontext():
            if logger:
                logger.info("Executing sync query", extra={
                    "query_type": "sync",
                    "interface": "streamlit",
                    "query_preview": query[:100] + "..." if len(query) > 100 else query
                })
            
            # Start timing the query execution
            start_time = time.time()
            
            cursor = _conn.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            results = normalize_results(cursor, results)
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Record comprehensive query metrics using correct method names
            if observability:
                # Record query duration
                observability.record_query_duration(
                    execution_time,
                    query_type="sync",
                    interface="streamlit",
                    result_count=len(results)
                )
                
                # Record query success count
                observability.increment_query_count(
                    query_type="sync",
                    status="success",
                    interface="streamlit"
                )
                
                # Record rows fetched
                observability.increment_rows_fetched(
                    len(results),
                    query_type="sync",
                    interface="streamlit"
                )
                
                # Set span attributes for detailed tracing
                if span_context:
                    span_context.set_attribute("query_execution_time", execution_time)
                    span_context.set_attribute("result_count", len(results))
                    span_context.set_attribute("query_length", len(query))
            
            if cursor.description is not None and not (results and isinstance(results[0], dict)):
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(results, columns=columns)
            else:
                return pd.DataFrame(results)
                
    except Exception as e:
        # Record error metrics using correct method names
        if observability:
            # Record query error
            observability.increment_query_count(
                query_type="sync",
                status="error",
                interface="streamlit"
            )
            
            # Record connection error for detailed error tracking
            observability.record_connection_error(
                error_type=type(e).__name__,
                interface="streamlit",
                query_type="sync"
            )
            
        if logger:
            logger.error("Sync query failed", exc_info=True, extra={
                "query_type": "sync",
                "interface": "streamlit",
                "error_type": type(e).__name__
            })
        
        st.error(f"Query execution failed: {str(e)}")
        return pd.DataFrame()

# Context manager for when observability is None
from contextlib import nullcontext

def execute_query_async(_conn, query, max_attempts=10, poll_interval=1, _progress_callback=None):
    try:
        # Get observability instance if available
        observability = getattr(_conn, '_observability', None)
        logger = observability.get_logger("yelp_explorer.async_query") if observability else None
        
        # Create span for async query execution
        span_context = observability.start_span(
            "yelp_explorer.execute_query_async",
            kind="INTERNAL",
            query_type="async",
            interface="streamlit",
            max_attempts=max_attempts
        ) if observability else None
        
        with span_context if span_context else nullcontext():
            if logger:
                logger.info("Executing async query", extra={
                    "query_type": "async",
                    "interface": "streamlit",
                    "max_attempts": max_attempts,
                    "query_preview": query[:100] + "..." if len(query) > 100 else query
                })
            
            # Start timing the query execution
            start_time = time.time()
            
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
            
            # Calculate execution time
            execution_time = time.time() - start_time
            
            # Record comprehensive async query metrics
            if observability:
                # Record query duration
                observability.record_query_duration(
                    execution_time,
                    query_type="async",
                    interface="streamlit",
                    result_count=len(results),
                    attempts_used=attempt
                )
                
                # Record query success count
                observability.increment_query_count(
                    query_type="async",
                    status="success",
                    interface="streamlit"
                )
                
                # Record rows fetched
                observability.increment_rows_fetched(
                    len(results),
                    query_type="async",
                    interface="streamlit"
                )
                
                # Set span attributes for detailed tracing
                if span_context:
                    span_context.set_attribute("query_execution_time", execution_time)
                    span_context.set_attribute("result_count", len(results))
                    span_context.set_attribute("attempts_used", attempt)
                    span_context.set_attribute("query_length", len(query))
            
            if cursor.description is not None and not (results and isinstance(results[0], dict)):
                columns = [desc[0] for desc in cursor.description]
                return pd.DataFrame(results, columns=columns)
            else:
                return pd.DataFrame(results)
                
    except Exception as e:
        # Record error metrics for async queries
        if observability:
            # Record query error
            observability.increment_query_count(
                query_type="async",
                status="error",
                interface="streamlit"
            )
            
            # Record connection error for detailed error tracking
            observability.record_connection_error(
                error_type=type(e).__name__,
                interface="streamlit",
                query_type="async"
            )
            
        if logger:
            logger.error("Async query failed", exc_info=True, extra={
                "query_type": "async",
                "interface": "streamlit",
                "error_type": type(e).__name__
            })
        
        st.error(f"Query execution failed: {str(e)}")
        return pd.DataFrame()
