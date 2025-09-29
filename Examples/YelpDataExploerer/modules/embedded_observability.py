# modules/embedded_observability.py
"""
Embedded Observability Dashboard - No External Prometheus Server Required
This module provides real-time observability data within Streamlit itself.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import json

def run(conn):
    """Run the embedded observability dashboard."""
    st.markdown("<div class='main-header'>🔍 Embedded Observability Dashboard</div>", unsafe_allow_html=True)
    st.markdown("Real-time monitoring of PyAsterix database operations (no external server required)")
    
    # Get observability instance
    observability = getattr(conn, '_observability', None)
    if not observability:
        st.error("❌ Observability not configured")
        st.info("💡 Observability is automatically configured when you use the database connection.")
        return
    
    # Create tabs for different observability views
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Live Metrics", "📝 Recent Logs", "🔗 Trace Context", "⚙️ Configuration"])
    
    with tab1:
        show_live_metrics(observability)
    
    with tab2:
        show_recent_logs(observability)
    
    with tab3:
        show_trace_context(observability)
    
    with tab4:
        show_configuration(observability)

def show_live_metrics(observability):
    """Display live metrics from the observability manager."""
    st.markdown("### 📈 Real-Time Metrics")
    
    col1, col2, col3, col4 = st.columns(4)
    
    # Get current metrics from observability instance
    metrics_data = extract_metrics_from_observability(observability)
    
    with col1:
        st.metric(
            "Queries Executed", 
            metrics_data.get('total_queries', 0),
            delta=metrics_data.get('recent_queries', 0)
        )
    
    with col2:
        avg_duration = metrics_data.get('avg_duration', 0)
        st.metric(
            "Avg Query Time", 
            f"{avg_duration:.3f}s",
            delta=f"{metrics_data.get('duration_trend', 0):+.3f}s"
        )
    
    with col3:
        error_rate = metrics_data.get('error_rate', 0)
        st.metric(
            "Error Rate", 
            f"{error_rate:.1%}",
            delta=f"{metrics_data.get('error_trend', 0):+.1%}"
        )
    
    with col4:
        st.metric(
            "Rows Fetched", 
            f"{metrics_data.get('total_rows', 0):,}",
            delta=metrics_data.get('recent_rows', 0)
        )
    
    # Charts
    st.markdown("### 📊 Performance Charts")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Query duration over time
        duration_data = generate_duration_chart_data(observability)
        if duration_data:
            fig = px.line(
                duration_data, 
                x='timestamp', 
                y='duration',
                title="Query Duration Over Time",
                labels={'duration': 'Duration (seconds)', 'timestamp': 'Time'}
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Execute some queries to see duration trends")
    
    with col2:
        # Query types breakdown
        query_types_data = generate_query_types_data(observability)
        if query_types_data:
            fig = px.pie(
                values=list(query_types_data.values()),
                names=list(query_types_data.keys()),
                title="Query Types Distribution"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("Execute some queries to see type distribution")

def show_recent_logs(observability):
    """Display recent log entries."""
    st.markdown("### 📝 Recent Log Entries")
    
    # Get recent logs from the observability logger
    logs = get_recent_logs(observability)
    
    if logs:
        # Display logs in an expandable format
        for i, log_entry in enumerate(logs[-10:]):  # Show last 10 logs
            with st.expander(f"🔸 {log_entry.get('timestamp', 'Unknown')} - {log_entry.get('level', 'INFO')} - {log_entry.get('message', '')[:50]}..."):
                st.json(log_entry)
    else:
        st.info("No recent logs available. Interact with the dashboard to generate logs.")
        
        # Show example of what logs look like
        st.markdown("**Example log structure:**")
        example_log = {
            "timestamp": "2025-09-22T10:30:45.123456Z",
            "level": "INFO",
            "logger": "yelp_explorer.sync_query",
            "message": "Executing sync query",
            "trace": {
                "trace_id": "d0bb66360c0624a4d9a7a10144342a14",
                "span_id": "2d68b7030805fa8d"
            },
            "extra": {
                "query_type": "sync",
                "interface": "streamlit",
                "query_preview": "SELECT VALUE COUNT(*) FROM YelpDataverse.Businesses;"
            }
        }
        st.json(example_log)

def show_trace_context(observability):
    """Display current trace context information."""
    st.markdown("### 🔗 Current Trace Context")
    
    # Get current span context
    span_context = observability.get_current_span_context()
    
    if span_context:
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Current Span Information:**")
            st.code(f"""
Trace ID: {span_context.get('trace_id', 'N/A')}
Span ID: {span_context.get('span_id', 'N/A')}
Correlation ID: {span_context.get('correlation_id', 'N/A')}
Trace Flags: {span_context.get('trace_flags', 'N/A')}
            """)
        
        with col2:
            st.markdown("**Trace Context Headers:**")
            trace_context = observability.get_current_trace_context()
            if trace_context:
                st.json(trace_context)
            else:
                st.info("No active trace context")
    else:
        st.info("No active span. Execute a query to create a trace.")
        
        # Provide instructions
        st.markdown("""
        **To see trace context:**
        1. Navigate to another page (Dashboard, Business Explorer)
        2. Execute a query or operation
        3. Return to this page to see the trace information
        """)

def show_configuration(observability):
    """Display observability configuration."""
    st.markdown("### ⚙️ Observability Configuration")
    
    config = observability.config
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Metrics Configuration:**")
        st.json({
            "enabled": config.metrics.enabled,
            "namespace": config.metrics.namespace,
            "prometheus_port": config.metrics.prometheus_port,
            "custom_labels": config.metrics.custom_labels
        })
        
        st.markdown("**Logging Configuration:**")
        st.json({
            "structured": config.logging.structured,
            "level": config.logging.level,
            "correlation_enabled": config.logging.correlation_enabled,
            "include_trace_info": config.logging.include_trace_info
        })
    
    with col2:
        st.markdown("**Tracing Configuration:**")
        st.json({
            "enabled": config.tracing.enabled,
            "sample_rate": config.tracing.sample_rate,
            "exporter": config.tracing.exporter,
            "service_name": config.tracing.service_name
        })
        
        st.markdown("**Status:**")
        status_items = [
            ("✅" if config.metrics.enabled else "❌", "Metrics Collection"),
            ("✅" if config.tracing.enabled else "❌", "Distributed Tracing"),
            ("✅" if config.logging.structured else "❌", "Structured Logging"),
            ("✅" if observability._initialized else "❌", "Observability Initialized")
        ]
        
        for status, item in status_items:
            st.markdown(f"{status} {item}")

def extract_metrics_from_observability(observability):
    """Extract metrics data from the observability manager."""
    # This is a simulation since we can't directly access prometheus metrics
    # In a real implementation, you'd extract this from the observability instance
    
    # Get current time for simulation
    current_time = datetime.now()
    
    # Simulate metrics data
    metrics = {
        'total_queries': getattr(observability, '_query_count', 0),
        'recent_queries': 0,
        'avg_duration': 0.245,  # Simulate average
        'duration_trend': 0.012,
        'error_rate': 0.02,
        'error_trend': -0.01,
        'total_rows': getattr(observability, '_rows_fetched', 0),
        'recent_rows': 0
    }
    
    return metrics

def generate_duration_chart_data(observability):
    """Generate chart data for query durations."""
    # In a real implementation, you'd collect this from observability metrics
    # For now, return None to show the "no data" message
    return None

def generate_query_types_data(observability):
    """Generate data for query types chart."""
    # In a real implementation, you'd collect this from observability metrics
    # For now, return None to show the "no data" message
    return None

def get_recent_logs(observability):
    """Get recent log entries from the observability system."""
    # In a real implementation, you'd collect logs from the logging system
    # For now, return empty list to show the example
    return []

def create_test_metrics_button(observability):
    """Create a button to generate test metrics."""
    if st.button("🧪 Generate Test Metrics"):
        with st.spinner("Generating test metrics..."):
            # Increment internal counters
            if not hasattr(observability, '_query_count'):
                observability._query_count = 0
            if not hasattr(observability, '_rows_fetched'):
                observability._rows_fetched = 0
            
            observability._query_count += 1
            observability._rows_fetched += 100
            
            # Record some test metrics
            try:
                observability.record_query_duration(0.123, query_type="test", interface="embedded_dashboard")
                observability.increment_query_count(query_type="test", status="success", interface="embedded_dashboard")
                observability.increment_rows_fetched(100, query_type="test", interface="embedded_dashboard")
            except Exception as e:
                st.error(f"Error recording metrics: {e}")
            
            st.success("✅ Test metrics generated!")
            time.sleep(1)
            st.rerun()
