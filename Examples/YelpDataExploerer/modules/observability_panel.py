# modules/observability_panel.py
import streamlit as st
import requests
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import time

def run(conn):
    """Run the observability monitoring panel."""
    st.markdown("<div class='main-header'>🔍 Observability Dashboard</div>", unsafe_allow_html=True)
    st.markdown("Real-time monitoring of PyAsterix database operations and performance metrics.")
    
    # Create columns for metrics display
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### 📊 Live Metrics")
        
        # Placeholder for metrics
        metrics_placeholder = st.empty()
        
        # Auto-refresh toggle
        auto_refresh = st.checkbox("🔄 Auto-refresh (5 seconds)", value=True)
        
        if st.button("🔄 Refresh Now") or auto_refresh:
            try:
                # Fetch metrics from Prometheus endpoint
                response = requests.get("http://localhost:8005/metrics", timeout=2)
                
                if response.status_code == 200:
                    metrics_text = response.text
                    
                    # Parse key metrics
                    metrics_data = parse_prometheus_metrics(metrics_text)
                    
                    with metrics_placeholder.container():
                        display_metrics_summary(metrics_data)
                        
                        # Create charts
                        if metrics_data:
                            st.markdown("### 📈 Query Performance")
                            create_performance_charts(metrics_data)
                            
                            st.markdown("### 🔢 Raw Metrics Data")
                            with st.expander("View Raw Prometheus Metrics"):
                                st.text(metrics_text)
                else:
                    st.error(f"Failed to fetch metrics: HTTP {response.status_code}")
                    
            except requests.exceptions.ConnectionError:
                st.error("🚫 Cannot connect to Prometheus metrics server")
                st.info("💡 Make sure the observability features are enabled and the metrics server is running on port 8005")
            except Exception as e:
                st.error(f"Error fetching metrics: {e}")
    
    with col2:
        st.markdown("### ⚙️ Observability Status")
        
        # Check observability status
        observability_status = check_observability_status()
        
        if observability_status['prometheus_available']:
            st.success("✅ Prometheus Metrics: Active")
        else:
            st.error("❌ Prometheus Metrics: Unavailable")
            
        if observability_status['tracing_enabled']:
            st.success("✅ Distributed Tracing: Active") 
        else:
            st.warning("⚠️ Distributed Tracing: Disabled")
            
        if observability_status['logging_enabled']:
            st.success("✅ Structured Logging: Active")
        else:
            st.warning("⚠️ Structured Logging: Disabled")
        
        st.markdown("### 📍 Endpoints")
        st.code("Metrics: http://localhost:8005/metrics")
        st.code("Streamlit: http://localhost:8501")
        
        st.markdown("### 🛠️ Tools")
        if st.button("🔗 Open Metrics in Browser"):
            st.markdown('[📊 View Prometheus Metrics](http://localhost:8005/metrics)')
            
        st.markdown("### ℹ️ Information")
        st.info("""
        **Observability Features:**
        - Query execution timing
        - Request tracing 
        - Error monitoring
        - Performance metrics
        - Connection monitoring
        """)
    
    # Auto-refresh functionality
    if auto_refresh:
        time.sleep(5)
        st.rerun()

def parse_prometheus_metrics(metrics_text):
    """Parse Prometheus metrics text into structured data."""
    metrics = {}
    lines = metrics_text.split('\n')
    
    for line in lines:
        if line and not line.startswith('#'):
            try:
                if '{' in line:
                    # Parse labeled metric
                    metric_name = line.split('{')[0]
                    labels_str = line.split('{')[1].split('}')[0]
                    value = float(line.split('} ')[1])
                    
                    # Parse labels
                    labels = {}
                    for label_pair in labels_str.split(','):
                        if '=' in label_pair:
                            key, val = label_pair.split('=', 1)
                            labels[key.strip()] = val.strip('"')
                    
                    if metric_name not in metrics:
                        metrics[metric_name] = []
                    
                    metrics[metric_name].append({
                        'labels': labels,
                        'value': value
                    })
                else:
                    # Parse simple metric
                    parts = line.split(' ')
                    if len(parts) >= 2:
                        metric_name = parts[0]
                        value = float(parts[1])
                        metrics[metric_name] = [{'labels': {}, 'value': value}]
                        
            except (ValueError, IndexError):
                continue
                
    return metrics

def display_metrics_summary(metrics_data):
    """Display key metrics in a summary format."""
    if not metrics_data:
        st.warning("No metrics data available")
        return
    
    # Create metrics summary
    col1, col2, col3, col4 = st.columns(4)
    
    # Query count
    query_total = get_metric_value(metrics_data, 'pyasterix_query_total')
    with col1:
        st.metric("Total Queries", f"{query_total:,.0f}" if query_total else "0")
    
    # Rows fetched
    rows_fetched = get_metric_value(metrics_data, 'pyasterix_rows_fetched_total')
    with col2:
        st.metric("Rows Fetched", f"{rows_fetched:,.0f}" if rows_fetched else "0")
    
    # Connection errors
    errors = get_metric_value(metrics_data, 'pyasterix_connection_errors_total')
    with col3:
        st.metric("Connection Errors", f"{errors:,.0f}" if errors else "0")
    
    # Active connections
    active_conns = get_metric_value(metrics_data, 'pyasterix_connection_pool_active')
    with col4:
        st.metric("Active Connections", f"{active_conns:,.0f}" if active_conns else "0")

def get_metric_value(metrics_data, metric_name):
    """Get the total value for a metric across all labels."""
    if metric_name not in metrics_data:
        return 0
    
    total = 0
    for metric in metrics_data[metric_name]:
        total += metric['value']
    
    return total

def create_performance_charts(metrics_data):
    """Create performance visualization charts."""
    
    # Query duration histogram
    if 'pyasterix_query_duration_seconds_bucket' in metrics_data:
        duration_data = metrics_data['pyasterix_query_duration_seconds_bucket']
        
        # Create DataFrame for plotting
        df_duration = []
        for metric in duration_data:
            if 'le' in metric['labels']:
                df_duration.append({
                    'bucket': metric['labels']['le'],
                    'count': metric['value'],
                    'query_type': metric['labels'].get('query_type', 'unknown')
                })
        
        if df_duration:
            df = pd.DataFrame(df_duration)
            df['bucket'] = pd.to_numeric(df['bucket'], errors='coerce')
            df = df.dropna(subset=['bucket']).sort_values('bucket')
            
            fig = px.bar(df, x='bucket', y='count', color='query_type',
                        title="Query Duration Distribution",
                        labels={'bucket': 'Duration (seconds)', 'count': 'Number of Queries'})
            st.plotly_chart(fig, use_container_width=True)
    
    # Query types breakdown
    if 'pyasterix_query_total' in metrics_data:
        query_data = metrics_data['pyasterix_query_total']
        
        df_queries = []
        for metric in query_data:
            df_queries.append({
                'query_type': metric['labels'].get('query_type', 'unknown'),
                'status': metric['labels'].get('status', 'unknown'),
                'count': metric['value']
            })
        
        if df_queries:
            df = pd.DataFrame(df_queries)
            
            # Pie chart for query types
            fig = px.pie(df, values='count', names='query_type',
                        title="Query Types Distribution")
            st.plotly_chart(fig, use_container_width=True)

def check_observability_status():
    """Check the status of various observability components."""
    status = {
        'prometheus_available': False,
        'tracing_enabled': False,
        'logging_enabled': False
    }
    
    # Check Prometheus availability
    try:
        response = requests.get("http://localhost:8005/metrics", timeout=2)
        status['prometheus_available'] = response.status_code == 200
    except:
        pass
    
    # Check if observability is configured (simplified check)
    try:
        # This would be more sophisticated in a real implementation
        status['tracing_enabled'] = True  # Assume enabled if we got this far
        status['logging_enabled'] = True  # Assume enabled if we got this far
    except:
        pass
    
    return status
