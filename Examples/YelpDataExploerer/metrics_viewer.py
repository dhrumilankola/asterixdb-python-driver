#!/usr/bin/env python3
"""
Simple Metrics Viewer for PyAsterix Observability
"""
import requests
import time
import json
from datetime import datetime

def fetch_and_display_metrics():
    """Fetch and display metrics from Prometheus endpoint."""
    try:
        response = requests.get("http://localhost:8005/metrics", timeout=5)
        if response.status_code == 200:
            print(f"\n{'='*60}")
            print(f"📊 PyAsterix Metrics - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*60}")
            
            lines = response.text.split('\n')
            current_metric = None
            
            for line in lines:
                if line.startswith('# HELP'):
                    current_metric = line.split('# HELP ')[1].split(' ')[0]
                    description = ' '.join(line.split(' ')[3:])
                    print(f"\n🔹 {current_metric}")
                    print(f"   Description: {description}")
                elif line.startswith('# TYPE'):
                    metric_type = line.split('# TYPE ')[1].split(' ')[1]
                    print(f"   Type: {metric_type}")
                elif line and not line.startswith('#') and current_metric:
                    # Parse metric line
                    if '{' in line:
                        metric_name = line.split('{')[0]
                        labels = line.split('{')[1].split('}')[0]
                        value = line.split('} ')[1]
                        print(f"   ├─ {labels} = {value}")
                    else:
                        parts = line.split(' ')
                        if len(parts) >= 2:
                            print(f"   └─ Value: {parts[1]}")
            
            print(f"\n{'='*60}")
            print("🔄 Refreshing every 5 seconds... (Ctrl+C to stop)")
            
        else:
            print(f"❌ Failed to fetch metrics: HTTP {response.status_code}")
            print("Make sure the Streamlit app is running!")
            
    except requests.exceptions.ConnectionError:
        print("❌ Connection refused to http://localhost:8005/metrics")
        print("💡 Make sure the Streamlit app is running to start the Prometheus server")
    except Exception as e:
        print(f"❌ Error fetching metrics: {e}")

def main():
    """Main metrics viewer loop."""
    print("🚀 PyAsterix Metrics Viewer")
    print("📡 Connecting to http://localhost:8005/metrics")
    print("💡 Make sure Streamlit app is running: streamlit run app.py")
    
    try:
        while True:
            fetch_and_display_metrics()
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n👋 Metrics viewer stopped")

if __name__ == "__main__":
    main()
