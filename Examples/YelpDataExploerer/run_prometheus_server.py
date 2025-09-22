#!/usr/bin/env python3
"""
Standalone Prometheus Metrics Server for PyAsterix Observability
Run this in a separate terminal to provide persistent metrics endpoint.
"""
import sys
import os
import time
import signal
import threading

# Add project root to path
sys.path.append('../..')

def main():
    print("🚀 Starting Standalone Prometheus Metrics Server for PyAsterix...")
    
    try:
        # Initialize observability components
        from src.pyasterix.observability import (
            initialize_observability, 
            ObservabilityConfig, 
            MetricsConfig, 
            TracingConfig, 
            LoggingConfig
        )
        from prometheus_client import start_http_server, generate_latest
        
        print("✅ Imports successful")
        
        # Create observability configuration
        config = ObservabilityConfig(
            metrics=MetricsConfig(
                enabled=True,
                namespace="yelp_data_explorer",
                prometheus_port=None  # We'll start manually
            ),
            tracing=TracingConfig(enabled=True),
            logging=LoggingConfig(structured=True)
        )
        
        print("✅ Configuration created")
        
        # Initialize observability to set up metrics
        observability = initialize_observability(config)
        print("✅ Observability initialized")
        
        # Start Prometheus HTTP server
        port = 8005
        start_http_server(port)
        print(f"🌐 Prometheus server started successfully!")
        print(f"📊 Metrics endpoint: http://localhost:{port}/metrics")
        print(f"🔗 Access from browser: http://localhost:{port}/metrics")
        print("\n" + "="*60)
        print("🎯 Server is now ready for Streamlit app!")
        print("   1. Keep this terminal open")
        print("   2. In another terminal, run: streamlit run app.py")
        print("   3. Use the Streamlit app to generate metrics")
        print("   4. Refresh http://localhost:8005/metrics to see updates")
        print("="*60)
        print("\n⚠️  Press Ctrl+C to stop the server")
        
        # Test the server
        print("\n🧪 Testing server...")
        try:
            import requests
            response = requests.get(f'http://localhost:{port}/metrics', timeout=5)
            print(f"✅ Server test successful: HTTP {response.status_code}")
            print(f"📝 Initial metrics: {len(response.text)} characters")
        except Exception as test_e:
            print(f"⚠️ Server test warning: {test_e}")
            print("   (This is normal if no metrics have been generated yet)")
        
        # Keep server alive
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\n👋 Prometheus server stopped gracefully")
            
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("Make sure you're in the correct directory and dependencies are installed")
    except Exception as e:
        print(f"❌ Error starting server: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
