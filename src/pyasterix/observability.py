"""
Observability and monitoring configuration for pyasterix.

This module provides comprehensive observability features including:
- Metrics collection via OpenTelemetry and Prometheus
- Distributed tracing with configurable exporters
- Structured logging with correlation IDs
"""

import os
import logging
import threading
import json
import time
import uuid
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler
from typing import Dict, Any, Optional, Union
from dataclasses import dataclass, field

try:
    from opentelemetry import metrics, trace, context
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from opentelemetry.trace import Status, StatusCode, SpanKind
    from opentelemetry.propagate import extract, inject
    from prometheus_client import start_http_server, generate_latest, CONTENT_TYPE_LATEST
    
    # Try to import additional exporters
    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        OTLP_AVAILABLE = True
    except ImportError:
        OTLP_AVAILABLE = False
    
    try:
        from opentelemetry.exporter.jaeger.thrift import JaegerExporter
        JAEGER_AVAILABLE = True
    except ImportError:
        JAEGER_AVAILABLE = False
    
    OBSERVABILITY_AVAILABLE = True
except ImportError:
    OBSERVABILITY_AVAILABLE = False
    OTLP_AVAILABLE = False
    JAEGER_AVAILABLE = False


@dataclass
class MetricsConfig:
    """Configuration for metrics collection."""
    enabled: bool = True
    prometheus_port: Optional[int] = None
    custom_labels: Dict[str, str] = field(default_factory=dict)
    namespace: str = "pyasterix"


@dataclass
class TracingConfig:
    """Configuration for distributed tracing."""
    enabled: bool = True
    sample_rate: float = 1.0
    exporter: str = "console"  # console, otlp, jaeger
    service_name: str = "pyasterix-client"
    service_version: str = "0.1.0"
    otlp_endpoint: Optional[str] = None  # e.g., "http://localhost:4317"
    jaeger_agent_host: str = "localhost"
    jaeger_agent_port: int = 14268
    batch_export: bool = True  # Use batch processor for better performance
    max_export_batch_size: int = 512
    export_timeout_millis: int = 30000


@dataclass
class LoggingConfig:
    """Configuration for structured logging."""
    structured: bool = True
    level: str = "INFO"
    correlation_enabled: bool = True
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    include_trace_info: bool = True
    max_message_length: int = 1000
    sensitive_fields: list = None
    
    def __post_init__(self):
        if self.sensitive_fields is None:
            self.sensitive_fields = ["password", "token", "secret", "key"]


class LogFilter(logging.Filter):
    """
    Custom log filter for performance and content filtering.
    """
    
    def __init__(self, config: LoggingConfig):
        super().__init__()
        self.config = config
        self.performance_threshold = 1.0  # Log performance warnings above 1 second
        
    def filter(self, record: logging.LogRecord) -> bool:
        """Filter log records based on configuration and performance criteria."""
        
        # Performance-based filtering
        if hasattr(record, 'duration_seconds'):
            duration = getattr(record, 'duration_seconds', 0)
            
            # For performance logs, only log if above threshold or if error
            if record.levelno < logging.WARNING and duration < self.performance_threshold:
                # Skip debug/info performance logs that are below threshold
                if hasattr(record, 'operation') and 'performance' in record.name:
                    return False
        
        # Content-based filtering
        message = record.getMessage()
        
        # Skip repetitive connection messages unless they're errors
        if record.levelno < logging.WARNING:
            if 'connection closed' in message.lower() or 'connection opened' in message.lower():
                # Only log every 10th connection event to reduce noise
                return hash(message) % 10 == 0
        
        # Filter out debug messages for successful simple operations
        if record.levelno == logging.DEBUG:
            if hasattr(record, 'rows_affected'):
                rows = getattr(record, 'rows_affected', 0)
                # Skip debug logs for simple single-row operations
                if rows <= 1 and not hasattr(record, 'error_type'):
                    return False
        
        return True


class SmartLogLevel:
    """
    Smart log level adjustment based on context and performance.
    """
    
    def __init__(self, base_level: str = "INFO"):
        self.base_level = getattr(logging, base_level.upper())
        self.error_count = 0
        self.last_error_time = 0
        self.performance_issues = 0
        
    def get_effective_level(self, context: Dict[str, Any] = None) -> int:
        """Get effective log level based on current context."""
        current_time = time.time()
        
        # Increase verbosity if recent errors
        if current_time - self.last_error_time < 300:  # 5 minutes
            if self.error_count > 3:
                return logging.DEBUG  # More verbose during error periods
        
        # Increase verbosity for performance issues
        if context and context.get('duration_seconds', 0) > 5.0:
            return logging.INFO  # Log slow operations
        
        return self.base_level
    
    def record_error(self):
        """Record an error occurrence."""
        self.error_count += 1
        self.last_error_time = time.time()
    
    def record_performance_issue(self, duration: float):
        """Record a performance issue."""
        if duration > 2.0:
            self.performance_issues += 1


class StructuredJSONFormatter(logging.Formatter):
    """
    Custom JSON formatter for structured logging with trace correlation.
    """
    
    def __init__(self, config: LoggingConfig):
        super().__init__()
        self.config = config
        self.hostname = os.getenv('HOSTNAME', 'unknown')
        
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON."""
        # Base log structure
        log_entry = {
            "timestamp": self._format_timestamp(record.created),
            "level": record.levelname,
            "logger": record.name,
            "message": self._sanitize_message(record.getMessage()),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
            "thread": record.thread,
            "process": record.process,
            "hostname": self.hostname
        }
        
        # Add trace correlation if enabled
        if self.config.correlation_enabled and self.config.include_trace_info:
            trace_info = self._get_trace_info()
            if trace_info:
                log_entry["trace"] = trace_info
        
        # Add extra fields from log record
        extra_fields = self._extract_extra_fields(record)
        if extra_fields:
            log_entry["extra"] = extra_fields
        
        # Add exception information if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info)
            }
        
        # Add stack info if present
        if record.stack_info:
            log_entry["stack_info"] = record.stack_info
        
        return json.dumps(log_entry, default=str, separators=(',', ':'))
    
    def _format_timestamp(self, timestamp: float) -> str:
        """Format timestamp in ISO format."""
        # Use datetime for proper microsecond formatting
        from datetime import datetime, timezone
        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    
    def _sanitize_message(self, message: str) -> str:
        """Sanitize log message to remove sensitive information."""
        if len(message) > self.config.max_message_length:
            message = message[:self.config.max_message_length] + "..."
        
        # Remove sensitive information
        for field in self.config.sensitive_fields:
            if field.lower() in message.lower():
                # Simple masking - replace sensitive values
                import re
                pattern = rf'({field}\s*[=:]\s*)[^\s,}}\]]+' 
                message = re.sub(pattern, r'\1***', message, flags=re.IGNORECASE)
        
        return message
    
    def _get_trace_info(self) -> Optional[Dict[str, str]]:
        """Extract current trace information."""
        if not OBSERVABILITY_AVAILABLE:
            return None
        
        try:
            current_span = trace.get_current_span()
            if current_span and current_span.is_recording():
                span_context = current_span.get_span_context()
                return {
                    "trace_id": f"{span_context.trace_id:032x}",
                    "span_id": f"{span_context.span_id:016x}",
                    "trace_flags": f"{span_context.trace_flags:02x}"
                }
        except Exception:
            pass  # Ignore tracing errors in logging
        
        return None
    
    def _extract_extra_fields(self, record: logging.LogRecord) -> Dict[str, Any]:
        """Extract extra fields from log record."""
        # Standard fields to exclude
        standard_fields = {
            'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename',
            'module', 'exc_info', 'exc_text', 'stack_info', 'lineno', 'funcName',
            'created', 'msecs', 'relativeCreated', 'thread', 'threadName',
            'processName', 'process', 'message'
        }
        
        extra_fields = {}
        for key, value in record.__dict__.items():
            if key not in standard_fields and not key.startswith('_'):
                # Sanitize extra field values
                if isinstance(value, str) and any(sensitive in key.lower() for sensitive in self.config.sensitive_fields):
                    extra_fields[key] = "***"
                else:
                    extra_fields[key] = value
        
        return extra_fields


class CorrelatedLogger:
    """
    Logger wrapper that automatically adds correlation context.
    """
    
    def __init__(self, logger: logging.Logger, observability_manager=None):
        self.logger = logger
        self.observability_manager = observability_manager
    
    def _add_correlation_context(self, extra: Dict[str, Any] = None) -> Dict[str, Any]:
        """Add correlation context to log extra fields."""
        if extra is None:
            extra = {}
        
        # Add correlation ID if available
        if self.observability_manager:
            correlation_id = self.observability_manager.create_correlation_id()
            extra["correlation_id"] = correlation_id
            
            # Add current span context if available
            span_context = self.observability_manager.get_current_span_context()
            if span_context:
                extra.update(span_context)
        
        return extra
    
    def debug(self, msg, *args, extra=None, **kwargs):
        extra = self._add_correlation_context(extra)
        self.logger.debug(msg, *args, extra=extra, **kwargs)
    
    def info(self, msg, *args, extra=None, **kwargs):
        extra = self._add_correlation_context(extra)
        self.logger.info(msg, *args, extra=extra, **kwargs)
    
    def warning(self, msg, *args, extra=None, **kwargs):
        extra = self._add_correlation_context(extra)
        self.logger.warning(msg, *args, extra=extra, **kwargs)
    
    def error(self, msg, *args, extra=None, **kwargs):
        extra = self._add_correlation_context(extra)
        self.logger.error(msg, *args, extra=extra, **kwargs)
    
    def critical(self, msg, *args, extra=None, **kwargs):
        extra = self._add_correlation_context(extra)
        self.logger.critical(msg, *args, extra=extra, **kwargs)
    
    def exception(self, msg, *args, extra=None, **kwargs):
        extra = self._add_correlation_context(extra)
        self.logger.exception(msg, *args, extra=extra, **kwargs)


class PerformanceLogger:
    """
    Performance-aware logger for timing operations with automatic context.
    """
    
    def __init__(self, logger: CorrelatedLogger, operation: str):
        self.logger = logger
        self.operation = operation
        self.start_time = None
        
    def start(self, **context):
        """Start timing the operation."""
        self.start_time = time.time()
        self.logger.debug(f"Starting {self.operation}", extra={
            "operation": self.operation,
            "phase": "start",
            **context
        })
        
    def checkpoint(self, checkpoint_name: str, **context):
        """Log a checkpoint with elapsed time."""
        if self.start_time:
            elapsed = time.time() - self.start_time
            self.logger.debug(f"{self.operation} checkpoint: {checkpoint_name}", extra={
                "operation": self.operation,
                "phase": "checkpoint",
                "checkpoint": checkpoint_name,
                "elapsed_seconds": elapsed,
                **context
            })
    
    def complete(self, success: bool = True, **context):
        """Complete the operation timing."""
        if self.start_time:
            elapsed = time.time() - self.start_time
            level = "info" if success else "warning"
            getattr(self.logger, level)(f"Completed {self.operation}", extra={
                "operation": self.operation,
                "phase": "complete",
                "success": success,
                "duration_seconds": elapsed,
                **context
            })
        
    def error(self, error: Exception, **context):
        """Log an error during the operation."""
        if self.start_time:
            elapsed = time.time() - self.start_time
        else:
            elapsed = 0
            
        self.logger.error(f"Error in {self.operation}: {error}", extra={
            "operation": self.operation,
            "phase": "error",
            "error_type": type(error).__name__,
            "error_message": str(error),
            "duration_seconds": elapsed,
            **context
        })


@dataclass
class ObservabilityConfig:
    """Comprehensive observability configuration."""
    enabled: bool = True
    metrics: MetricsConfig = field(default_factory=MetricsConfig)
    tracing: TracingConfig = field(default_factory=TracingConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    @classmethod
    def from_env(cls) -> 'ObservabilityConfig':
        """Create configuration from environment variables."""
        return cls(
            enabled=_get_env_bool("PYASTERIX_OBSERVABILITY_ENABLED", True),
            metrics=MetricsConfig(
                enabled=_get_env_bool("PYASTERIX_METRICS_ENABLED", True),
                prometheus_port=_get_env_int("PYASTERIX_PROMETHEUS_PORT"),
                namespace=os.getenv("PYASTERIX_METRICS_NAMESPACE", "pyasterix")
            ),
            tracing=TracingConfig(
                enabled=_get_env_bool("PYASTERIX_TRACING_ENABLED", True),
                sample_rate=_get_env_float("PYASTERIX_TRACE_SAMPLE_RATE", 1.0),
                exporter=os.getenv("PYASTERIX_TRACE_EXPORTER", "console"),
                service_name=os.getenv("PYASTERIX_SERVICE_NAME", "pyasterix-client"),
                service_version=os.getenv("PYASTERIX_SERVICE_VERSION", "0.1.0"),
                otlp_endpoint=os.getenv("PYASTERIX_OTLP_ENDPOINT"),
                jaeger_agent_host=os.getenv("PYASTERIX_JAEGER_HOST", "localhost"),
                jaeger_agent_port=_get_env_int("PYASTERIX_JAEGER_PORT", 14268),
                batch_export=_get_env_bool("PYASTERIX_BATCH_EXPORT", True)
            ),
            logging=LoggingConfig(
                structured=_get_env_bool("PYASTERIX_STRUCTURED_LOGGING", True),
                level=os.getenv("PYASTERIX_LOG_LEVEL", "INFO"),
                correlation_enabled=_get_env_bool("PYASTERIX_LOG_CORRELATION", True),
                include_trace_info=_get_env_bool("PYASTERIX_LOG_TRACE_INFO", True),
                max_message_length=_get_env_int("PYASTERIX_LOG_MAX_LENGTH", 1000)
            )
        )


class PrometheusServer:
    """Robust Prometheus HTTP server that stays alive."""
    
    class MetricsHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/metrics':
                try:
                    output = generate_latest()
                    self.send_response(200)
                    self.send_header('Content-Type', CONTENT_TYPE_LATEST)
                    self.send_header('Content-Length', str(len(output)))
                    self.end_headers()
                    self.wfile.write(output)
                except Exception as e:
                    self.send_error(500, f"Error generating metrics: {e}")
            else:
                self.send_error(404, "Not Found")
        
        def log_message(self, format, *args):
            # Suppress default HTTP server logging
            pass
    
    def __init__(self, port):
        self.port = port
        self.server = None
        self.thread = None
        self.started = False
    
    def start(self):
        """Start the Prometheus HTTP server in a daemon thread."""
        if self.started:
            return True
            
        try:
            # Use the original prometheus_client server which is more robust
            from prometheus_client import start_http_server
            
            # Check if port is available
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                result = s.connect_ex(('localhost', self.port))
                if result == 0:
                    print(f"⚠️ Port {self.port} is already in use")
                    return False
            
            # Start the prometheus_client HTTP server
            start_http_server(self.port)
            self.started = True
            
            print(f"🚀 Prometheus metrics server started on port {self.port}")
            print(f"📊 Metrics available at: http://localhost:{self.port}/metrics")
            
            # Test the server immediately
            import time
            time.sleep(1)
            try:
                import requests
                response = requests.get(f'http://localhost:{self.port}/metrics', timeout=2)
                print(f"✅ Server test successful: {response.status_code}")
            except Exception as test_e:
                print(f"⚠️ Server test failed: {test_e}")
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to start Prometheus server: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def stop(self):
        """Stop the Prometheus HTTP server."""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.started = False

# Global server instance to keep it alive
_prometheus_server = None


class ObservabilityManager:
    """Manages observability instrumentation for pyasterix."""
    
    def __init__(self, config: Optional[ObservabilityConfig] = None):
        self.config = config or ObservabilityConfig.from_env()
        self._initialized = False
        self._lock = threading.Lock()
        self._tracer = None
        self._meter = None
        self._metrics = {}
        self._prometheus_server_started = False
        self.smart_log_level = SmartLogLevel(self.config.logging.level)
        
        # Initialize if observability is enabled
        if self.config.enabled and OBSERVABILITY_AVAILABLE:
            self._initialize()
    
    def _initialize(self):
        """Initialize observability components."""
        with self._lock:
            if self._initialized:
                return
                
            try:
                # Initialize metrics
                if self.config.metrics.enabled:
                    self._init_metrics()
                
                # Initialize tracing  
                if self.config.tracing.enabled:
                    self._init_tracing()
                
                # Initialize logging
                if self.config.logging.structured:
                    self._init_logging()
                
                self._initialized = True
                
            except Exception as e:
                # Graceful fallback - log error but don't break functionality
                logging.getLogger(__name__).warning(
                    f"Failed to initialize observability: {e}. Continuing without observability."
                )
    
    def _init_metrics(self):
        """Initialize metrics collection."""
        try:
            # Create Prometheus metric reader
            prometheus_reader = PrometheusMetricReader()
            
            # Create meter provider
            meter_provider = MeterProvider(metric_readers=[prometheus_reader])
            metrics.set_meter_provider(meter_provider)
            
            # Get meter for this service
            self._meter = metrics.get_meter(
                self.config.metrics.namespace
            )
            
            # Create core metrics
            self._metrics = {
                'query_duration': self._meter.create_histogram(
                    name="query_duration_seconds",
                    description="Query execution duration in seconds",
                    unit="s"
                ),
                'query_total': self._meter.create_counter(
                    name="query_total",
                    description="Total number of queries executed",
                    unit="1"
                ),
                'connection_pool_active': self._meter.create_gauge(
                    name="connection_pool_active",
                    description="Number of active connections",
                    unit="1"
                ),
                'rows_fetched_total': self._meter.create_counter(
                    name="rows_fetched_total", 
                    description="Total number of rows fetched",
                    unit="1"
                ),
                'connection_errors': self._meter.create_counter(
                    name="connection_errors_total",
                    description="Total number of connection errors",
                    unit="1"
                )
            }
            
            # Start robust Prometheus HTTP server if port is configured
            if (self.config.metrics.prometheus_port and 
                not self._prometheus_server_started):
                global _prometheus_server
                if _prometheus_server is None:
                    _prometheus_server = PrometheusServer(self.config.metrics.prometheus_port)
                
                if _prometheus_server.start():
                    self._prometheus_server_started = True
                else:
                    print(f"⚠️ Failed to start Prometheus server on port {self.config.metrics.prometheus_port}")
                
        except Exception as e:
            logging.getLogger(__name__).warning(f"Failed to initialize metrics: {e}")
            print(f"⚠️ Metrics initialization failed: {e}")
            import traceback
            print(f"Error details: {traceback.format_exc()}")
    
    def _init_tracing(self):
        """Initialize distributed tracing with configurable exporters."""
        try:
            # Create tracer provider with resource information
            resource_attributes = {
                "service.name": self.config.tracing.service_name,
                "service.version": self.config.tracing.service_version,
                "library.name": "pyasterix",
                "library.version": "0.1.0"
            }
            
            from opentelemetry.sdk.resources import Resource
            resource = Resource.create(resource_attributes)
            tracer_provider = TracerProvider(resource=resource)
            
            # Configure span processor and exporter based on config
            span_exporter = self._create_span_exporter()
            if span_exporter:
                if self.config.tracing.batch_export:
                    span_processor = BatchSpanProcessor(
                        span_exporter,
                        max_export_batch_size=self.config.tracing.max_export_batch_size,
                        export_timeout_millis=self.config.tracing.export_timeout_millis
                    )
                else:
                    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
                    span_processor = SimpleSpanProcessor(span_exporter)
                
                tracer_provider.add_span_processor(span_processor)
            
            # Set the tracer provider globally
            trace.set_tracer_provider(tracer_provider)
            
            # Get tracer for this service
            self._tracer = trace.get_tracer(
                self.config.tracing.service_name
            )
            
        except Exception as e:
            logging.getLogger(__name__).warning(f"Failed to initialize tracing: {e}")
    
    def _create_span_exporter(self):
        """Create appropriate span exporter based on configuration."""
        exporter_type = self.config.tracing.exporter.lower()
        
        try:
            if exporter_type == "console":
                return ConsoleSpanExporter()
            
            elif exporter_type == "otlp" and OTLP_AVAILABLE:
                if self.config.tracing.otlp_endpoint:
                    return OTLPSpanExporter(endpoint=self.config.tracing.otlp_endpoint)
                else:
                    return OTLPSpanExporter()  # Use default endpoint
            
            elif exporter_type == "jaeger" and JAEGER_AVAILABLE:
                return JaegerExporter(
                    agent_host_name=self.config.tracing.jaeger_agent_host,
                    agent_port=self.config.tracing.jaeger_agent_port
                )
            
            else:
                logging.getLogger(__name__).warning(
                    f"Unsupported or unavailable trace exporter: {exporter_type}. "
                    f"Falling back to console exporter."
                )
                return ConsoleSpanExporter()
                
        except Exception as e:
            logging.getLogger(__name__).warning(
                f"Failed to create {exporter_type} exporter: {e}. Using console exporter."
            )
            return ConsoleSpanExporter()
    
    def _init_logging(self):
        """Initialize structured logging with JSON formatter."""
        try:
            # Get root logger for pyasterix
            root_logger = logging.getLogger('pyasterix')
            
            # Clear existing handlers to avoid duplication
            root_logger.handlers.clear()
            
            # Set log level
            root_logger.setLevel(getattr(logging, self.config.logging.level.upper()))
            
            # Create console handler
            console_handler = logging.StreamHandler()
            
            # Add log filter for smart filtering
            log_filter = LogFilter(self.config.logging)
            console_handler.addFilter(log_filter)
            
            if self.config.logging.structured:
                # Use structured JSON formatter
                formatter = StructuredJSONFormatter(self.config.logging)
            else:
                # Use standard formatter
                formatter = logging.Formatter(self.config.logging.format)
            
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)
            
            # Prevent propagation to avoid duplicate logs
            root_logger.propagate = False
            
        except Exception as e:
            # Fallback to basic logging
            logging.basicConfig(
                level=getattr(logging, self.config.logging.level.upper()),
                format=self.config.logging.format
            )
            logging.getLogger(__name__).warning(f"Failed to configure structured logging: {e}")
    
    def get_logger(self, name: str) -> CorrelatedLogger:
        """
        Get a correlated logger instance.
        
        Args:
            name: Logger name
            
        Returns:
            CorrelatedLogger instance with automatic correlation context
        """
        base_logger = logging.getLogger(name)
        return CorrelatedLogger(base_logger, self)
    
    def create_performance_logger(self, operation: str) -> 'PerformanceLogger':
        """
        Create a performance logger for timing operations.
        
        Args:
            operation: Operation name for logging context
            
        Returns:
            PerformanceLogger instance
        """
        return PerformanceLogger(self.get_logger(f"pyasterix.performance.{operation}"), operation)
    
    @property
    def tracer(self):
        """Get the tracer instance."""
        return self._tracer
    
    @property
    def meter(self):
        """Get the meter instance."""
        return self._meter
    
    def get_metric(self, name: str):
        """Get a specific metric by name."""
        return self._metrics.get(name)
    
    def record_query_duration(self, duration: float, **labels):
        """Record query execution duration."""
        if metric := self.get_metric('query_duration'):
            try:
                metric.record(duration, labels)
            except Exception:
                pass  # Silently ignore metric recording errors
        
        # Track performance issues for smart logging
        self.smart_log_level.record_performance_issue(duration)
    
    def increment_query_count(self, **labels):
        """Increment total query counter."""
        if metric := self.get_metric('query_total'):
            try:
                metric.add(1, labels)
            except Exception:
                pass
    
    def increment_rows_fetched(self, count: int, **labels):
        """Increment rows fetched counter."""
        if metric := self.get_metric('rows_fetched_total'):
            try:
                metric.add(count, labels)
            except Exception:
                pass
    
    def record_connection_error(self, **labels):
        """Record a connection error."""
        if metric := self.get_metric('connection_errors'):
            try:
                metric.add(1, labels)
            except Exception:
                pass
        
        # Update smart log level for error tracking
        self.smart_log_level.record_error()
    
    def set_active_connections(self, count: int, **labels):
        """Set the number of active connections."""
        if metric := self.get_metric('connection_pool_active'):
            try:
                metric.set(count, labels)
            except Exception:
                pass
    
    # Tracing utility methods
    def start_span(self, name: str, kind: str = "INTERNAL", **attributes):
        """
        Start a new span with the given name and attributes.
        
        Args:
            name: The span name
            kind: The span kind (CLIENT, SERVER, INTERNAL, etc.)
            **attributes: Additional span attributes
        
        Returns:
            Span context manager or None if tracing is disabled
        """
        if not self._tracer:
            return self._NoOpSpan()
        
        try:
            span_kind_map = {
                "CLIENT": SpanKind.CLIENT,
                "SERVER": SpanKind.SERVER, 
                "INTERNAL": SpanKind.INTERNAL,
                "PRODUCER": SpanKind.PRODUCER,
                "CONSUMER": SpanKind.CONSUMER
            }
            
            span_kind = span_kind_map.get(kind.upper(), SpanKind.INTERNAL)
            
            # Use start_as_current_span to properly activate the span
            span_context = self._tracer.start_as_current_span(name, kind=span_kind)
            
            # The context manager doesn't have set_attribute, so we need to handle this differently
            # We'll create a wrapper that can set attributes when the span is active
            return self._SpanContextManager(span_context, attributes)
            
        except Exception as e:
            logging.getLogger(__name__).debug(f"Failed to start span: {e}")
            return self._NoOpSpan()
    
    def create_database_span(self, operation: str, query: str = None, **attributes):
        """
        Create a database-specific span with standard attributes.
        
        Args:
            operation: Database operation (execute, fetch, etc.)
            query: SQL++ query string (truncated for safety)
            **attributes: Additional span attributes
        
        Returns:
            Span context manager
        """
        span_name = f"pyasterix.{operation}"
        
        # Standard database span attributes
        db_attributes = {
            "db.system": "asterixdb",
            "db.operation": operation,
            "component": "pyasterix",
            **attributes
        }
        
        # Add query if provided (truncate for safety)
        if query:
            if len(query) > 1000:
                db_attributes["db.statement"] = query[:1000] + "..."
            else:
                db_attributes["db.statement"] = query
        
        return self.start_span(span_name, kind="CLIENT", **db_attributes)
    
    def record_span_exception(self, span, exception: Exception):
        """Record an exception on a span and set error status."""
        if span and hasattr(span, 'record_exception'):
            try:
                span.record_exception(exception)
                span.set_status(Status(StatusCode.ERROR, str(exception)))
            except Exception:
                pass  # Ignore tracing errors
    
    def set_span_success(self, span):
        """Mark a span as successful."""
        if span and hasattr(span, 'set_status'):
            try:
                span.set_status(Status(StatusCode.OK))
            except Exception:
                pass
    
    # Context propagation methods
    def get_current_trace_context(self) -> Dict[str, str]:
        """
        Get the current trace context as a dictionary for propagation.
        
        Returns:
            Dictionary containing trace context headers
        """
        if not OBSERVABILITY_AVAILABLE:
            return {}
        
        try:
            # Create carrier for context injection
            carrier = {}
            inject(carrier)
            
            # If no context was injected, try to get span context directly
            if not carrier:
                span_context = self.get_current_span_context()
                if span_context:
                    # Create basic trace context from span context
                    carrier = {
                        "traceparent": f"00-{span_context['trace_id']}-{span_context['span_id']}-{span_context['trace_flags']}"
                    }
            
            return carrier
        except Exception as e:
            logging.getLogger(__name__).debug(f"Failed to get trace context: {e}")
            return {}
    
    def set_trace_context(self, context_dict: Dict[str, str]):
        """
        Set the trace context from a dictionary.
        
        Args:
            context_dict: Dictionary containing trace context headers
        """
        if not OBSERVABILITY_AVAILABLE or not context_dict:
            return
        
        try:
            # Extract context from carrier and set as current
            ctx = extract(context_dict)
            token = context.attach(ctx)
            return token
        except Exception as e:
            logging.getLogger(__name__).debug(f"Failed to set trace context: {e}")
            return None
    
    def create_correlation_id(self) -> str:
        """
        Create a unique correlation ID for request tracking.
        
        Returns:
            Unique correlation ID string
        """
        return str(uuid.uuid4())
    
    def get_current_span_context(self) -> Optional[Dict[str, str]]:
        """
        Get current span context information.
        
        Returns:
            Dictionary with trace_id, span_id, and correlation_id if available
        """
        if not OBSERVABILITY_AVAILABLE:
            return None
        
        try:
            current_span = trace.get_current_span()
            if current_span and hasattr(current_span, 'get_span_context'):
                span_context = current_span.get_span_context()
                if span_context and span_context.is_valid:
                    return {
                        "trace_id": f"{span_context.trace_id:032x}",
                        "span_id": f"{span_context.span_id:016x}",
                        "trace_flags": f"{span_context.trace_flags:02x}",
                        "correlation_id": self.create_correlation_id()
                    }
        except Exception as e:
            logging.getLogger(__name__).debug(f"Failed to get span context: {e}")
        
        return None
    
    def start_span_with_context(self, name: str, parent_context: Optional[Dict[str, str]] = None, **attributes):
        """
        Start a span with optional parent context.
        
        Args:
            name: Span name
            parent_context: Optional parent context from upstream service
            **attributes: Span attributes
        
        Returns:
            Span context manager
        """
        if not self._tracer:
            return self._NoOpSpan()
        
        try:
            # Set parent context if provided
            token = None
            if parent_context:
                token = self.set_trace_context(parent_context)
            
            # Create span
            span = self.start_span(name, **attributes)
            
            # Add correlation info if we have a token
            if token and span and hasattr(span, 'set_attribute'):
                correlation_id = self.create_correlation_id()
                span.set_attribute("correlation.id", correlation_id)
                span.set_attribute("trace.propagated", "true")
            
            return span
            
        except Exception as e:
            logging.getLogger(__name__).debug(f"Failed to start span with context: {e}")
            return self._NoOpSpan()

    class _SpanContextManager:
        """Context manager for spans that properly activates them."""
        def __init__(self, span_context, attributes=None):
            self.span_context = span_context
            self.attributes = attributes or {}
            self.active_span = None
        
        def __enter__(self):
            # Use the span's built-in context manager
            self.active_span = self.span_context.__enter__()
            
            # Set attributes on the active span
            for key, value in self.attributes.items():
                if value is not None:
                    self.active_span.set_attribute(key, str(value))
            
            return self.active_span
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            # Use the span's built-in context manager
            return self.span_context.__exit__(exc_type, exc_val, exc_tb)
        
        def set_attribute(self, key, value):
            if self.active_span:
                self.active_span.set_attribute(key, value)
        
        def record_exception(self, exception):
            if self.active_span:
                self.active_span.record_exception(exception)
        
        def set_status(self, status):
            if self.active_span:
                self.active_span.set_status(status)

    class _NoOpSpan:
        """No-operation span for when tracing is disabled."""
        def __enter__(self):
            return self
        
        def __exit__(self, exc_type, exc_val, exc_tb):
            pass
        
        def set_attribute(self, key, value):
            pass
        
        def record_exception(self, exception):
            pass
        
        def set_status(self, status):
            pass


# Utility functions for environment variable parsing
def _get_env_bool(key: str, default: bool = False) -> bool:
    """Get boolean value from environment variable."""
    value = os.getenv(key, "").lower()
    if value in ("true", "1", "yes", "on"):
        return True
    elif value in ("false", "0", "no", "off"):
        return False
    return default


def _get_env_int(key: str, default: Optional[int] = None) -> Optional[int]:
    """Get integer value from environment variable."""
    value = os.getenv(key)
    if value is not None:
        try:
            return int(value)
        except ValueError:
            pass
    return default


def _get_env_float(key: str, default: float = 0.0) -> float:
    """Get float value from environment variable."""
    value = os.getenv(key)
    if value is not None:
        try:
            return float(value)
        except ValueError:
            pass
    return default


# Global observability manager instance
_global_observability_manager: Optional[ObservabilityManager] = None


def get_observability_manager() -> Optional[ObservabilityManager]:
    """Get the global observability manager instance."""
    return _global_observability_manager


def initialize_observability(config: Optional[ObservabilityConfig] = None) -> ObservabilityManager:
    """Initialize global observability manager."""
    global _global_observability_manager
    if _global_observability_manager is None:
        _global_observability_manager = ObservabilityManager(config)
    return _global_observability_manager
