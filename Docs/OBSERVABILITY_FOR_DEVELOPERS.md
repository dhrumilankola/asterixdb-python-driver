# Observability for Developers

This document explains how observability is implemented in the pyasterix driver and how to use it effectively as a developer. It covers architecture, configuration, APIs, operational guidance, and best practices for production.

## Goals
- Provide consistent Metrics, Tracing, and Logging across driver components
- Enable rapid diagnosis of failures and performance problems
- Offer safe defaults with tunable overhead for prod vs. dev
- Integrate easily with Prometheus, Grafana, Jaeger/OTel backends

## Components Overview
- Metrics: exported via Prometheus and OpenTelemetry
- Tracing: OpenTelemetry spans for DB operations, async polling, pooling, and DataFrame flows
- Logging: structured JSON logs with trace correlation and performance-aware filtering

Key files:
- `src/pyasterix/observability.py`: core implementation (configs, manager, formatters)
- `src/pyasterix/connection.py`: connection-level span context and metrics
- `src/pyasterix/cursor.py`: query spans, HTTP timing, error recording
- `src/pyasterix/pool.py`: pool lifecycle metrics and async polling spans
- `src/pyasterix/dataframe/base.py`: DataFrame execution spans and summary metrics

## Quick Start
```python
from src.pyasterix import connect, ObservabilityConfig, MetricsConfig, TracingConfig, LoggingConfig

config = ObservabilityConfig(
    metrics=MetricsConfig(enabled=True, namespace="pyasterix_dev"),
    tracing=TracingConfig(enabled=True, exporter="console", service_name="pyasterix-client-dev"),
    logging=LoggingConfig(structured=True, level="DEBUG", include_trace_info=True)
)

conn = connect(host="localhost", port=19002, observability_config=config)
cur = conn.cursor()
cur.execute("SELECT VALUE 1")
print(cur.fetchall())
```

## Configuration
Observability is configured via `ObservabilityConfig` with nested configs:
- `MetricsConfig`: enable, namespace, optional Prometheus port
- `TracingConfig`: enable, exporter (`console`, `otlp`, `jaeger`), `sample_rate`, `service_name`
- `LoggingConfig`: `structured`, `level`, `include_trace_info`, sensitive fields masking

Environment-based bootstrap is supported (see `ObservabilityConfig.from_env()` usage in examples). Suggested environment keys:
- `OBSERVABILITY_METRICS_ENABLED`, `OBSERVABILITY_TRACING_ENABLED`, `OBSERVABILITY_LOGGING_ENABLED`
- `OBSERVABILITY_TRACING_EXPORTER`, `OBSERVABILITY_SERVICE_NAME`, `OBSERVABILITY_OTLP_ENDPOINT`

## Tracing Model
Spans are created for:
- `query.execute` (with attributes: url, method, params count, processed statement hash)
- async polling: `query.async.poll` and per-attempt child spans
- fetch operations: `fetch.one`, `fetch.many`, `fetch.all`
- DataFrame `.execute()` flows with result size metadata
- Pool operations: borrow/return, validation, cleanup

Trace context propagation:
- `Connection.get_trace_context()` returns W3C traceparent headers
- Upstream context can be set when creating the connection (for distributed traces)

Recommended tracing exporters:
- Dev: `exporter="console"`
- Prod: `exporter="otlp"` to an OTel collector, or `jaeger` if available

## Metrics
Default driver metrics (labels include status, mode, service):
- query_duration_seconds (histogram)
- query_total (counter)
- query_rows_fetched (counter)
- connection_pool_active/available (gauges)
- connection_errors_total (counter)
- async polling attempts and timeouts (counters)

Prometheus
- Option A: Use `prometheus_client.start_http_server(port)` in your app
- Option B: Integrate via OTel `PrometheusMetricReader` (already wired when available)

## Logging
- Structured JSON via `StructuredJSONFormatter`
- Context added: trace_id/span_id (when enabled), module/function/line, extra fields
- Performance-aware filtering suppresses low-signal debug logs below thresholds
- Sensitive fields masking: passwords, tokens, secrets, keys

Recommended levels:
- Dev: `DEBUG` or `INFO`
- Prod: `INFO` or `WARNING` with structured logs

## Error Recording
- All exceptions are captured into spans and logs with context
- HTTP failures mapped to driver exceptions and recorded in metrics
- Async timeouts and handle errors produce precise span attributes and counters

## Pool Observability
- Borrow/return events tracked with pool size and availability
- Validation failures recorded with counters and optional deep health checks
- Cleanup thread logs errors and updates gauges

## Performance Tuning
- Reduce tracing `sample_rate` in prod (e.g., `0.1`)
- Disable detailed performance metrics if overhead is a concern
- Prefer readonly queries when possible to optimize instrumentation
- Use async with proper polling intervals from `PoolConfig`

## Production Recipes
- Export Prometheus on port 8000 and scrape with Prometheus; visualize in Grafana
- Export traces via OTLP to your collector; view in Jaeger/Tempo/Zipkin
- Forward structured logs to ELK/Datadog with trace correlation

## Troubleshooting
- No metrics? Ensure the server is started or OTel reader is available
- No traces? Check exporter choice, endpoint, and `sample_rate`
- Loud logs? Raise `LoggingConfig.level`, disable performance details
- Overhead? Lower sampling, disable deep metrics, use batch span processor

## Best Practices
- Always provide a `service_name` in tracing for clarity
- Include business labels in metrics via custom counters where helpful
- Use `extra` fields in logs for high-value context (user_id, order_id)
- Propagate trace context across service boundaries for end-to-end visibility

## API Pointers
- `initialize_observability(config) -> ObservabilityManager`
- `ObservabilityManager.create_database_span(operation, **attrs)`
- `ObservabilityManager.get_logger(name)`
- `ObservabilityManager.record_query_duration(seconds, **labels)`
- `Connection.get_trace_context()` / `get_span_context()`

See also: `Docs/Observability/OBSERVABILITY_USAGE_GUIDE.md` for end-user examples and `examples/` for runnable demos.
