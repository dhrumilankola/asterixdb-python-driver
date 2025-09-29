# PyAsterix Driver Overview

This document provides a high-level, developer-friendly overview of the PyAsterix driver: its modules, features, architecture, and how to use it effectively in production.

## Modules
- Low-level DB-API module (PEP 249 compliant): `connection`, `cursor`, `exceptions`
- High-level DataFrame module: `src/pyasterix/dataframe/*`

Both modules share core services:
- Observability: metrics, tracing, structured logging
- Connection Pool: lifecycle, validation, async-aware polling
- Templates: SQL++ helpers in `src/pyasterix/templates`

## Feature Summary
- PEP 249 compliant DB-API
- High-level DataFrame API (select, filter, agg, join, order, execute)
- Connection pooling with validation, idle expiry, background cleanup
- Async query support (status/result handles, pooled polling)
- Parameter handling (positional and named)
- Robust exception system (PEP 249 + Asterix-specific mapping)
- Observability (Prometheus metrics, OpenTelemetry tracing, structured logging)
- Performance-friendly configuration for dev vs. prod

## Installation
See `requirements/requirements.txt`. Typical usage in apps:
```bash
pip install -r requirements/requirements.txt
```

## Basic Usage
### DB-API
```python
from src.pyasterix import connect

conn = connect(host="localhost", port=19002)
cur = conn.cursor()
cur.execute("SELECT VALUE 1")
print(cur.fetchall())
cur.close()
conn.close()
```

### DataFrame API
```python
from src.pyasterix.dataframe import AsterixDataFrame
from src.pyasterix import connect

conn = connect()
df = AsterixDataFrame(conn, "products")
rows = (df
    .filter("price > 100")
    .select(["id", "name", "price"]) 
    .order_by("price", desc=True)
    .execute()
    .result_set)
```

### Connection Pool
```python
from src.pyasterix import create_pool, PoolConfig

pool = create_pool(pool_config=PoolConfig(max_pool_size=10, min_pool_size=2))
with pool.get_connection() as conn:
    cur = conn.cursor()
    cur.execute("SELECT VALUE count(*) FROM products")
    print(cur.fetchone())
```

## Architecture
- `Connection`: wraps `requests.Session`, holds timeouts, retries, trace context
- `Cursor`: builds payloads, parameterizes queries, handles async modes, parses JSON
- `Pool`: tracks connections with validation, idle/lifetime expiry, background cleanup
- `ObservabilityManager`: exposes metrics, tracing spans, and structured logging
- `Exceptions`: unified exceptions with AsterixDB error mapping and context

Data Flow (DB-API):
1) `Cursor.execute()` builds payload, sends POST to `/query/service`
2) Handles immediate/deferred/async result modes
3) Maps HTTP/Asterix errors to driver exceptions
4) Parses JSON and exposes `fetch*()` APIs

Data Flow (Pool Async):
1) Async query returns status handle
2) Pool polls status/ result endpoints with configured interval and limits
3) Metrics and spans reflect attempts, outcomes, and timings

## Error Handling
- PEP 249 exceptions plus: `HTTPError`, `NetworkError`, `TimeoutError`, `SyntaxError`, `IdentifierError`, `AsyncQueryError`, `PoolExhaustedError`, etc.
- Error mappers convert HTTP status codes and AsterixDB error payloads to precise exceptions
- All exceptions carry context (`.context`) and serialize via `.to_dict()`

## Observability
- Metrics: query durations, counts, rows fetched, pool gauges, errors
- Tracing: spans for execute/fetch/async/pool ops
- Logging: JSON logs with trace correlation
See `docs/OBSERVABILITY_FOR_DEVELOPERS.md` for full guidance.

## Performance & Tuning
- Configure `PoolConfig` for size, timeouts, validation, cleanup interval
- Lower tracing `sample_rate` and disable detailed metrics for prod
- Prefer readonly mode when appropriate; parameterize complex queries client-side

## Examples
- `examples/exception_handling_guide.py`
- `Examples/connection_pool_example.py`
- `Docs/Observability/OBSERVABILITY_USAGE_GUIDE.md`

## Roadmap
- More DataFrame operators
- Streaming results
- Async-native API surface (asyncio)

## Support
- Check structured logs, metrics, and traces first
- Review `docs/EXCEPTION_HANDLING.md` for errors and mapping
- Use provided examples to reproduce issues
