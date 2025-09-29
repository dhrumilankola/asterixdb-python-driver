# DB-API (PEP 249) Developer Guide

This guide documents the low-level DB-API module of the PyAsterix driver, compliant with PEP 249 and extended with AsterixDB-specific capabilities.

## Quick Start
```python
from src.pyasterix import connect

conn = connect(host="localhost", port=19002, timeout=30)
cur = conn.cursor()
cur.execute("SELECT VALUE 1")
print(cur.fetchall())
cur.close()
conn.close()
```

## Connection
`connect(host, port, timeout, max_retries, retry_delay, observability_config, trace_context)`
- Returns a `Connection`
- `commit()` and `rollback()` raise `NotSupportedError` (AsterixDB has no transactions)
- `cursor()` returns a `Cursor`
- `close()` closes the underlying HTTP session
- `get_trace_context()` / `get_span_context()` expose tracing context

## Cursor
### `execute(statement, params=None, mode="immediate", pretty=False, readonly=False)`
- Modes: `immediate`, `deferred`, `async`
- Positional params: `args=[...]`
- Named params: `$name=value` emitted as form data
- Client-side substitution for complex Python types (datetime, date, list[dict], set -> multiset)

### Fetch APIs
- `fetchone()` returns next row or `None`
- `fetchmany(size=1)` returns list of rows
- `fetchall()` returns all remaining rows

### Async Workflow
```python
cur.execute("SELECT VALUE count(*) FROM big", mode="async")
# Later
result = cur.get_async_result(timeout=30)
```

## Parameters
- Positional: `cur.execute("SELECT * FROM ds WHERE x > ? AND y = ?", [10, "foo"])`
- Named: `cur.execute("SELECT * FROM ds WHERE x > $min AND y = $name", {"min": 10, "name": "foo"})`
- Complex values are serialized to SQL++ literals

## Exceptions
PEP 249 + extended mapping (see `docs/EXCEPTION_HANDLING.md`):
- Standard: `InterfaceError`, `DatabaseError`, `DataError`, `OperationalError`, `IntegrityError`, `InternalError`, `ProgrammingError`, `NotSupportedError`
- Extended: `HTTPError`, `NetworkError`, `TimeoutError`, `SyntaxError`, `IdentifierError`, `AsyncQueryError`, `HandleError`

All exceptions carry `.context` and support `.to_dict()`.

## Observability
- Query spans include `http.url`, `http.method`, `db.statement.processed` (truncated), `db.params.count`
- Metrics: `query_duration_seconds`, `query_total`, rows fetched
- Logging: structured context with performance checkpoints

## Patterns
### Safe Resource Management
```python
with connect() as conn:
    cur = conn.cursor()
    cur.execute("SELECT VALUE 1")
    print(cur.fetchall())
```

### Error Handling
```python
from src.pyasterix import SyntaxError, NetworkError, TimeoutError, DatabaseError

try:
    cur.execute("SELECT * FROM WHERE broken")
except SyntaxError as e:
    ...
except (NetworkError, TimeoutError) as e:
    ...
except DatabaseError as e:
    ...
```

### Read-only Queries
Use `readonly=True` to reject DDL/DML from the server side:
```python
cur.execute("SELECT VALUE 1", readonly=True)
```

## Tips
- Prefer named params for clarity in complex queries
- Use `pretty=True` during development only
- For large queries, consider `mode="deferred"`/`"async"` + pooling utilities
