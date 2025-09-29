# DataFrame API Developer Guide

This guide documents the high-level DataFrame API built on top of the PyAsterix DB-API.
It provides a pandas-like fluent interface to build and execute SQL++ queries.

## Quick Start
```python
from src.pyasterix import connect
from src.pyasterix.dataframe import AsterixDataFrame

conn = connect()
df = AsterixDataFrame(conn, "products")
rows = (df
    .select(["id", "name", "price"]) 
    .filter("price > 100")
    .order_by("price", desc=True)
    .execute()
    .result_set)
```

## Core Concepts
- `AsterixDataFrame(connection, dataset)`: binds a dataset and a connection
- Builder pattern collects clauses and compiles to SQL++ at `.execute()`
- Results available in `result_set` after execution

## Operators
### Projection
```python
df.select(["id", "name", "price"])  # or df.select("id")
```

### Filtering
```python
df.filter("price > 100")
# Or using attributes/predicates API if available
```

### Aggregations
```python
df.agg({
  "price": "avg",
  "id": "count"
})
```

### Group By
```python
df.group_by(["category"]).agg({"price": "avg", "id": "count"})
```

### Joins
```python
df.join(other_df, on=("id", "product_id"))
# Or using left_on/right_on
```

### Ordering and Limit
```python
df.order_by("price", desc=True)
df.limit(100)
```

## Execution
- `.execute()` compiles the query and runs it via the underlying `Cursor`
- Observability adds spans/metrics for the whole DataFrame flow

## Validation Helpers
- Identifier checks prevent malformed field/alias names
- Clear `DataError` and `QueryBuildError` on invalid usage

## Error Handling
- `DataFrameError` wraps execution failures with the effective SQL++
- Upstream DB exceptions propagate with context (see `docs/EXCEPTION_HANDLING.md`)

## Patterns
### Progressive Building
```python
df = AsterixDataFrame(conn, "orders")
df = df.filter("status = 'PAID'")
df = df.group_by(["customer_id"]).agg({"amount": "sum"})
res = df.execute().result_set
```

### Reusable Builders
```python
def top_products(df, limit=10):
    return (df
        .select(["id", "name", "price"]) 
        .order_by("price", desc=True)
        .limit(limit))

res = top_products(AsterixDataFrame(conn, "products")).execute().result_set
```

## Observability
- DataFrame execution emits a parent span; cursor execute emits child spans
- Metrics capture result sizes and execution durations

## Tips
- Keep field names validated via helpers
- Chain small transformations for clarity
- Use joins sparingly; consider pre-aggregations when possible
