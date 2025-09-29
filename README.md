# PyAsterix: Python Connector for AsterixDB

PyAsterix is a feature-rich Python library designed for seamless interaction with AsterixDB, a scalable NoSQL database management system. It offers two powerful interfaces: a low-level DB-API compliant interface and a high-level DataFrame API.

## Table of Contents
- [Installation](#installation)
- [Features](#features)
- [Architecture](#architecture)
- [Contributing](#contributing)
- [License](#license)

---

## Installation

Install from PyPI:

```
pip install pyasterix
```

With observability extras (Prometheus + OpenTelemetry):

```
pip install "pyasterix[observability]"
```

Install from source (editable for development):

```
git clone https://github.com/your-org/pyasterix.git
cd pyasterix
python -m venv .venv
".venv"/Scripts/activate
pip install -U pip build twine
pip install -e .
```

## Features
Core Features
- PEP 249 compliant database interface
- Pandas-like DataFrame API
- Support for both synchronous and asynchronous queries
- Comprehensive error handling with custom, context-rich exceptions
- Connection pooling and intelligent connection management
- Native support for AsterixDB data types
- Easy integration with pandas ecosystem
- Built-in observability: metrics (Prometheus), tracing (OpenTelemetry), structured logging

DB-API Features
- Standard cursor interface
- Transaction support (where applicable)
- Parameterized queries
- Multiple result fetch methods

Advanced Features
- Observability (metrics, tracing, logging) with production-ready configuration
- Async query support (status/result handles, pooled polling)
- Connection pool lifecycle management (validation, idle expiry, cleanup thread)
- Error mapping from HTTP/AsterixDB payloads to precise exceptions
- DataFrame API Features
- Intuitive query building
- Method chaining
- Complex aggregations
- Join operations
- Filtering and sorting
- Group by operations
- Direct pandas DataFrame conversion


## Architecture

### Components

Connection Management

- Connection pooling and lifecycle (validation, idle/lifetime expiry, background cleanup)
- Session handling via HTTP sessions
- Query execution including async/deferred modes

Query Building

- SQL++ query generation
- Parameter binding
- Query validation

Result Processing

- Type conversion
- Result caching
- Data streaming

 Observability

- Metrics: query durations, counts, rows fetched, pool gauges, error counters
- Tracing: spans for execute/fetch/async/pool and DataFrame operations (OTel compatible)
- Logging: structured JSON with trace correlation and performance-aware filtering

 Exception Handling

- PEP 249 standard hierarchy + AsterixDB-specific exceptions (HTTPError, NetworkError,
  TimeoutError, SyntaxError, IdentifierError, AsyncQueryError, PoolExhaustedError, etc.)
- Rich error context attached to each exception and `.to_dict()` serialization

## Best Practices

Connection Management

- Use context managers (with statements)
- Close connections explicitly
- Implement connection pooling for web applications and batch services

Query Optimization

- Use appropriate indexes
- Leverage query hints when necessary
- Monitor query performance

Error Handling

- Implement proper exception handling
- Use retry mechanisms for transient failures
- Log errors appropriately
- Prefer catching specific driver exceptions (e.g., SyntaxError, NetworkError, TimeoutError)
- Inspect `.context` on exceptions and leverage `.to_dict()` for structured logging

 Observability

- Enable metrics and tracing in non-prod first; tune sampling in prod
- Export Prometheus metrics and view traces via OTLP/Jaeger
- Use structured logs with correlation IDs for cross-service debugging

## Documentation

- Driver Overview: `docs/DRIVER_OVERVIEW.md`
- DB-API Guide: `docs/DBAPI_GUIDE.md`
- DataFrame Guide: `docs/DATAFRAME_GUIDE.md`
- Observability for Developers: `docs/OBSERVABILITY_FOR_DEVELOPERS.md`
- Exception Handling: `docs/EXCEPTION_HANDLING.md`

## Contributing
- We welcome contributions! Please follow these steps:
    1. Fork the repository
    2. Create a feature branch
    3. Commit your changes
    4. Create a pull request

## License 
- This project is licensed under the MIT License - see the LICENSE file for details.