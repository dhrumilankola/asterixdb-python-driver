import time
import json
from urllib.parse import urljoin
import datetime
from typing import Optional, Any
from .exceptions import (
    DatabaseError, InterfaceError, NotSupportedError, 
    ErrorMapper, AsyncErrorMapper, HandleError, 
    AsyncTimeoutError, ResultProcessingError, TimeoutError
)
from .observability import ObservabilityManager

class Cursor:
    """
    A Cursor object represents a database cursor, which is used to execute queries and fetch results.
    """

    def __init__(self, connection, observability: Optional[ObservabilityManager] = None):
        """
        Initialize a Cursor instance.

        Args:
            connection: A reference to the Connection object.
            observability: Optional observability manager for metrics and tracing.
        """
        self.connection = connection
        self.results = []
        self.description = None  # Placeholder for column metadata (optional)
        self.rowcount = -1       # Number of rows affected by last operation (-1 if not applicable)
        self._closed = False
        self.observability = observability
        
        # Get structured logger
        if self.observability:
            self.logger = self.observability.get_logger("pyasterix.cursor")
        else:
            import logging
            self.logger = logging.getLogger("pyasterix.cursor")

    def _noop_context(self):
        """No-operation context manager for when tracing is disabled."""
        class NoOpContext:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc_val, exc_tb):
                pass
        return NoOpContext()

    def execute(self, query, params=None, mode="immediate", pretty=False, readonly=False):
        """Execute a SQL++ query with parameter substitution."""
        if self._closed:
            raise InterfaceError("Cannot execute a query on a closed cursor.")

        if mode not in ("immediate", "deferred", "async"):
            raise ValueError(f"Invalid execution mode: {mode}")

        # Record query start time for metrics
        start_time = time.time()
        query_labels = {
            "mode": mode,
            "readonly": str(readonly),
            "service": "asterixdb-client"
        }

        # Create distributed tracing span for the entire operation
        span = None
        if self.observability:
            span = self.observability.create_database_span(
                operation="query.execute",
                query=query,
                mode=mode,
                readonly=readonly,
                pretty=pretty
            )

        # Create performance logger for detailed timing
        perf_logger = None
        if self.observability:
            perf_logger = self.observability.create_performance_logger("query_execution")
            perf_logger.start(query_hash=hash(query) % 10000, mode=mode)

        try:
            with span if span else self._noop_context():
                # Process query with parameters if provided
                processed_query = query
                if params:
                    if perf_logger:
                        perf_logger.checkpoint("parameter_processing_start", param_count=len(params))
                    
                    # Determine if we need client-side parameter substitution
                    needs_substitution = False
                    if isinstance(params, (list, tuple)):
                        # Check for complex parameters (dict or list of dicts)
                        for param in params:
                            if isinstance(param, (dict, list, tuple)) or "?" in query:
                                needs_substitution = True
                                break
                    elif isinstance(params, dict):
                        needs_substitution = True

                    if needs_substitution:
                        processed_query = self._process_query_params(query, params)
                        # Clear params since they're now in the query string
                        params = None
                    
                    if perf_logger:
                        perf_logger.checkpoint("parameter_processing_complete", 
                                             needs_substitution=needs_substitution)
                        
                # Update span with processed query
                if span and hasattr(span, 'set_attribute'):
                    span.set_attribute("db.statement.processed", processed_query[:500])
                    if params:
                        span.set_attribute("db.params.count", len(params) if isinstance(params, (list, tuple)) else len(params.keys()))
                    
                # Prepare query payload as form data
                payload = {
                    "statement": processed_query,
                    "mode": mode,
                    "pretty": "true" if pretty else "false",
                    "readonly": "true" if readonly else "false"
                }
                
                # Handle remaining parameters via AsterixDB's parameter mechanism
                if params:
                    if isinstance(params, list) or isinstance(params, tuple):
                        payload["args"] = json.dumps(params)
                    elif isinstance(params, dict):
                        for key, value in params.items():
                            clean_key = key[1:] if key.startswith('$') else key
                            payload[f"${clean_key}"] = json.dumps(value)
                
                url = urljoin(self.connection.base_url, "/query/service")
                
                # Add HTTP-level span attributes
                if span and hasattr(span, 'set_attribute'):
                    span.set_attribute("http.url", url)
                    span.set_attribute("http.method", "POST")

                # Make HTTP request using the connection's session
                try:
                    if perf_logger:
                        perf_logger.checkpoint("http_request_start", 
                                             payload_size=len(str(payload)),
                                             url=url)
                    
                    # Set appropriate headers for form data
                    headers = {
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Accept": "application/json"
                    }
                    
                    response = self.connection.session.post(
                        url, 
                        data=payload,
                        headers=headers,
                        timeout=self.connection.timeout
                    )
                    
                    if perf_logger:
                        perf_logger.checkpoint("http_response_received", 
                                             status_code=response.status_code,
                                             response_size=len(response.content))
                    
                    # Record HTTP response in span
                    if span and hasattr(span, 'set_attribute'):
                        span.set_attribute("http.status_code", response.status_code)
                    
                    # For debugging
                    if response.status_code >= 400:
                        print(f"DEBUG: Request failed with status {response.status_code}")
                        print(f"DEBUG: Request URL: {url}")
                        print(f"DEBUG: Request payload: {payload}")
                        print(f"DEBUG: Response content: {response.text}")
                        
                    response.raise_for_status()
                    
                except Exception as e:
                    # Record error in performance logger
                    if perf_logger:
                        perf_logger.error(e, url=url, timeout=self.connection.timeout)
                    
                    # Record error in span
                    if span and self.observability:
                        self.observability.record_span_exception(span, e)
                    
                    # Record error metrics
                    error_labels = {**query_labels, "error_type": type(e).__name__, "status": "error"}
                    if self.observability:
                        self.observability.increment_query_count(**error_labels)
                        self.observability.record_connection_error(**error_labels)
                    
                    self.logger.error("Query execution failed", extra={
                        "query": processed_query[:100] + "..." if len(processed_query) > 100 else processed_query,
                        "mode": mode,
                        "readonly": readonly,
                        "pretty": pretty,
                        "error_type": type(e).__name__,
                        "error_message": str(e),
                        "url": url,
                        "timeout": self.connection.timeout,
                        "connection_id": id(self.connection)
                    })
                    
                    # Use enhanced error mapping
                    if hasattr(e, 'response'):
                        # HTTP error with response object
                        context = {
                            'query': processed_query[:200],
                            'mode': mode,
                            'url': url,
                            'timeout': self.connection.timeout
                        }
                        raise ErrorMapper.from_http_response(e.response, context)
                    else:
                        # Network or other error
                        context = {
                            'query': processed_query[:200],
                            'mode': mode,
                            'url': url,
                            'timeout': self.connection.timeout,
                            'operation': 'query_execution'
                        }
                        raise ErrorMapper.from_network_error(e, context)

                if perf_logger:
                    perf_logger.checkpoint("response_parsing_start")
                
                # Enhanced JSON parsing with error handling
                try:
                    result_data = response.json()
                except (json.JSONDecodeError, ValueError) as e:
                    raise ErrorMapper.from_json_error(e, response.text)

                # Handle asynchronous queries with enhanced support
                if mode == "async":
                    if "handle" in result_data:
                        # Store full response for async handling
                        self.results = result_data
                        if span and hasattr(span, 'set_attribute'):
                            span.set_attribute("db.async.handle", result_data.get("handle"))
                            span.set_attribute("db.async.status", result_data.get("status", "unknown"))
                        if perf_logger:
                            perf_logger.checkpoint("async_handle_extracted", 
                                                 handle=result_data.get("handle"),
                                                 initial_status=result_data.get("status"))
                        
                        # For async queries, we can optionally auto-poll here
                        # This maintains backward compatibility while allowing pool optimization
                        if hasattr(self.connection, '_auto_poll_async') and self.connection._auto_poll_async:
                            self._handle_async_query(result_data)
                    else:
                        # Immediate result in async mode (query completed quickly)
                        self.results = result_data.get("results", [])
                        if span and hasattr(span, 'set_attribute'):
                            span.set_attribute("db.async.completed_immediately", True)
                else:
                    self.results = result_data.get("results", [])

                self.rowcount = len(self.results) if isinstance(self.results, list) else -1

                # Set description (optional metadata)
                self.description = self._parse_description(result_data)
                
                if perf_logger:
                    perf_logger.checkpoint("response_parsing_complete", 
                                         rows_parsed=self.rowcount)
                
                # Record span success and final attributes
                if span and self.observability:
                    span.set_attribute("db.rows.affected", self.rowcount)
                    span.set_attribute("db.response.size", len(str(result_data)))
                    self.observability.set_span_success(span)
                
                # Record successful execution metrics
                execution_time = time.time() - start_time
                success_labels = {**query_labels, "status": "success"}
                
                if self.observability:
                    self.observability.record_query_duration(execution_time, **success_labels)
                    self.observability.increment_query_count(**success_labels)
                    if self.rowcount > 0:
                        self.observability.increment_rows_fetched(self.rowcount, **success_labels)
                
                # Complete performance logging
                if perf_logger:
                    perf_logger.complete(success=True, 
                                       rows_affected=self.rowcount,
                                       query_length=len(processed_query),
                                       response_size=len(str(result_data)))
                
                self.logger.info("Query executed successfully", extra={
                    "duration_seconds": execution_time,
                    "rows_affected": self.rowcount,
                    "mode": mode,
                    "readonly": readonly,
                    "pretty": pretty,
                    "query_hash": hash(processed_query) % 10000,
                    "query_length": len(processed_query),
                    "response_size": len(str(result_data)),
                    "connection_id": id(self.connection),
                    "async_query": mode == "async",
                    "has_results": self.rowcount > 0
                })
            
        except Exception as e:
            # Complete performance logging with error
            if perf_logger and not isinstance(e, DatabaseError):
                perf_logger.error(e)
            
            # Record error in span if not already recorded
            if span and self.observability and not isinstance(e, DatabaseError):
                self.observability.record_span_exception(span, e)
            
            # Record error metrics if not already recorded
            execution_time = time.time() - start_time
            error_labels = {**query_labels, "error_type": type(e).__name__, "status": "error"}
            
            if self.observability and not isinstance(e, DatabaseError):
                self.observability.record_query_duration(execution_time, **error_labels)
                self.observability.increment_query_count(**error_labels)
            
            raise  # Re-raise the exception

    def _process_query_params(self, query, params):
        """Process query string with parameters."""
        if not params:
            return query
            
        if not isinstance(params, (list, tuple)):
            # Handle single parameter case
            params = [params]

        # Count placeholders to validate parameter count
        placeholder_count = query.count('?')
        if placeholder_count != len(params):
            raise ValueError(f"Number of parameters ({len(params)}) does not match number of placeholders ({placeholder_count})")
        
        # Process parameters one by one
        parts = []
        last_end = 0
        
        for i, param in enumerate(params):
            placeholder_pos = query.find('?', last_end)
            if placeholder_pos == -1:
                break
                
            # Add everything up to placeholder
            parts.append(query[last_end:placeholder_pos])
            
            # Add serialized parameter
            serialized = self._serialize_parameter(param)
            parts.append(serialized)
            
            last_end = placeholder_pos + 1
        
        # Add any remaining part of the query
        if last_end < len(query):
            parts.append(query[last_end:])
        
        return ''.join(parts)

    def get_async_result(self, timeout: Optional[float] = None) -> Any:
        """
        Get the result of an async query that was executed earlier.
        
        This method allows manual control over async query polling,
        which is useful when working with connection pools.
        
        Args:
            timeout: Maximum time to wait for result (in seconds)
            
        Returns:
            Query results
            
        Raises:
            DatabaseError: If query failed or timed out
        """
        if not isinstance(self.results, dict) or "handle" not in self.results:
            raise HandleError("No async query handle available")
        
        return self._handle_async_query(self.results, timeout)
    
    def _handle_async_query(self, initial_response: dict, timeout: Optional[float] = None):
        """
        Handle asynchronous query execution with comprehensive tracing.

        Args:
            initial_response: Response from the initial async query request.
            timeout: Optional timeout for the async operation
        """
        handle = initial_response.get("handle")
        if not handle:
            raise HandleError("Async query did not return a handle.")

        # Use provided timeout or fall back to connection retry logic
        if timeout is not None:
            start_time = time.time()
            max_retries = int(timeout / (self.connection.retry_delay or 0.1)) + 1
        else:
            start_time = None
            max_retries = self.connection.max_retries

        # Create span for async polling operations
        span = None
        if self.observability:
            span = self.observability.create_database_span(
                operation="query.async.poll",
                handle=handle,
                max_retries=max_retries,
                timeout=timeout
            )

        try:
            with span if span else self._noop_context():
                status_url = urljoin(self.connection.base_url, handle)
                attempts = 0

                if span and hasattr(span, 'set_attribute'):
                    span.set_attribute("db.async.handle", handle)
                    span.set_attribute("db.async.status_url", status_url)
                    span.set_attribute("db.async.max_attempts", max_retries)
                    if timeout:
                        span.set_attribute("db.async.timeout", timeout)

                while attempts < max_retries:
                    # Check timeout if specified
                    if timeout and start_time and (time.time() - start_time) > timeout:
                        timeout_error = AsyncTimeoutError(
                            f"Async query timeout after {timeout}s",
                            timeout_duration=timeout,
                            operation_type="async_query_polling",
                            context={'handle': handle, 'attempts': attempts}
                        )
                        if span and self.observability:
                            self.observability.record_span_exception(span, timeout_error)
                        raise timeout_error
                    # Create child span for each polling attempt
                    poll_span = None
                    if self.observability:
                        poll_span = self.observability.start_span(
                            f"pyasterix.async.poll.attempt",
                            kind="CLIENT",
                            attempt=attempts + 1,
                            handle=handle
                        )

                    try:
                        with poll_span if poll_span else self._noop_context():
                            time.sleep(self.connection.retry_delay)
                            
                            if poll_span and hasattr(poll_span, 'set_attribute'):
                                poll_span.set_attribute("db.async.attempt", attempts + 1)
                                poll_span.set_attribute("db.async.delay", self.connection.retry_delay)
                            
                            status_response = self.connection.session.get(status_url)
                            status_data = status_response.json()
                            
                            if poll_span and hasattr(poll_span, 'set_attribute'):
                                poll_span.set_attribute("http.status_code", status_response.status_code)
                                poll_span.set_attribute("db.async.query_status", status_data.get("status", "unknown"))

                            if status_data.get("status") == "success":
                                self.results = status_data.get("results", [])
                                self.rowcount = len(self.results)
                                
                                # Update spans with success
                                if poll_span and hasattr(poll_span, 'set_attribute'):
                                    poll_span.set_attribute("db.async.result", "completed")
                                    poll_span.set_attribute("db.rows.returned", self.rowcount)
                                    
                                if span and hasattr(span, 'set_attribute'):
                                    span.set_attribute("db.async.final_status", "success")
                                    span.set_attribute("db.async.total_attempts", attempts + 1)
                                    span.set_attribute("db.rows.returned", self.rowcount)
                                
                                if self.observability:
                                    self.observability.set_span_success(poll_span)
                                    self.observability.set_span_success(span)
                                
                                return
                                
                            elif status_data.get("status") == "error":
                                # Use AsyncErrorMapper for proper error handling
                                error = AsyncErrorMapper.from_async_status(status_data, handle)
                                
                                if poll_span and hasattr(poll_span, 'set_attribute'):
                                    poll_span.set_attribute("db.async.result", "error")
                                    poll_span.set_attribute("db.async.error", str(status_data.get('errors')))
                                
                                if self.observability:
                                    self.observability.record_span_exception(poll_span, error)
                                    self.observability.record_span_exception(span, error)
                                
                                raise error
                            
                            # Query still in progress
                            if poll_span and hasattr(poll_span, 'set_attribute'):
                                poll_span.set_attribute("db.async.result", "in_progress")
                            
                            if self.observability:
                                self.observability.set_span_success(poll_span)

                    except Exception as e:
                        if poll_span and self.observability:
                            self.observability.record_span_exception(poll_span, e)
                        raise

                    attempts += 1

                # Exceeded retry limit
                if timeout:
                    timeout_error = AsyncTimeoutError(
                        f"Async query timeout after {timeout}s and {attempts} attempts",
                        timeout_duration=timeout,
                        operation_type="async_query_polling",
                        context={'handle': handle, 'total_attempts': attempts}
                    )
                else:
                    timeout_error = AsyncTimeoutError(
                        f"Async query did not complete within {max_retries} retry attempts",
                        operation_type="async_query_polling",
                        context={'handle': handle, 'max_retries': max_retries, 'total_attempts': attempts}
                    )
                
                if span and hasattr(span, 'set_attribute'):
                    span.set_attribute("db.async.final_status", "timeout")
                    span.set_attribute("db.async.total_attempts", attempts)
                    if timeout:
                        span.set_attribute("db.async.exceeded_timeout", True)
                
                if self.observability:
                    self.observability.record_span_exception(span, timeout_error)
                
                raise timeout_error
                
        except Exception as e:
            if span and self.observability:
                self.observability.record_span_exception(span, e)
            raise
    
    def _serialize_parameter(self, param):
        """Serialize a parameter value for SQL++ query inclusion."""
        if param is None:
            return "null"
        elif isinstance(param, bool):
            return "true" if param else "false"
        elif isinstance(param, (int, float)):
            return str(param)
        elif isinstance(param, str):
            # Escape single quotes
            escaped = param.replace("'", "''")
            return f"'{escaped}'"
        elif isinstance(param, (list, tuple)):
            if all(isinstance(item, dict) for item in param):
                # For lists of objects in inserts
                serialized_items = [self._serialize_dict(item) for item in param]
                return f"[{', '.join(serialized_items)}]"
            else:
                # Regular array
                serialized_items = [self._serialize_parameter(item) for item in param]
                return f"[{', '.join(serialized_items)}]"
        elif isinstance(param, dict):
            return self._serialize_dict(param)
        elif isinstance(param, datetime.datetime):
            # Format as AsterixDB datetime
            iso_format = param.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            return f"datetime('{iso_format}')"
        elif isinstance(param, datetime.date):
            return f"date('{param.strftime('%Y-%m-%d')}')"
        elif isinstance(param, datetime.time):
            return f"time('{param.strftime('%H:%M:%S.%f')[:-3]}Z')"
        elif isinstance(param, set):
            # Format as AsterixDB multiset
            serialized_items = [self._serialize_parameter(item) for item in param]
            return f"{{ {', '.join(serialized_items)} }}"
        else:
            # Default fallback
            return f"'{str(param)}'"

    def _serialize_dict(self, d):
        """
        Serialize a dictionary to AsterixDB object syntax.
        
        Args:
            d: The dictionary to serialize
            
        Returns:
            A string in AsterixDB object syntax
        """
        parts = []
        for key, value in d.items():
            serialized_key = f'"{key}"'
            serialized_value = self._serialize_parameter(value)
            parts.append(f"{serialized_key}: {serialized_value}")
        
        return f"{{{', '.join(parts)}}}"
    
    def _get_query_status(self, handle: str) -> dict:
        """
        Check the status of an asynchronous query.

        Args:
            handle: The query handle returned from the async query.

        Returns:
            A dictionary containing the query's status.

        Raises:
            DatabaseError: If the query fails or an unexpected status is returned.
        """
        if not handle:
            raise HandleError("No handle provided for status check.")

        status_url = urljoin(self.connection.base_url, handle)
        response = self.connection.session.get(status_url, timeout=self.connection.timeout)
        try:
            response.raise_for_status()
            return response.json()
        except Exception as e:
            context = {'handle': handle, 'operation': 'status_check'}
            if hasattr(e, 'response'):
                raise ErrorMapper.from_http_response(e.response, context)
            else:
                raise ErrorMapper.from_network_error(e, context)

    def _get_query_result(self, handle: str) -> dict:
        """
        Fetch the result of a completed asynchronous query.

        Args:
            handle: The query handle for fetching results.

        Returns:
            A dictionary containing the query's final result.

        Raises:
            DatabaseError: If the result fetching fails.
        """
        if not handle:
            raise HandleError("No handle provided for result fetching.")

        result_url = urljoin(self.connection.base_url, handle)
        response = self.connection.session.get(result_url, timeout=self.connection.timeout)
        try:
            response.raise_for_status()
            return response.json()
        except Exception as e:
            context = {'handle': handle, 'operation': 'result_fetch'}
            if hasattr(e, 'response'):
                raise ErrorMapper.from_http_response(e.response, context)
            else:
                raise ErrorMapper.from_network_error(e, context)

    def _parse_description(self, result_data: dict):
        """
        Parse column metadata from the query result (if available).

        Args:
            result_data: The result data from the query.

        Returns:
            List of column metadata (or None if not applicable).
        """
        # Placeholder for extracting column metadata
        # Modify this method if the AsterixDB API provides such metadata
        return None

    def fetchone(self):
        """
        Fetch the next row of a query result set.

        Returns:
            The next row, or None if no more data is available.
        """
        # Create span for fetch operation
        span = None
        if self.observability:
            span = self.observability.create_database_span(
                operation="fetch.one",
                rows_available=len(self.results) if self.results else 0
            )
        
        try:
            with span if span else self._noop_context():
                if not self.results:
                    if span and hasattr(span, 'set_attribute'):
                        span.set_attribute("db.fetch.result", "empty")
                    return None
                
                row = self.results.pop(0)
                
                # Update span with fetch results
                if span and hasattr(span, 'set_attribute'):
                    span.set_attribute("db.fetch.result", "success")
                    span.set_attribute("db.rows.fetched", 1)
                    span.set_attribute("db.rows.remaining", len(self.results))
                
                # Record row fetch metrics
                if self.observability and row is not None:
                    self.observability.increment_rows_fetched(1, operation="fetchone", service="asterixdb-client")
                    self.observability.set_span_success(span)
                
                return row
                
        except Exception as e:
            if span and self.observability:
                self.observability.record_span_exception(span, e)
            raise

    def fetchmany(self, size: int = 1):
        """
        Fetch the next `size` rows of a query result set.

        Args:
            size: Number of rows to fetch.

        Returns:
            A list of rows.
        """
        # Create span for fetch operation
        span = None
        if self.observability:
            span = self.observability.create_database_span(
                operation="fetch.many",
                fetch_size=size,
                rows_available=len(self.results) if self.results else 0
            )
        
        try:
            with span if span else self._noop_context():
                if not self.results:
                    if span and hasattr(span, 'set_attribute'):
                        span.set_attribute("db.fetch.result", "empty")
                    return []
                
                rows = self.results[:size]
                self.results = self.results[size:]
                
                # Update span with fetch results
                if span and hasattr(span, 'set_attribute'):
                    span.set_attribute("db.fetch.result", "success")
                    span.set_attribute("db.rows.fetched", len(rows))
                    span.set_attribute("db.rows.remaining", len(self.results))
                
                # Record row fetch metrics
                if self.observability and rows:
                    self.observability.increment_rows_fetched(len(rows), operation="fetchmany", service="asterixdb-client")
                    self.observability.set_span_success(span)
                
                return rows
                
        except Exception as e:
            if span and self.observability:
                self.observability.record_span_exception(span, e)
            raise

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result set.

        Returns:
            A list of all remaining rows.
        """
        # Create span for fetch operation
        span = None
        if self.observability:
            span = self.observability.create_database_span(
                operation="fetch.all",
                rows_available=len(self.results) if self.results else 0
            )
        
        try:
            with span if span else self._noop_context():
                rows = self.results
                self.results = []
                
                # Update span with fetch results
                if span and hasattr(span, 'set_attribute'):
                    if rows:
                        span.set_attribute("db.fetch.result", "success")
                        span.set_attribute("db.rows.fetched", len(rows))
                    else:
                        span.set_attribute("db.fetch.result", "empty")
                        span.set_attribute("db.rows.fetched", 0)
                    span.set_attribute("db.rows.remaining", 0)
                
                # Record row fetch metrics
                if self.observability and rows:
                    self.observability.increment_rows_fetched(len(rows), operation="fetchall", service="asterixdb-client")
                    self.observability.set_span_success(span)
                
                return rows
                
        except Exception as e:
            if span and self.observability:
                self.observability.record_span_exception(span, e)
            raise

    def close(self):
        """
        Close the cursor.
        """
        self._closed = True

    def __iter__(self):
        """
        Allow the cursor to be used as an iterator.
        """
        return iter(self.fetchall())
