import time
import json
from urllib.parse import urljoin
import datetime
from .exceptions import DatabaseError, InterfaceError, NotSupportedError

class Cursor:
    """
    A Cursor object represents a database cursor, which is used to execute queries and fetch results.
    """

    def __init__(self, connection):
        """
        Initialize a Cursor instance.

        Args:
            connection: A reference to the Connection object.
        """
        self.connection = connection
        self.results = []
        self.description = None  # Placeholder for column metadata (optional)
        self.rowcount = -1       # Number of rows affected by last operation (-1 if not applicable)
        self._closed = False


    def execute(self, query, params=None, mode="immediate", pretty=False, readonly=False):
        """Execute a SQL++ query with parameter substitution."""
        if self._closed:
            raise InterfaceError("Cannot execute a query on a closed cursor.")

        if mode not in ("immediate", "deferred", "async"):
            raise ValueError(f"Invalid execution mode: {mode}")

        # Process query with parameters if provided
        processed_query = query
        if params:
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

        # Make HTTP request using the connection's session
        try:
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
            
            # For debugging
            if response.status_code >= 400:
                print(f"DEBUG: Request failed with status {response.status_code}")
                print(f"DEBUG: Request URL: {url}")
                print(f"DEBUG: Request payload: {payload}")
                print(f"DEBUG: Response content: {response.text}")
                
            response.raise_for_status()
        except Exception as e:
            raise DatabaseError(f"Query execution failed: {e}")

        result_data = response.json()

        # Handle asynchronous queries
        if mode == "async" and "handle" in result_data:
            self.results = result_data  # Preserve the full response for async queries
        else:
            self.results = result_data.get("results", [])

        self.rowcount = len(self.results) if isinstance(self.results, list) else -1

        # Set description (optional metadata)
        self.description = self._parse_description(result_data)

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

    def _handle_async_query(self, initial_response: dict):
        """
        Handle asynchronous query execution.

        Args:
            initial_response: Response from the initial async query request.
        """
        handle = initial_response.get("handle")
        if not handle:
            raise DatabaseError("Async query did not return a handle.")

        status_url = urljoin(self.connection.base_url, handle)
        attempts = 0

        while attempts < self.connection.max_retries:
            time.sleep(self.connection.retry_delay)
            status_response = self.connection.session.get(status_url)
            status_data = status_response.json()

            if status_data.get("status") == "success":
                self.results = status_data.get("results", [])
                self.rowcount = len(self.results)
                return
            elif status_data.get("status") == "error":
                raise DatabaseError(f"Async query failed: {status_data.get('errors')}")

            attempts += 1

        raise DatabaseError("Async query did not complete within the retry limit.")
    
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
            raise DatabaseError("No handle provided for status check.")

        status_url = urljoin(self.connection.base_url, handle)
        response = self.connection.session.get(status_url, timeout=self.connection.timeout)
        try:
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise DatabaseError(f"Failed to retrieve async query status: {e}")

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
            raise DatabaseError("No handle provided for result fetching.")

        result_url = urljoin(self.connection.base_url, handle)
        response = self.connection.session.get(result_url, timeout=self.connection.timeout)
        try:
            response.raise_for_status()
            return response.json()
        except Exception as e:
            raise DatabaseError(f"Failed to retrieve async query result: {e}")

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
        if not self.results:
            return None
        return self.results.pop(0)

    def fetchmany(self, size: int = 1):
        """
        Fetch the next `size` rows of a query result set.

        Args:
            size: Number of rows to fetch.

        Returns:
            A list of rows.
        """
        if not self.results:
            return []
        rows = self.results[:size]
        self.results = self.results[size:]
        return rows

    def fetchall(self):
        """
        Fetch all (remaining) rows of a query result set.

        Returns:
            A list of all remaining rows.
        """
        rows = self.results
        self.results = []
        return rows

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
