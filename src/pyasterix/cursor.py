import time
from urllib.parse import urljoin
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
        """
        Execute a SQL++ query.
        """
        if self._closed:
            raise InterfaceError("Cannot execute a query on a closed cursor.")

        if mode not in ("immediate", "deferred", "async"):
            raise ValueError(f"Invalid execution mode: {mode}")

        # Prepare query payload
        payload = {
            "statement": query,
            "mode": mode,
            "pretty": pretty,
            "readonly": readonly
        }
        if params:
            payload.update(params)

        url = urljoin(self.connection.base_url, "/query/service")

        # Make HTTP request using the connection's session
        try:
            response = self.connection.session.post(
                url, json=payload, timeout=self.connection.timeout
            )
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
