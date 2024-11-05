import requests
import json
import logging
import time
from typing import Optional, Dict, Any, Union, TypedDict
from urllib.parse import urljoin
from requests.exceptions import RequestException, Timeout, ConnectionError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom exception definitions
class AsterixDBError(Exception):
    """Base exception class for AsterixDB client errors."""
    pass

class ConnectionTimeoutError(AsterixDBError):
    """Raised when connection times out."""
    pass

class QueryTimeoutError(AsterixDBError):
    """Raised when query execution times out."""
    pass

class InvalidJSONResponseError(AsterixDBError):
    """Raised when response contains invalid JSON."""
    pass

class QueryExecutionError(AsterixDBError):
    """Raised when query execution fails."""
    pass

class AsterixDBHttpClient:
    """
    Low-level client for communicating with AsterixDB HTTP API endpoints.
    Handles raw HTTP communication and query execution modes.
    """
    
    def __init__(
        self, 
        base_url: str = "http://localhost:19002",
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 0.1,
        max_async_attempts: int = 100,
        async_check_interval: float = 0.5
    ):
        """
        Initialize AsterixDB client.
        
        Args:
            base_url: Base URL of the AsterixDB instance
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            retry_delay: Initial delay between retries (will be exponentially increased)
            max_async_attempts: Maximum number of status checks for async queries
            async_check_interval: Initial time between async status checks
        """
        self.base_url = base_url.rstrip('/')
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.max_async_attempts = max_async_attempts
        self.async_check_interval = async_check_interval
        self._closed = False

        # Configure session with default headers
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        
        logger.info(f"Initialized AsterixDB HTTP client with base URL: {base_url}")

    def _make_request(
        self,
        method: str,
        url: str,
        data: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Make HTTP request with retry logic and error handling.
        
        Args:
            method: HTTP method ('GET' or 'POST')
            url: Request URL
            data: Request payload (will be JSON-encoded)
            **kwargs: Additional request parameters
            
        Returns:
            JSON response as dictionary
            
        Raises:
            ConnectionTimeoutError: If connection times out
            QueryExecutionError: If query fails
            InvalidJSONResponseError: If response isn't valid JSON
        """
        if self._closed:
            raise RuntimeError("Invalid or closed session")
        
        retry_count = 0
        current_delay = self.retry_delay

        while retry_count <= self.max_retries:
            try:
                kwargs['timeout'] = self.timeout
                logger.debug(f"Making {method} request to {url}")
                
                if data is not None:
                    kwargs['json'] = data  # Use json parameter for automatic JSON encoding
                
                response = self.session.request(method, url, **kwargs)
                response.raise_for_status()
                
                try:
                    return response.json()
                except json.JSONDecodeError as e:
                    error_msg = (
                        f"Invalid JSON response: {response.text[:1000]}..."
                        if len(response.text) > 1000
                        else response.text
                    )
                    logger.error(error_msg)
                    raise InvalidJSONResponseError(
                        f"Failed to decode JSON response: {str(e)}\n{error_msg}"
                    ) from e

            except Timeout as e:
                logger.warning(f"Request timeout (attempt {retry_count + 1}): {str(e)}")
                if retry_count == self.max_retries:
                    raise ConnectionTimeoutError(
                        f"Request timed out after {self.max_retries} retries"
                    ) from e

            except ConnectionError as e:
                logger.warning(f"Connection error (attempt {retry_count + 1}): {str(e)}")
                if retry_count == self.max_retries:
                    raise QueryExecutionError(
                        f"Connection failed after {self.max_retries} retries"
                    ) from e

            except RequestException as e:
                logger.error(f"Request failed (attempt {retry_count + 1}): {str(e)}")
                if retry_count == self.max_retries:
                    raise QueryExecutionError(
                        f"Request failed: {str(e)}"
                    ) from e

            retry_count += 1
            time.sleep(current_delay)
            current_delay *= 2  # Exponential backoff
            logger.info(f"Retrying request (attempt {retry_count}/{self.max_retries})")
        
        return {}

    def execute_query(
        self,
        statement: str,
        mode: str = "immediate",
        params: Optional[Dict[str, Any]] = None,
        pretty: bool = False,
        client_context_id: Optional[str] = None,
        dataverse: Optional[str] = None,
        readonly: bool = False
    ) -> Dict[str, Any]:
        """
        Execute a query using the specified mode.
        
        Args:
            statement: Query string to be executed
            mode: Query execution mode ('immediate', 'deferred', 'async')
            params: Additional query parameters
            pretty: Format response in a readable way
            client_context_id: Optional client context ID for tracking
            dataverse: Specific dataverse to use for the query
            readonly: Boolean flag for readonly queries
            
        Returns:
            Query result as a dictionary
            
        Raises:
            QueryExecutionError: If query execution fails
            ConnectionTimeoutError: If connection times out
            InvalidJSONResponseError: If response isn't valid JSON
        """
        if not isinstance(statement, str):
            raise TypeError("Query statement must be a string")
    
        if mode not in ("immediate", "deferred", "async"):
            raise ValueError(f"Invalid mode: {mode}")
                
        endpoint = "/query/service"
        
        # Prepare request data
        data = {
            "statement": statement,
            "mode": mode,
            "pretty": pretty,
            "readonly": readonly
        }
        
        if params:
            data.update(params)
        if client_context_id:
            data["client_context_id"] = client_context_id
        if dataverse:
            data["dataverse"] = dataverse

        logger.info(f"Executing query in {mode} mode")
        logger.debug(f"Query statement: {statement}")
        
        initial_response = self._make_request('POST', urljoin(self.base_url, endpoint), data=data)

        if mode == "immediate":
            return initial_response
        elif mode == "async":
            return self._handle_async_query(initial_response)
        else:
            return initial_response

    # def _handle_non_immediate_query(
    #     self,
    #     initial_response: Dict[str, Any],
    #     mode: str
    # ) -> Dict[str, Any]:
    #     """
    #     Handle deferred and async query responses.
    #     """
    #     # If the response doesn't have a handle but has a successful status,
    #     # it means the query was executed immediately (common for DDL/DML)
    #     if initial_response.get("status") == "success" and "results" in initial_response:
    #         logger.info(f"Query executed immediately despite {mode} mode (common for DDL/DML operations)")
    #         return initial_response

    #     handle = initial_response.get("handle")
    #     if not handle:
    #         error_msg = (
    #             f"No handle received in {mode} response. Response: "
    #             f"{json.dumps(initial_response, indent=2)}"
    #         )
    #         logger.error(error_msg)
    #         raise QueryExecutionError(error_msg)

    #     logger.debug(f"Received handle for {mode} query: {handle}")

    #     try:
    #         if mode == "deferred":
    #             return self._get_query_result(handle)
    #         else:  # async
    #             return self._wait_for_result(handle)
    #     except Exception as e:
    #         logger.error(f"Error handling {mode} query: {str(e)}")
    #         raise

    def _handle_async_query(self, initial_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle async query based on AsterixDB's specific response format.
        """
        # Check if we have a running status and handle
        if initial_response.get("status") != "running" or "handle" not in initial_response:
            logger.error(f"Unexpected initial async response: {initial_response}")
            raise QueryExecutionError("Invalid async query response format")

        status_handle = initial_response["handle"]
        logger.debug(f"Received status handle: {status_handle}")

        # Return the initial response with handle for the calling code to manage the async state
        return {
            "status": "running",
            "handle": status_handle,
            "requestID": initial_response.get("requestID"),
            "metrics": initial_response.get("metrics", {})
        }

    def _get_query_status(self, status_handle: str) -> Dict[str, Any]:
        """
        Check query status using status handle.
        
        Args:
            status_handle: URL handle for checking status
            
        Returns:
            Query status information
        """
        logger.debug(f"Checking query status: {status_handle}")
        status_response = self._make_request('GET', status_handle)
        # If we get a success status and a new handle, it's the result handle
        if status_response.get("status") == "success" and "handle" in status_response:
            return {
                "status": "SUCCESS",
                "handle": status_response["handle"]
            }
        
        return status_response

    def _get_query_result(self, result_handle: str) -> Dict[str, Any]:
        """
        Retrieve query result using result handle.
        
        Args:
            result_handle: URL handle for retrieving results
            
        Returns:
            Query result
        """
        logger.debug(f"Fetching query result: {result_handle}")
        return self._make_request('GET', result_handle)

    def close(self):
        """Close the HTTP session."""
        logger.info("Closing HTTP client session")
        self.session.close()
        self._closed = True

    def __enter__(self):
        if self._closed:
            raise RuntimeError("Cannot reuse closed client")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
