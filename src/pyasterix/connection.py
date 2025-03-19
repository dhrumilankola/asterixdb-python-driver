import requests
from urllib.parse import urljoin
from .exceptions import NotSupportedError, InterfaceError
from .cursor import Cursor
import logging

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class Connection:
    """
    Represents a connection to the AsterixDB database.
    Manages the HTTP session and provides access to Cursor objects for query execution.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:19002",
        timeout: int = 30,
        max_retries: int = 3,
        retry_delay: float = 0.1
    ):
        """
        Initialize a Connection instance.

        Args:
            base_url: Base URL of the AsterixDB instance.
            timeout: Request timeout in seconds.
            max_retries: Maximum number of retry attempts.
            retry_delay: Initial delay between retries (in seconds).
        """
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._closed = False

        # HTTP session without default headers - we'll set them per request
        self.session = requests.Session()
        
        logger.info(f"Initialized Connection with base URL: {base_url}")

    def cursor(self) -> Cursor:
        """
        Create a new Cursor object for executing queries.

        Returns:
            Cursor: A new Cursor instance.
        
        Raises:
            InterfaceError: If the connection is closed.
        """
        if self._closed:
            raise InterfaceError("Cannot create a cursor on a closed connection.")
        return Cursor(self)

    def commit(self):
        """
        Commit the current transaction.
        Not supported by AsterixDB.
        """
        raise NotSupportedError("AsterixDB does not support transactions.")

    def rollback(self):
        """
        Rollback the current transaction.
        Not supported by AsterixDB.
        """
        raise NotSupportedError("AsterixDB does not support transactions.")

    def close(self):
        """
        Close the connection and the HTTP session.
        """
        if not self._closed:
            self.session.close()
            self._closed = True
            logger.info("Connection closed.")
        else:
            logger.warning("Attempted to close an already closed connection.")

    def __enter__(self):
        """
        Support the context manager protocol for using connections with `with` statements.
        """
        if self._closed:
            raise InterfaceError("Cannot use a closed connection.")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Close the connection when exiting a `with` block.
        """
        self.close()
