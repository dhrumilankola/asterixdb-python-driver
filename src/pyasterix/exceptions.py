class AsterixError(Exception):
    """Base exception class for AsterixDB client errors."""
    pass

class ConnectionError(AsterixError):
    """Raised when connection fails."""
    pass

class QueryError(AsterixError):
    """Raised when query execution fails."""
    pass

class ValidationError(AsterixError):
    """Raised when input validation fails."""
    pass

class TypeMappingError(AsterixError):
    """Raised when type mapping between Python and AsterixDB fails."""
    pass

class QueryBuildError(AsterixError):
    """Raised when query building fails."""
    pass