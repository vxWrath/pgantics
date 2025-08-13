__all__ = ["pganticsError", "AlreadyRegisteredError", "NotRegisteredError"]

class pganticsError(Exception):
    """Base class for all pgantics exceptions."""

class AlreadyRegisteredError(pganticsError):
    """Exception raised when trying to register an already registered class."""

class NotRegisteredError(pganticsError):
    """Exception raised when trying to access a class that is not registered."""