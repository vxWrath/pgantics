__all__ = (
    "pgdanticError",
    "UnsupportedType",
    "MissingAnnotation",
    "UnknownReferenceError",
    "MetaClassError",
    "ValidationError",
    "ForeignKeyError",
    "ForeignKeyReferenceError",
    "AlreadyRegistered",
    "InvalidType",
)


class pgdanticError(Exception):
    """Base class for all pgdantic-related errors."""
    pass

class UnsupportedType(pgdanticError):
    """Raised when a field type is not supported."""
    
class UnknownReferenceError(pgdanticError):
    """Raised when a reference to a type, field, or table is unknown."""
    pass

class MissingAnnotation(pgdanticError):
    """Raised when a field is missing an annotation."""
    pass

class MetaClassError(pgdanticError):
    """Raised when a class does not define the required Meta class or its attributes."""
    pass

class ValidationError(pgdanticError):
    """Raised when a value fails validation."""
    pass

class ForeignKeyError(ValidationError):
    """Raised when a foreign key validation fails."""
    pass

class ForeignKeyReferenceError(ValidationError):
    """Raised when a foreign key reference is invalid or does not exist."""
    pass

class AlreadyRegistered(pgdanticError):
    """Raised when a type or table is already registered."""
    pass

class InvalidType(pgdanticError):
    """Raised when a field type is invalid."""
    pass