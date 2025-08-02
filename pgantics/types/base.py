import datetime
import decimal
import ipaddress
import uuid
from typing import Any, Type, Union, get_args, get_origin

from ..core.exceptions import UnknownReferenceError, UnsupportedType
from ..core.registry import get_type_class, register_type

__all__ = (
    "PostgresType",
)

class PostgresType:
    """Base class for all PostgreSQL types."""

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        register_type(cls)

    def __repr__(self) -> str:
        """Return a string representation of the PostgreSQL type."""
        return f"<PostgresType(name={self.__class__.__name__!r})>"
    
    def __str__(self) -> str:
        """Return a string representation of the PostgreSQL type."""
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement the __str__ method to return a valid SQL string."
        )
    
def to_postgres_type(value: Type[Any]) -> Union[Type[PostgresType], PostgresType]:
    """Convert a Python type to a PostgreSQL type. If return type is a class rather than an instance, it is a composite type."""

    from .primitives import (
        CIDR,
        INET,
        JSONB,
        Boolean,
        Bytea,
        Date,
        Decimal,
        Integer,
        Interval,
        PGuuid,
        Real,
        Text,
        Time,
        Timestamp,
    )

    if value is str:
        return Text()
    if value is int:
        return Integer()
    if value is float:
        return Real()
    if value is bool:
        return Boolean()
    if value is bytes or value is bytearray:
        return Bytea()
    
    if value is decimal.Decimal:
        return Decimal()
    
    if value is datetime.date:
        return Date()
    if value is datetime.time:
        return Time()
    if value is datetime.datetime:
        return Timestamp()
    if value is datetime.timedelta:
        return Interval()

    if value is uuid.UUID:
        return PGuuid()

    if value is ipaddress.IPv4Address or value is ipaddress.IPv6Address:
        return INET()
    if value is ipaddress.IPv4Network or value is ipaddress.IPv6Network or value is ipaddress.IPv4Interface or value is ipaddress.IPv6Interface:
        return CIDR()

    try:
        CompositeType = get_type_class('CompositeType')
        if issubclass(value, CompositeType):
            return value
    except UnknownReferenceError:
        pass

    from .complex import Array
    origin = get_origin(value)

    if origin is list:
        args = get_args(value)
        
        if len(args) != 1:
            raise UnsupportedType(
                f"Array type must have exactly one type argument, got {len(args)}"
            )
        
        return Array(args[0])
    
    if origin is tuple:
        args = get_args(value)

        if len(args) == 1 or (len(args) == 2 and args[1] is Ellipsis):
            return Array(args[0])
        else:
            raise UnsupportedType(
                f"Tuple type must have one argument with an optional second argument that must be an ellipsis"
            )
    
    if origin is dict:
        return JSONB()
    
    raise UnsupportedType(
        f"Could not convert '{value}' to a PostgreSQL type"
    )