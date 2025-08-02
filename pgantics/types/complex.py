import inspect
from functools import cached_property
from typing import Any, Optional, Type, Union

from ..core.exceptions import InvalidType
from ..core.registry import _CUSTOM_TYPE_REGISTRY, get_type_class
from .base import PostgresType, to_postgres_type
from .primitives import (
    CIDR,
    INET,
    JSON,
    JSONB,
    MACADDR,
    MACADDR8,
    BigInt,
    BigSerial,
    Bit,
    BitVarying,
    Boolean,
    Box,
    Bytea,
    Char,
    Circle,
    Date,
    Decimal,
    DoublePrecision,
    Integer,
    Interval,
    Line,
    LSeg,
    Money,
    Numeric,
    Path,
    PGuuid,
    Point,
    Polygon,
    Real,
    Serial,
    SmallInt,
    SmallSerial,
    Text,
    Time,
    Timestamp,
    TimestampTZ,
    TimeTZ,
    TSQuery,
    TSVector,
    VarChar,
)

__all__ = (
    "Array",
    "Range",
    "Domain"
)

ARRAY_TYPES = ARRAY_TYPES = (
    # Numeric types
    SmallInt, Integer, BigInt, Decimal, Numeric, Real, DoublePrecision,
    SmallSerial, Serial, BigSerial, Money,

    # Character types
    VarChar, Char, Text,

    # Boolean
    Boolean,

    # Date/time types
    Date, Time, TimeTZ, Timestamp, TimestampTZ, Interval,

    # UUID and network types
    PGuuid, INET, CIDR, MACADDR, MACADDR8,

    # Geometric types
    Point, Line, LSeg, Box, Path, Polygon, Circle,

    # Bit and binary
    Bit, BitVarying, Bytea,

    # Full-text search
    TSVector, TSQuery,

    # JSON
    JSON, JSONB
)

RANGE_TYPES = (
    Integer,
    BigInt,
    Decimal,
    Numeric,
    Date,
    Timestamp,
    TimestampTZ,
)

class Array(PostgresType):
    """Postgres ARRAY type."""

    def __init__(self, element_type: Union[Type[PostgresType], PostgresType, Any, str]):
        self._element_type = element_type

    @cached_property
    def element_type(self) -> Union[Type[PostgresType], PostgresType]:
        CompositeType = get_type_class('CompositeType')
        element_type: Optional[Union[Type[PostgresType], PostgresType]] = None

        if isinstance(self._element_type, str):
            element_type = get_type_class(self._element_type)

        elif isinstance(self._element_type, type) and issubclass(self._element_type, PostgresType):
            if issubclass(self._element_type, CompositeType):
                element_type = self._element_type
            else:
                element_type = self._element_type()

        elif isinstance(self._element_type, PostgresType):
            element_type = self._element_type

        else:
            element_type = to_postgres_type(self._element_type)

        if not is_array_type(element_type):
            raise InvalidType(f"Element type '{element_type}' is not a valid PostgreSQL array type.")

        return element_type

    def __str__(self) -> str:
        CompositeType = get_type_class('CompositeType')
        EnumType = get_type_class('EnumType')

        base = self.element_type
        depth = 1

        while isinstance(base, Array):
            base = base.element_type
            depth += 1

        if isinstance(base, type) and issubclass(base, (CompositeType, EnumType)):
            return f"{base.Meta.type_name}[]"
        return f"{str(base)}" + "[]" * depth
    
    @classmethod
    def nested(cls, element: Union[Type[PostgresType], PostgresType], depth: int) -> "Array":
        """Create a nested array type."""

        if depth < 1:
            raise ValueError("Depth must be at least 1.")
        
        for _ in range(depth):
            element = Array(element)

        return cls(element)

class Range(PostgresType):
    def __init__(self, element_type: Union[PostgresType, Any, str]):
        self._element_type = element_type

    @cached_property
    def element_type(self) -> PostgresType:
        CompositeType = get_type_class('CompositeType')
        element_type: Optional[Union[Type[PostgresType], PostgresType]] = None

        if isinstance(self._element_type, str):
            element_type = get_type_class(self._element_type)

        elif isinstance(self._element_type, type) and issubclass(self._element_type, PostgresType):
            if issubclass(self._element_type, CompositeType):
                element_type = self._element_type
            else:
                element_type = self._element_type()

        elif isinstance(self._element_type, PostgresType):
            element_type = self._element_type

        else:
            element_type = to_postgres_type(self._element_type)

        if not is_range_type(element_type):
            raise InvalidType(f"Element type '{element_type}' is not a valid PostgreSQL range type.")

        # This can be ignored because is_range_type only allows instances
        return element_type # type: ignore
    
    def __str__(self) -> str:
        element = self.element_type

        # Map to PostgreSQL range type names
        range_mapping = {
            'INTEGER': 'INT4RANGE',
            'BIGINT': 'INT8RANGE', 
            'NUMERIC': 'NUMRANGE',
            'DATE': 'DATERANGE',
            'TIMESTAMP': 'TSRANGE',
            'TIMESTAMP WITH TIME ZONE': 'TSTZRANGE'
        }
        
        element_str = str(element)
        return range_mapping.get(element_str, f"{element_str}RANGE")

class Domain(PostgresType):
    pass

def is_array_type(value: Union[Type[Any], Any]) -> bool:
    """Check if the given type is a PostgreSQL array type."""
    CompositeType = get_type_class('CompositeType')
    EnumType = get_type_class('EnumType')

    all_valid_types = (Array, EnumType, Domain, CompositeType, *_CUSTOM_TYPE_REGISTRY.values(), *ARRAY_TYPES)

    if inspect.isclass(value):
        return issubclass(value, all_valid_types)
    return isinstance(value, all_valid_types)

def is_range_type(value: Union[Type[Any], Any]) -> bool:
    """Check if the given type is a PostgreSQL range type."""
    all_valid_types = RANGE_TYPES

    if inspect.isclass(value):
        return issubclass(value, all_valid_types)
    return isinstance(value, all_valid_types)