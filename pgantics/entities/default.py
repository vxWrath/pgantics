import decimal
import uuid
from typing import Any, Dict, List, Optional, Type, Union

import orjson

from ..core.registry import get_type_class
from ..types.base import PostgresType
from ..types.primitives import JSON, JSONB


class _DefaultValue:
    """Postgres DEFAULT constraint."""

    def __init__(self, value: Union[
            None,
            str, bool, int, float,
            decimal.Decimal,  # For NUMERIC/DECIMAL columns
            uuid.UUID,        # For UUID columns
            bytes,            # For BYTEA columns
            List[Any], Dict[Any, Any]        # For JSON/JSONB columns
        ], /, *, is_expression: bool = False
    ):
        self.value = value
        self.is_expression = is_expression

    def __str__(self) -> str:
        if self.is_expression:
            if not isinstance(self.value, str):
                raise TypeError(f"Expression value must be a string, got '{type(self.value).__name__}'.")
            return self.value
        elif self.value is None:
            return "NULL"
        elif isinstance(self.value, str):
            return f"'{self.value.replace("'", "''")}'"
        elif isinstance(self.value, bool):
            return 'TRUE' if self.value else 'FALSE'
        elif isinstance(self.value, (int, float)):
            return str(self.value)
        elif isinstance(self.value, decimal.Decimal):
            return str(self.value)
        elif isinstance(self.value, uuid.UUID):
            return f"'{self.value}'"
        elif isinstance(self.value, (bytes, bytearray, memoryview)):
            return f"'\\x{self.value.hex()}'"
        elif isinstance(self.value, (list, dict)):
            return f"'{orjson.dumps(self.value).decode()}'"
        else:
            return f"'{self.value.isoformat()}'"
        
    def __repr__(self) -> str:
        if self.is_expression:
            return f"<DefaultValue expression={self.value!r}>"
        return f"<DefaultValue value={self.value!r}>"
        
    def to_sql(self) -> str:
        """Convert the default value to a SQL string."""
        return f"DEFAULT {str(self)}"

def Null() -> _DefaultValue:
    """Postgres DEFAULT constraint for NULL values."""
    return _DefaultValue(None, is_expression=False)

def String(value: str) -> _DefaultValue:
    """Postgres DEFAULT constraint for string values."""
    return _DefaultValue(value, is_expression=False)

def Bool(value: bool) -> _DefaultValue:
    """Postgres DEFAULT constraint for boolean values."""
    return _DefaultValue(value, is_expression=False)

def Integer(value: int) -> _DefaultValue:
    """Postgres DEFAULT constraint for integer values."""
    return _DefaultValue(value, is_expression=False)

def Float(value: float) -> _DefaultValue:
    """Postgres DEFAULT constraint for float values."""
    return _DefaultValue(value, is_expression=False)

def Decimal(value: decimal.Decimal) -> _DefaultValue:
    """Postgres DEFAULT constraint for decimal values."""
    return _DefaultValue(value, is_expression=False)

def Bytes(value: Union[bytes, bytearray, memoryview]) -> _DefaultValue:
    """Postgres DEFAULT constraint for byte values."""
    if isinstance(value, (bytearray, memoryview)):
        value = bytes(value)
    return _DefaultValue(value, is_expression=False)

def Expression(value: str) -> _DefaultValue:
    """Postgres DEFAULT constraint for expression values."""
    return _DefaultValue(value, is_expression=True)

def Json(value: Union[List[Any], Dict[Any, Any]]) -> _DefaultValue:
    """Postgres DEFAULT constraint for JSON values."""
    return Cast(value, JSON())

def JsonB(value: Union[List[Any], Dict[Any, Any]]) -> _DefaultValue:
    """Postgres DEFAULT constraint for JSONB values."""
    return Cast(value, JSONB())

def Array(*values: Any, cast: Optional[Union[Type[PostgresType], PostgresType]]=None) -> _DefaultValue:
    """Postgres DEFAULT constraint for array values."""
    if not values:
        return Expression("'{}'")
    
    formatted_values = []
    for v in values:
        if isinstance(v, _DefaultValue):
            formatted_values.append(str(v))
        elif isinstance(v, str):
            formatted_values.append(f"'{v.replace("'", "''")}'")
        elif v is None:
            formatted_values.append("NULL")
        else:
            formatted_values.append(str(v))
    
    array_str = "{" + ",".join(formatted_values) + "}"

    if cast:
        return Cast(array_str, cast)
    return Expression(f"'{array_str}'")

def Now() -> _DefaultValue:
    """Postgres DEFAULT constraint for current time."""
    return Expression("NOW()")

def CurrentTimestamp(timezone: Optional[str] = None) -> _DefaultValue:
    """Postgres DEFAULT constraint for current timestamp."""
    if timezone:
        return Expression(f"CURRENT_TIMESTAMP AT TIME ZONE '{timezone}'")
    return Expression("CURRENT_TIMESTAMP")

def CurrentTime(timezone: Optional[str] = None) -> _DefaultValue:
    """Postgres DEFAULT constraint for current time."""
    if timezone:
        return Expression(f"CURRENT_TIME AT TIME ZONE '{timezone}'")
    return Expression("CURRENT_TIME")

def CurrentDate() -> _DefaultValue:
    """Postgres DEFAULT constraint for current date."""
    return Expression("CURRENT_DATE")

def UUID4() -> _DefaultValue:
    """Postgres DEFAULT constraint for a new UUID."""
    return Expression("gen_random_uuid()")

def NextVal(sequence_name: str) -> _DefaultValue:
    """Postgres DEFAULT constraint for the next value of a sequence."""
    return Expression(f"nextval('{sequence_name}')")

def Random(multiplier: Optional[float] = None) -> _DefaultValue:
    if multiplier is not None:
        return Expression(f"RANDOM() * {multiplier}")
    return Expression("RANDOM()")

def Cast(value: Union[_DefaultValue, Any], postgres_type: Union[Type[PostgresType], PostgresType]) -> _DefaultValue:
    """Postgres DEFAULT constraint with type casting."""
    CompositeType = get_type_class("CompositeType")

    if isinstance(postgres_type, type):
        if issubclass(postgres_type, CompositeType):
            raise TypeError("Cannot cast to a composite type directly. Use the composite type's methods instead.")
        postgres_type = postgres_type()

    cast_type = str(postgres_type)

    if isinstance(value, _DefaultValue):
        return Expression(f"{str(value)}::{cast_type}")
    elif value is None:
        return Expression(f"NULL::{cast_type}")
    elif isinstance(value, (list, dict)):
        return Expression(f"'{orjson.dumps(value).decode()}'::{cast_type}")
    elif isinstance(value, str):
        return Expression("'" + value.replace("'", "''") + f"'::{cast_type}")
    else:
        return Expression(f"{value}::{cast_type}")
    
def Coalesce(*values: Union[_DefaultValue, Any]) -> _DefaultValue:
    """Postgres COALESCE function."""
    if not values:
        raise ValueError("At least one value must be provided to COALESCE.")
    
    formatted_values = []
    for v in values:
        if isinstance(v, _DefaultValue):
            formatted_values.append(str(v))
        elif v is None:
            formatted_values.append("NULL")
        else:
            formatted_values.append(str(v))
    
    return Expression(f"COALESCE({', '.join(formatted_values)})")
    
def Ceil(value: Union[_DefaultValue, Any]) -> _DefaultValue:
    """Postgres CEIL function."""
    if isinstance(value, _DefaultValue):
        return Expression(f"CEIL({str(value)})")
    else:
        return Expression(f"CEIL({value})")

def Floor(value: Union[_DefaultValue, Any]) -> _DefaultValue:
    """Postgres FLOOR function."""
    if isinstance(value, _DefaultValue):
        return Expression(f"FLOOR({str(value)})")
    else:
        return Expression(f"FLOOR({value})")

def Round(value: Union[_DefaultValue, Any], precision: int = 0) -> _DefaultValue:
    """Postgres ROUND function."""
    if isinstance(value, _DefaultValue):
        return Expression(f"ROUND({str(value)}, {precision})")
    else:
        return Expression(f"ROUND({value}, {precision})")