from typing import Optional

from ..core.enums import IntervalField
from .base import PostgresType

__all__ = (
    "SmallInt",
    "Integer",
    "BigInt",
    "Decimal",
    "Numeric",
    "Real",
    "DoublePrecision",
    "SmallSerial",
    "Serial",
    "BigSerial",
    "Money",
    "VarChar",
    "Char",
    "BPChar",
    "Text",
    "Bytea",
    "Timestamp",
    "TimestampTZ",
    "Date",
    "Time",
    "TimeTZ",
    "Interval",
    "Boolean",
    "Point",
    "Line",
    "LSeg",
    "Box",
    "Path",
    "Polygon",
    "Circle",
    "CIDR",
    "INET",
    "MACADDR",
    "MACADDR8",
    "Bit",
    "BitVarying",
    "TSVector",
    "TSQuery",
    "PGuuid",
    "XML",
    "JSON",
    "JSONB"
)

class SmallInt(PostgresType):
    """Postgres SMALLINT type (-32768 to 32767)"""

    def __str__(self):
        return "SMALLINT"

class Integer(PostgresType):
    """Postgres INTEGER type (-2,147,483,648 to 2,147,483,647)"""

    def __str__(self):
        return "INTEGER"

class BigInt(PostgresType):
    """Postgres BIGINT type (-9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)"""

    def __str__(self):
        return "BIGINT"

class Decimal(PostgresType):
    """Postgres DECIMAL type (-10^38 +1 to 10^38 -1)"""

    def __init__(self, precision: Optional[int] = None, scale: Optional[int] = None):
        if scale is not None and precision is None:
            raise ValueError("Cannot specify scale without precision")
        if precision is not None and precision < 1:
            raise ValueError("Precision must be at least 1")
        if scale is not None and scale < 0:
            raise ValueError("Scale cannot be negative")
        if scale is not None and precision is not None and scale > precision:
            raise ValueError("Scale cannot be greater than precision")

        self.precision = precision
        self.scale = scale

    def __str__(self):
        name = self.__class__.__name__.upper()

        if self.precision is not None and self.scale is not None:
            return f"{name}({self.precision},{self.scale})"
        elif self.precision is not None:
            return f"{name}({self.precision})"
        else:
            return name

class Numeric(Decimal):
    """Postgres NUMERIC type. Alias for Decimal."""

class Real(PostgresType):
    """Postgres REAL type (-3.4028235E38 to 3.4028235E38)"""

    def __str__(self):
        return "REAL"

class DoublePrecision(PostgresType):
    """Postgres DOUBLE PRECISION type (-1.7976931348623157E308 to 1.7976931348623157E308)"""

    def __str__(self):
        return "DOUBLE PRECISION"

class SmallSerial(PostgresType):
    """Postgres SMALLSERIAL type (1 to 32,767)"""

    def __str__(self):
        return "SMALLSERIAL"

class Serial(PostgresType):
    """Postgres SERIAL type (1 to 2,147,483,647)"""

    def __str__(self):
        return "SERIAL"

class BigSerial(PostgresType):
    """Postgres BIGSERIAL type (1 to 9,223,372,036,854,775,807)"""

    def __str__(self):
        return "BIGSERIAL"

class Money(PostgresType):
    """Postgres MONEY type (1 to 92233720368547758)"""

    def __str__(self):
        return "MONEY"
    
class Char(PostgresType):
    """Postgres CHAR type, fixed length (1 to 8,000 characters)"""

    def __init__(self, length: Optional[int] = None):
        if length is not None and (length < 1 or length > 8000):
            raise ValueError("Length must be between 1 and 8000")
        self.length = length

    def __str__(self):
        name = self.__class__.__name__.upper()

        if self.length is not None:
            return f"{name}({self.length})"
        return name

class BPChar(Char):
    """Postgres BPCHAR type, fixed length (1 to 8,000 characters)"""

class VarChar(Char):
    """Postgres VARCHAR type, variable length (1 to 65,535 characters)"""

    def __init__(self, length: Optional[int] = None):
        if length is not None and (length < 1 or length > 65535):
            raise ValueError("Length must be between 1 and 65,535")
        self.length = length

class Text(PostgresType):
    """Postgres TEXT type, variable length (1 to 1 GB)"""

    def __str__(self):
        return "TEXT"

class Bytea(PostgresType):
    """Postgres BYTEA type, variable length (1 to 1 GB)"""

    def __str__(self):
        return "BYTEA"

class Timestamp(PostgresType):
    """Postgres TIMESTAMP type (date and time without time zone)"""

    def __init__(self, precision: Optional[int] = None):
        if precision is not None and (precision < 0 or precision > 6):
            raise ValueError("Precision must be between 0 and 6")
        self.precision = precision
    
    def __str__(self) -> str:
        name = self.__class__.__name__.upper()

        if self.precision is not None:
            return f"{name}({self.precision})"
        return f"{name}"

class TimestampTZ(Timestamp):
    """Postgres TIMESTAMPTZ type (date and time with time zone)"""

class Date(PostgresType):
    """Postgres DATE type (calendar date)"""

    def __str__(self):
        return "DATE"

class Time(Timestamp):
    """Postgres TIME type (time without time zone)"""

    def __init__(self, precision: Optional[int] = None):
        if precision is not None and (precision < 0 or precision > 6):
            raise ValueError("Precision must be between 0 and 6")
        self.precision = precision

class TimeTZ(Time):
    """Postgres TIMETZ type (time with time zone)"""

    def __str__(self):
        if self.precision is not None:
            return f"TIMETZ({self.precision})"
        return "TIMETZ"

class Interval(Time):
    """Postgres INTERVAL type (time span)"""

    def __init__(self, field: Optional[IntervalField] = None, precision: Optional[int] = None):
        super().__init__(precision)
        self.field = field

    def __str__(self):
        result = "INTERVAL"

        if self.field:
            result += f" {self.field.value}"
        if self.precision is not None:
            result += f"({self.precision})"

        return result

class Boolean(PostgresType):
    """Postgres BOOLEAN type"""

    def __str__(self):
        return "BOOLEAN"

class Point(PostgresType):
    """Postgres POINT type"""

    def __str__(self):
        return "POINT"

class Line(PostgresType):
    """Postgres LINE type."""

    def __str__(self):
        return "LINE"

class LSeg(PostgresType):
    """Postgres LSEG type."""

    def __str__(self):
        return "LSEG"

class Box(PostgresType):
    """Postgres BOX type."""

    def __str__(self):
        return "BOX"

class Path(PostgresType):
    """Postgres PATH type."""

    def __str__(self):
        return "PATH"

class Polygon(PostgresType):
    """Postgres POLYGON type."""

    def __str__(self):
        return "POLYGON"

class Circle(PostgresType):
    """Postgres CIRCLE type."""

    def __str__(self):
        return "CIRCLE"

class CIDR(PostgresType):
    """Postgres CIDR type."""

    def __str__(self):
        return "CIDR"

class INET(PostgresType):
    """Postgres INET type."""

    def __str__(self):
        return "INET"

class MACADDR(PostgresType):
    """Postgres MACADDR type."""

    def __str__(self):
        return "MACADDR"

class MACADDR8(PostgresType):
    """Postgres MACADDR8 type."""

    def __str__(self):
        return "MACADDR8"

class Bit(PostgresType):
    """Postgres BIT type."""

    def __init__(self, length: Optional[int] = None):
        if length is not None and length < 1:
            raise ValueError("Length must be at least 1")
        self.length = length
    
    def __str__(self) -> str:
        if self.length is not None:
            return f"BIT({self.length})"
        return "BIT"

class BitVarying(PostgresType):
    """Postgres BIT VARYING type."""

    def __init__(self, length: Optional[int] = None):
        if length is not None and length < 1:
            raise ValueError("Length must be at least 1")
        self.length = length

    def __str__(self):
        if self.length is not None:
            return f"BIT VARYING({self.length})"
        return "BIT VARYING"

class TSVector(PostgresType):
    """Postgres TSVECTOR type."""

    def __str__(self):
        return "TSVECTOR"

class TSQuery(PostgresType):
    """Postgres TSQUERY type."""

    def __str__(self):
        return "TSQUERY"

class PGuuid(PostgresType):
    """Postgres UUID type."""

    def __str__(self):
        return "UUID"

class XML(PostgresType):
    """Postgres XML type."""

    def __str__(self):
        return "XML"

class JSON(PostgresType):
    """Postgres JSON type."""

    def __str__(self):
        return "JSON"

class JSONB(PostgresType):
    """Postgres JSONB type."""

    def __str__(self):
        return "JSONB"