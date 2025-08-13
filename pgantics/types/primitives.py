from typing import Optional

from .base import PostgresType

__all__ = [
    "Boolean", 
    "SmallInt", "Integer", "BigInt", 
    "Real", 
    "SmallSerial", "Serial", "BigSerial", 
    "Char", "BPChar", "VarChar", "Text",
    "Timestamp", "TimestampTZ", 
    "JSON", "JSONB", 
    "UUID"
]

class Boolean(PostgresType):
    """Postgres BOOLEAN type. Represents a boolean value.

    Example:
        # Basic
        is_active: Mapped[bool] = Column(types.Boolean())

        # With default
        has_premium: Mapped[bool] = Column(types.Boolean(), postgres_default=default.Bool(False))
    """

    def __str__(self):
        return "BOOLEAN"

class SmallInt(PostgresType):
    """Postgres SMALLINT type. (-32768 to 32767)

    Example:
        # Basic
        age: Mapped[int] = Column(types.SmallInt())

        # With default
        age: Mapped[int] = Column(types.SmallInt(), postgres_default=default.Int(18))
    """

    def __str__(self):
        return "SMALLINT"
    
class Integer(PostgresType):
    """Postgres INTEGER type. (-2147483648 to 2147483647)

    Example:
        # Basic
        age: Mapped[int] = Column(types.Integer())

        # With default
        age: Mapped[int] = Column(types.Integer(), postgres_default=default.Int(18))
    """

    def __str__(self):
        return "INTEGER"

class BigInt(PostgresType):
    """Postgres BIGINT type. (-9223372036854775808 to 9223372036854775807)

    Example:
        # Basic
        id: Mapped[int] = Column(types.BigInt())
    """

    def __str__(self):
        return "BIGINT"
    
class Real(PostgresType):
    """Postgres REAL type. (Single precision floating point)

    Example:
        # Basic
        price: Mapped[float] = Column(types.Real())

        # With default
        price: Mapped[float] = Column(types.Real(), postgres_default=default.Float(19.99))
    """

    def __str__(self):
        return "REAL"

class SmallSerial(PostgresType):
    """Postgres SMALLSERIAL type. (1 to 32767)

    Example:
        # Basic
        item_id: Mapped[int] = Column(types.SmallSerial())
    """

    def __str__(self):
        return "SMALLSERIAL"

class Serial(PostgresType):
    """Postgres SERIAL type. (1 to 2147483647)

    Example:
        # Basic
        user_id: Mapped[int] = Column(types.Serial())
    """

    def __str__(self):
        return "SERIAL"

class BigSerial(PostgresType):
    """Postgres BIGSERIAL type. (1 to 9223372036854775807)

    Example:
        # Basic
        order_id: Mapped[int] = Column(types.BigSerial())
    """

    def __str__(self):
        return "BIGSERIAL"
    
class __string(PostgresType):
    _min = 1
    _max = 8000

    def __init__(self, length: Optional[int] = None):
        if length is not None and (length < self._min or length > self._max):
            raise ValueError(f"Length must be between {self._min} and {self._max}. Got {length}.")
        self.length = length

    def __str__(self):
        type = self.__class__.__name__.upper()

        if self.length is not None:
            return f"{type}({self.length})"
        return type

class Char(__string):
    """Postgres CHAR type. Fixed-length character type.

    Example:
        # Basic
        initials: Mapped[str] = Column(types.Char(3))
    """

class BPChar(__string):
    """Postgres BPCHAR type. Blank-padded character type.

    Example:
        # Basic
        initials: Mapped[str] = Column(types.BPChar(3))
    """

class VarChar(__string):
    """Postgres VARCHAR type. Variable-length character type.

    Example:
        # Basic
        email: Mapped[str] = Column(types.VarChar(100))
    """

    _max = 65535

class Text(PostgresType):
    """Postgres TEXT type. Variable-length character type.

    Example:
        # Basic
        bio: Mapped[str] = Column(types.Text())
    """

class Timestamp(PostgresType):
    """Postgres TIMESTAMP type. Represents a point in time.

    Example:
        # Basic
        created_at: Mapped[datetime] = Column(types.Timestamp())
    """

    def __init__(self, precision: Optional[int] = None):
        if precision is not None and (precision < 0 or precision > 6):
            raise ValueError("Precision must be between 0 and 6.")
        self.precision = precision

    def __str__(self):
        type = self.__class__.__name__.upper()

        if self.precision is not None:
            return f"{type}({self.precision})"
        return type

class TimestampTZ(PostgresType):
    """Postgres TIMESTAMPTZ type. Represents a point in time with time zone.

    Example:
        # Basic
        created_at: Mapped[datetime] = Column(types.TimestampTZ())
    """

class JSON(PostgresType):
    """Postgres JSON type. Stores JSON (JavaScript Object Notation) data.

    Example:
        # Basic
        data: Mapped[Dict[str, Any]] = Column(types.JSON())

        # With default
        settings: Mapped[Dict[str, Any]] = Column(types.JSON(), postgres_default=default.JSON({"theme": "dark"}))
    """

class JSONB(PostgresType):
    """Postgres JSONB type. Stores JSON (JavaScript Object Notation) data in a binary format.

    Example:
        # Basic
        data: Mapped[Dict[str, Any]] = Column(types.JSONB())

        # With default
        settings: Mapped[Dict[str, Any]] = Column(types.JSONB(), postgres_default=default.JSONB({"theme": "dark"}))
    """

class UUID(PostgresType):
    """Postgres UUID type. Stores Universally Unique Identifiers.

    Example:
        # Basic
        id: Mapped[uuid.UUID] = Column(types.UUID())

        # With default
        id: Mapped[uuid.UUID] = Column(types.UUID(), postgres_default=default.UUID4())
    """