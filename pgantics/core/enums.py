from enum import Enum

__all__ = (
    "Action",
    "RelationshipType",
    "IntervalField",
    "IndexMethod",
    "Order",
    "NullsOrder",
    "PartitionStrategy",
)

class Action(str, Enum):
    """Enum for defining actions in foreign key constraints."""

    NO_ACTION = "NO ACTION"
    RESTRICT = "RESTRICT"
    CASCADE = "CASCADE"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"

class RelationshipType(str, Enum):
    """Enum for defining relationship types between tables."""

    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"
    ONE_TO_ONE = "one_to_one"

class IntervalField(str, Enum):
    """Enum for defining interval fields for the INTERVAL type."""

    YEAR = "YEAR"
    MONTH = "MONTH"
    DAY = "DAY"
    HOUR = "HOUR"
    MINUTE = "MINUTE"
    SECOND = "SECOND"
    YEAR_TO_MONTH = "YEAR_TO_MONTH"
    DAY_TO_HOUR = "DAY_TO_HOUR"
    DAY_TO_MINUTE = "DAY_TO_MINUTE"
    DAY_TO_SECOND = "DAY_TO_SECOND"
    HOUR_TO_MINUTE = "HOUR_TO_MINUTE"
    HOUR_TO_SECOND = "HOUR_TO_SECOND"
    MINUTE_TO_SECOND = "MINUTE_TO_SECOND"

class IndexMethod(str, Enum):
    """Enum for defining index methods."""

    BTREE = "btree"
    HASH = "hash"
    GIST = "gist"
    SPGIST = "spgist"
    GIN = "gin"
    BRIN = "brin"

class Order(str, Enum):
    """Enum for defining order in indexes."""

    ASC = "ASC"
    DESC = "DESC"

class NullsOrder(str, Enum):
    """Enum for defining nulls order in indexes."""

    FIRST = "FIRST"
    LAST = "LAST"

class PartitionStrategy(str, Enum):
    """Enum for defining partitioning strategies."""
    
    RANGE = "RANGE"
    LIST = "LIST" 
    HASH = "HASH"