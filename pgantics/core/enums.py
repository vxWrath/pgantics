from enum import Enum

__all__ = (
    "Action",
    "RelationshipType",
    "IntervalField"
)

class Action(str, Enum):
    NO_ACTION = "NO ACTION"
    RESTRICT = "RESTRICT"
    CASCADE = "CASCADE"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"

class RelationshipType(str, Enum):
    ONE_TO_MANY = "one_to_many"
    MANY_TO_ONE = "many_to_one"
    MANY_TO_MANY = "many_to_many"
    ONE_TO_ONE = "one_to_one"

class IntervalField(str, Enum):
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