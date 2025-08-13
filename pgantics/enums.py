from enum import StrEnum

__all__ = [
    "Operator",
    "JoinType"
]

class Operator(StrEnum):
    EQ = "="
    NEQ = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="

    LIKE = "LIKE"
    ILIKE = "ILIKE"
    IN = "IN"
    NOT_IN = "NOT IN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"

    OR = "OR"
    AND = "AND"

class JoinType(StrEnum):
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL OUTER"
    CROSS = "CROSS"
    NATURAL = "NATURAL"