from enum import StrEnum

__all__ = [
    "Operator",
    "BinaryOperator",
    "JoinType",
    "Order",
    "Action",
    "ConflictAction"
]

class Operator(StrEnum):
    """SQL comparison and logical operators."""

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
    BETWEEN = "BETWEEN"
    NOT_BETWEEN = "NOT BETWEEN"

    OR = "OR"
    AND = "AND"

class BinaryOperator(StrEnum):
    """SQL arithmetic and binary operators."""

    ADD = "+"
    SUBTRACT = "-"
    MULTIPLY = "*"
    DIVIDE = "/"
    MODULUS = "%"
    POWER = "^"
    
    # Bitwise operators
    BITWISE_AND = "&"
    BITWISE_OR = "|"
    BITWISE_XOR = "#"
    BITWISE_SHIFT_LEFT = "<<"
    BITWISE_SHIFT_RIGHT = ">>"
    
    # String operators
    CONCAT = "||"
    
class JoinType(StrEnum):
    """SQL JOIN types."""

    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL OUTER"
    CROSS = "CROSS"
    NATURAL = "NATURAL"

class Order(StrEnum):
    """SQL ORDER BY directions."""

    ASC = "ASC"
    DESC = "DESC"

class Action(StrEnum):
    """Enum for defining actions in foreign key constraints."""

    NO_ACTION = "NO ACTION"
    RESTRICT = "RESTRICT"
    CASCADE = "CASCADE"
    SET_NULL = "SET NULL"
    SET_DEFAULT = "SET DEFAULT"

class ConflictAction(StrEnum):
    """Enum for defining actions in ON CONFLICT clauses."""

    DO_NOTHING = "DO NOTHING"
    DO_UPDATE = "DO UPDATE"