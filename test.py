from enum import StrEnum
from typing import Any, Union


class Operator(StrEnum):
    EQ = "="
    NEQ = "!="
    LT = "<"
    LTE = "<="
    GT = ">"
    GTE = ">="

    OR = "OR"
    AND = "AND"


class Field:
    def __init__(self, column: str):
        self.column = column

    def __eq__(self, other) -> 'Condition':
        return Condition(self, Operator.EQ, other)

    def __ne__(self, other) -> 'Condition':
        return Condition(self, Operator.NEQ, other)

    def __lt__(self, other) -> 'Condition':
        return Condition(self, Operator.LT, other)

    def __gt__(self, other) -> 'Condition':
        return Condition(self, Operator.GT, other)

    def __le__(self, other) -> 'Condition':
        return Condition(self, Operator.LTE, other)

    def __ge__(self, other) -> 'Condition':
        return Condition(self, Operator.GTE, other)

    def __str__(self) -> str:
        return self.column


class Condition:
    def __init__(self, field: Union['Field', 'Condition'], operator: Operator, value: Any):
        self.field = field
        self.operator = operator
        self.value = value

    def __str__(self) -> str:
        return f"({self.field} {self.operator} {self.value})"

    __repr__ = __str__

    def __or__(self, other: 'Condition') -> 'Condition':
        return Condition(self, Operator.OR, other)

    def __and__(self, other: 'Condition') -> 'Condition':
        return Condition(self, Operator.AND, other)


def where(*conditions):
    for cond in conditions:
        print(cond)


# Usage
name = Field("name")
age = Field("age")

sql = where((name == "John") & (age > 18))