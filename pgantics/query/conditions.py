from functools import cache
from typing import TYPE_CHECKING, Any, List, Tuple, Type

from ..core.enums import Operator

if TYPE_CHECKING:
    from ..fields.column import ColumnInfo

@cache
def _get_column_class() -> Type['ColumnInfo']:
    """Avoids circular import."""
    from ..fields.column import ColumnInfo
    return ColumnInfo

class Condition:
    def __init__(self, column: 'ColumnInfo', operator: Operator, value: Any):
        self.column = column
        self.operator = operator
        self.value = value
    
    def __and__(self, other: 'Condition') -> 'AndCondition':
        return AndCondition(self, other)
    
    def __or__(self, other: 'Condition') -> 'OrCondition':
        return OrCondition(self, other)
    
    def __invert__(self) -> 'NotCondition':
        return NotCondition(self)

    def build(self) -> Tuple[str, List[Any]]:
        ColumnInfo = _get_column_class()

        if self.operator == Operator.IN and isinstance(self.value, list):
            placeholders = ', '.join(['%s'] * len(self.value))
            return f"{self.column} IN ({placeholders})", self.value
        elif self.operator == Operator.IS_NULL and self.value is None:
            return f"{self.column} IS NULL", []
        else:
            return f"{self.column} {self.operator.value} %s", [self.value]

class AndCondition(Condition):
    def __init__(self, left: Condition, right: Condition):
        self.left = left
        self.right = right
    
    def build(self) -> Tuple[str, List[Any]]:
        left_sql, left_params = self.left.build()
        right_sql, right_params = self.right.build()

        return f"({left_sql}) AND ({right_sql})", left_params + right_params
    
class OrCondition(Condition):
    def __init__(self, left: Condition, right: Condition):
        self.left = left
        self.right = right
    
    def build(self) -> Tuple[str, List[Any]]:
        left_sql, left_params = self.left.build()
        right_sql, right_params = self.right.build()
        return f"({left_sql}) OR ({right_sql})", left_params + right_params
    
class NotCondition(Condition):
    def __init__(self, condition: Condition):
        self.condition = condition
    
    def build(self) -> Tuple[str, List[Any]]:
        sql, params = self.condition.build()
        return f"NOT ({sql})", params