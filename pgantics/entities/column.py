from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Self,
    Sequence,
    Sized,
    Tuple,
    Type,
    Union,
    Unpack,
)

from pydantic.fields import FieldInfo, _FromFieldInfoInputs

from ..enums import Operator
from ..types.base import PostgresType
from ..utils import MISSING

if TYPE_CHECKING:
    from .table import Table

__all__ = ["Column"]

class pganticsFieldInfo(FieldInfo):
    def __init__(self, *,
        type: Union[Type[PostgresType], PostgresType]=MISSING,
        default: Any = MISSING,
        **kwargs: Unpack[_FromFieldInfoInputs]
    ):
        self._type = type

        if default is MISSING:
            super().__init__(**kwargs)
        else:
            super().__init__(default=default, **kwargs)

class ColumnInfo[T](pganticsFieldInfo):
    def __init__(self, *,
        type: Union[Type[PostgresType], PostgresType]=MISSING,
        primary_key: bool = False,
        default: Any = MISSING,
        **kwargs: Unpack[_FromFieldInfoInputs]
    ):
        super().__init__(default=default, **kwargs)

        self.sql_data: Dict[str, Any] = {}

        self._source_table: Type[Table] = MISSING
        self._source_field: str = MISSING

        if primary_key:
            self.sql_data['primary_key'] = True

    def __eq__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.EQ, other)
    
    def __ne__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.NEQ, other)
    
    def __lt__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.LT, other)

    def __le__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.LTE, other)

    def __gt__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.GT, other)

    def __ge__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.GTE, other)

    def in_(self, item: Sequence[Any]) -> 'Condition':
        return Condition(self, Operator.IN, item)

    def not_in_(self, item: Sequence[Any]) -> 'Condition':
        return Condition(self, Operator.NOT_IN, item)

    def like(self, pattern: str) -> 'Condition':
        return Condition(self, Operator.LIKE, pattern)

    def ilike(self, pattern: str) -> 'Condition':
        return Condition(self, Operator.ILIKE, pattern)
    
    def is_null(self) -> 'Condition':
        return Condition(self, Operator.IS_NULL, None)

    def is_not_null(self) -> 'Condition':
        return Condition(self, Operator.IS_NOT_NULL, None)
    
    def __str__(self) -> str:
        return f"{self._source_table.Meta.table_name}.{self._source_field}"

    def asc(self) -> Tuple[Self, str]:
        """Sort in ascending order. This should only be used in ORDER BY clauses.
        
        Example:
        ```
            User.select().order_by(User.created_at.asc())
        ```
        """
        return self, "ASC"

    def desc(self) -> Tuple[Self, str]:
        """Sort in descending order. This should only be used in ORDER BY clauses.

        Example:
        ```
            User.select().order_by(User.created_at.desc())
        ```
        """
        return self, "DESC"

def Column(
    type: Union[Type[PostgresType], PostgresType]=MISSING,
    /, *,
    primary_key: bool = False,
    default: Any = MISSING,
    **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
) -> Any:
    """Create a new column.
    
    Args:
        type: The postgres type for the column. If not provided, it will be inferred from the annotation.
        primary_key: Whether the column is a primary key. Defaults to False.
        default: The default value for pydantic fields.
        **pydantic_kwargs: Additional keyword arguments for the column.

    Example:
    ```
        # Basic
        name: Mapped[str] = Column(Text, unique=True)

        # Primary key
        id: Mapped[int] = Column(Bigint(), primary_key=True)
    ```
    """

    return ColumnInfo(
        type=type,
        primary_key=primary_key,
        default=default,
        **pydantic_kwargs
    )

class Condition[T]:
    def __init__(self, column: ColumnInfo[T], operator: Operator, comparator: Any):
        self.column = column
        self.operator = operator
        self.comparator = comparator

    def __and__(self, other: 'Condition'):
        if not isinstance(other, Condition):
            return NotImplemented
        return ConditionTree("AND", self, other)

    def __or__(self, other: 'Condition'):
        if not isinstance(other, Condition):
            return NotImplemented
        return ConditionTree("OR", self, other)

    def __invert__(self):
        return NotCondition(self)
    
    def build(self) -> Tuple[str, List[Any]]:
        if self.operator == Operator.IN and isinstance(self.comparator, Sequence) and isinstance(self.comparator, Sized):
            placeholders = ', '.join(['%s'] * len(self.comparator))
            return (f"{str(self.column)} IN ({placeholders})", list(self.comparator))
        elif self.operator == Operator.NOT_IN and isinstance(self.comparator, Sequence) and isinstance(self.comparator, Sized):
            placeholders = ', '.join(['%s'] * len(self.comparator))
            return (f"{str(self.column)} NOT IN ({placeholders})", list(self.comparator))

        elif self.operator == Operator.IS_NULL and self.comparator is None:
            return (f"{str(self.column)} IS NULL", [])
        elif self.operator == Operator.IS_NOT_NULL and self.comparator is None:
            return (f"{str(self.column)} IS NOT NULL", [])
        
        elif isinstance(self.comparator, ColumnInfo):
            return (f"{str(self.column)} {self.operator.value} {str(self.comparator)}", [])
        
        else:
            return (f"{self.column} {self.operator.value} %s", [self.comparator])
        
    def __str__(self) -> str:
        sql, _ = self.build()
        return sql

class ConditionTree(Condition):
    def __init__(self, joiner: str, left: Condition, right: Condition):
        self.left = left
        self.right = right
        self.joiner = joiner

    def build(self) -> Tuple[str, List[Any]]:
        left_sql, left_params = self.left.build()
        right_sql, right_params = self.right.build()

        return (f"({left_sql}) {self.joiner} ({right_sql})", left_params + right_params)
    
class NotCondition(Condition):
    def __init__(self, condition: Condition):
        self.condition = condition

    def build(self) -> Tuple[str, List[Any]]:
        sql, params = self.condition.build()
        return (f"NOT ({sql})", params)