from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Literal,
    Optional,
    Self,
    Tuple,
    Type,
    Union,
    overload,
)

from ..entities.column import ColumnInfo, Condition
from ..enums import JoinType
from ..registry import TABLE_REGISTRY
from ..utils import MISSING
from .base import Query
from .expression import FuncExpression

if TYPE_CHECKING:
    from ..entities.table import Table

__all__ = ["Select"]

class Select(Query):
    def __init__(self, table: Type['Table']):
        self.table = table

        self._columns: List[Tuple[Optional[Type['Table']], str]] = []
        self._joins: Dict[str, 'Join'] = {}
        self._wheres: List[Tuple[Literal['AND', 'OR'], List[Condition[Any]]]] = []
        self._order_by: List[Tuple[Optional[Type['Table']], Tuple[str, str]]] = []
        self._limit: Optional[int] = None
        self._offset: Optional[int] = None

    def build(self) -> Tuple[str, List[Any]]:
        parts = ["SELECT"]
        params: List[Any] = []

        if not self._columns:
            parts.append("*")
        else:
            columns: List[str] = []
            for table, col in self._columns:
                # Table is only None when its a function expression
                if table is None:
                    columns.append(col)
                else:
                    if table != self.table and table.Meta.table_name not in self._joins:
                        raise ValueError(f"Table '{table.Meta.table_name}' is not joined.")
                    
                    columns.append(f"{table.Meta.table_name}.{col}")

            parts.append(", ".join(columns))

        parts.extend(["FROM", self.table.Meta.table_name])

        for join in self._joins.values():
            if join.join_type in [JoinType.INNER, JoinType.LEFT, JoinType.RIGHT, JoinType.FULL] and not join.on_conditions:
                raise ValueError(f"Join 'ON' condition is required for {join.join_type.value}.")
            elif join.join_type in [JoinType.CROSS, JoinType.NATURAL] and join.on_conditions:
                raise ValueError(f"Join 'ON' condition is not allowed for {join.join_type.value}.")

            join_sql, join_params = join._build()

            parts.append(join_sql)
            params.extend(join_params)

        if self._wheres:
            where_clauses: List[str] = []
            for conjunction, conditions in self._wheres:
                conditions_sql: List[str] = []
                for cond in conditions:
                    where_sql, where_params = cond.build()

                    conditions_sql.append(where_sql)
                    params.extend(where_params)

                where_clauses.append("(" + f" {conjunction} ".join(conditions_sql) + ")")
            parts.extend(["WHERE", " AND ".join(where_clauses)])

        if self._order_by:
            order_clauses = []
            for table, (col, direction) in self._order_by:
                # Table is only None when its a function expression
                if table is None:
                    order_clauses.append(col)
                else:
                    qualified_col = f"{table.Meta.table_name}.{col}"
                    if direction and direction != MISSING:
                        order_clauses.append(f"{qualified_col} {direction}")
                    else:
                        order_clauses.append(qualified_col)
                    
            parts.extend(["ORDER BY", ", ".join(order_clauses)])

        if self._limit is not None:
            parts.append(f"LIMIT {self._limit}")

        if self._offset is not None:
            parts.append(f"OFFSET {self._offset}")

        return " ".join(parts), params

    def select(self, *columns: Union[str, ColumnInfo[Any], FuncExpression]) -> Self:
        for col in columns:
            if isinstance(col, str):
                if '.' in col:
                    table_name, column = col.split('.', 1)
                    table = TABLE_REGISTRY.get(table_name)
                    if table is None:
                        raise ValueError(f"Table '{table_name}' not found in registry.")
                else:
                    table = self.table
                    column = col
            elif isinstance(col, ColumnInfo):
                table = col._source_table
                column = col._source_field
            elif isinstance(col, FuncExpression):
                self._columns.append((None, str(col)))
                continue
            else:
                raise TypeError(f"Invalid column type: {type(col)}. Must be str, ColumnInfo, or FuncExpression.")

            if column not in table.__pgantics_fields__:
                raise AttributeError(f"Column '{column}' does not exist on table '{table.__name__}'.")
            
            self._columns.append((table, column))

        return self
    
    @overload
    def join(self, table: Union[Type['Table'], str]) -> 'Join': ...

    @overload
    def join(self, table: Union[Type['Table'], str], join_type: Literal[JoinType.INNER, JoinType.LEFT, JoinType.RIGHT, JoinType.FULL]) -> 'Join': ...

    @overload
    def join(self, table: Union[Type['Table'], str], join_type: Literal[JoinType.CROSS, JoinType.NATURAL]) -> Self: ...

    def join(self, table: Union[Type['Table'], str], join_type: JoinType = JoinType.INNER) -> Union['Join', Self]:
        if isinstance(table, str):
            resolved_table = TABLE_REGISTRY.get(table)
            if resolved_table is None:
                raise ValueError(f"Table '{table}' not found in registry.")
            table = resolved_table

        join = Join(self, table, join_type)
        self._joins[table.Meta.table_name] = join

        if join_type in [JoinType.CROSS, JoinType.NATURAL]:
            return self
        return join

    def where(self, *conditions: Condition[Any], conjunction: Literal['AND', 'OR'] = 'AND') -> Self:
        if not conditions:
            raise ValueError("At least one condition is required.")

        for condition in conditions:
            if not isinstance(condition, Condition):
                raise TypeError(f"Expected Condition, got {type(condition).__name__}")
            
        self._wheres.append((conjunction, list(conditions)))
        return self

    def order_by(self, *columns: Union[str, ColumnInfo[Any], FuncExpression, Tuple[Union[str, ColumnInfo[Any], FuncExpression], str]]) -> Self:
        for col in columns:
            direction: str = MISSING

            if isinstance(col, tuple):
                col, direction = col

                if direction not in ("ASC", "DESC"):
                    raise ValueError(f"Invalid sort direction: {direction}. Must be 'ASC' or 'DESC'.")

            if isinstance(col, str):
                if '.' in col:
                    table_name, column = col.split('.', 1)
                    table = TABLE_REGISTRY.get(table_name)
                    if table is None:
                        raise ValueError(f"Table '{table_name}' not found in registry.")
                else:
                    table = self.table
                    column = col
            elif isinstance(col, ColumnInfo):
                table = col._source_table
                column = col._source_field
            elif isinstance(col, FuncExpression):
                self._order_by.append((None, (str(col), direction)))
                continue
            else:
                raise TypeError(f"Invalid column type: {type(col)}. Must be str, ColumnInfo, FuncExpression, or tuple.")

            if column not in table.__pgantics_fields__:
                raise AttributeError(f"Column '{column}' does not exist on table '{table.__name__}'.")

            self._order_by.append((table, (column, direction)))

        return self

    def limit(self, count: int) -> Self:
        if count < 0:
            raise ValueError("LIMIT count cannot be negative.")
        self._limit = count
        return self

    def offset(self, count: int) -> Self:
        if count < 0:
            raise ValueError("OFFSET count cannot be negative.")
        self._offset = count
        return self


class Join:
    def __init__(self, query: Select, table: Type['Table'], join_type: JoinType = JoinType.INNER, alias: Optional[str] = None):
        self.query = query
        self.table = table
        self.join_type = join_type
        self.alias = alias

        self.on_conditions: List[Condition[Any]] = []

    def _build(self) -> Tuple[str, List[Any]]:
        """This should not be called directly."""

        if self.join_type in [JoinType.CROSS, JoinType.NATURAL]:
            return f"{self.join_type.value} JOIN {self.table.Meta.table_name}", []

        # Build the JOIN clause
        table_name = self.table.Meta.table_name
        alias_part = f" AS {self.alias}" if self.alias else ""
        join_clause = f"{self.join_type.value} JOIN {table_name}{alias_part}"

        if not self.on_conditions:
            return f"{join_clause} ON", []

        params: List[Any] = []
        condition_sqls: List[str] = []

        for condition in self.on_conditions:
            condition_sql, condition_params = condition.build()
            condition_sqls.append(condition_sql)
            params.extend(condition_params)

        on_clause = " AND ".join(condition_sqls)
        return f"{join_clause} ON {on_clause}", params

    def on(self, *conditions: Condition[Any]) -> Select:
        """Add ON conditions to the join. Multiple conditions are combined with AND."""
        if not conditions:
            raise ValueError("At least one condition is required.")
        
        for condition in conditions:
            if not isinstance(condition, Condition):
                raise TypeError(f"Expected Condition, got {type(condition).__name__}")
            
        self.on_conditions.extend(conditions)
        return self.query