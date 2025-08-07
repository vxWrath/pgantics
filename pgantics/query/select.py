from typing import TYPE_CHECKING, Any, List, Optional, Tuple, Type

from ..core.enums import JoinType
from ..fields.column import ColumnInfo
from .base import Query
from .conditions import Condition

if TYPE_CHECKING:
    from ..entities.table import Table

class Join:
    def __init__(self, table: Type['Table'], join_type: JoinType = JoinType.INNER):
        self.table = table
        self.join_type = join_type
        self.on_conditions: List[Condition] = []

    def on(self, condition: Condition) -> 'Join':
        self.on_conditions.append(condition)
        return self

class SelectQuery(Query):
    def __init__(self, table: Type['Table']):
        super().__init__(table)

        self._select_fields: List[str] = []
        self._where_conditions: List[Condition] = []
        self._joins: List[Join] = []
        self._order_by: List[str] = []
        self._group_by: List[str] = []
        self._having: List[str] = []
        self._limit_value: Optional[int] = None
        self._offset_value: Optional[int] = None

    def add_select_field(self, field: str) -> None:
        self._select_fields.append(field)

    def build(self) -> Tuple[str, List[Any]]:
        sql_parts = ["SELECT"]
        params = []
        
        # SELECT fields
        if not self._select_fields:
            sql_parts.append("*")
        else:
            sql_parts.append(", ".join(self._select_fields))
        
        # FROM table
        sql_parts.extend(["FROM", self.table.Meta.table_name])

        # JOINs
        for join in self._joins:
            join_sql = [join.join_type.value, join.table.Meta.table_name]

            if join.on_conditions:
                on_clauses = []
                for cond in join.on_conditions:
                    cond_sql, cond_params = cond.build()

                    cond_sql = cond_sql % tuple(str(x) if isinstance(x, ColumnInfo) else "%s" for x in cond_params)

                    on_clauses.append(cond_sql)
                    params.extend([x for x in cond_params if not isinstance(x, ColumnInfo)])
                    
                join_sql.append("ON " + " AND ".join(on_clauses))

            sql_parts.append(" ".join(join_sql))
        
        # WHERE conditions
        if self._where_conditions:
            where_parts = []
            for condition in self._where_conditions:
                cond_sql, cond_params = condition.build()
                where_parts.append(cond_sql)
                params.extend(cond_params)
            sql_parts.extend(["WHERE", " AND ".join(where_parts)])
        
        # ORDER BY
        if self._order_by:
            sql_parts.extend(["ORDER BY", ", ".join(self._order_by)])
        
        # LIMIT/OFFSET
        if self._limit_value:
            sql_parts.extend(["LIMIT", str(self._limit_value)])
        if self._offset_value:
            sql_parts.extend(["OFFSET", str(self._offset_value)])
        
        return " ".join(sql_parts), params

    def where(self, *conditions: Any) -> 'SelectQuery':
        """Add WHERE conditions"""
        for condition in conditions:
            if not isinstance(condition, Condition):
                raise TypeError(f"Expected Condition, got {type(condition).__name__}")
            self._where_conditions.append(condition)

        return self

    def order_by(self, *columns: Any) -> 'SelectQuery':
        """Add ORDER BY"""
        for column in columns:
            if isinstance(column, str):
                self._order_by.append(column)
            else:
                self._order_by.append(str(column))

        return self
    
    def limit(self, count: int) -> 'SelectQuery':
        """Add LIMIT"""
        self._limit_value = count
        return self
    
    def offset(self, count: int) -> 'SelectQuery':
        """Add OFFSET"""
        self._offset_value = count
        return self

    def join(self, table: Type['Table'], join_type: JoinType = JoinType.INNER) -> 'SelectQuery':
        """Add JOIN"""
        join = Join(table, join_type)
        self._joins.append(join)
        return self
    
    def left_join(self, table: Type['Table']) -> 'SelectQuery':
        """Add LEFT JOIN"""
        join = Join(table, JoinType.LEFT)
        self._joins.append(join)
        return self
    
    def on(self, condition: Condition) -> 'SelectQuery':
        """Add ON condition to last join"""
        if not self._joins:
            raise ValueError("No JOIN to add ON condition to")
        self._joins[-1].on(condition)
        return self