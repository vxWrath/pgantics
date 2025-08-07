from typing import TYPE_CHECKING, Any, List, Tuple

from .base import Query

if TYPE_CHECKING:
    from ..entities.table import Table

class UpdateQuery(Query):
    def __init__(self, table: 'Table'):
        super().__init__(table)
        self._set_values = {}
        self._where_conditions = []
        self._returning = []
    
    def set(self, **kwargs) -> 'UpdateQuery':
        """Set values to update"""
        self._set_values.update(kwargs)
        return self
    
    def where(self, *conditions) -> 'UpdateQuery':
        """Add WHERE conditions"""
        self._where_conditions.extend(conditions)
        return self
    
    def build(self) -> Tuple[str, List[Any]]:
        if not self._set_values:
            raise ValueError("No values provided for UPDATE")
        
        set_clauses = []
        params = []
        
        for column, value in self._set_values.items():
            set_clauses.append(f"{column} = %s")
            params.append(value)
        
        sql = f"UPDATE {self.table.Meta.table_name} SET {', '.join(set_clauses)}"
        
        # WHERE conditions (similar to SELECT)
        if self._where_conditions:
            where_parts = []
            for condition in self._where_conditions:
                cond_sql, cond_params = condition.build()
                where_parts.append(cond_sql)
                params.extend(cond_params)
            sql += f" WHERE {' AND '.join(where_parts)}"
        
        return sql, params