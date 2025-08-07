from typing import TYPE_CHECKING, Any, List, Tuple, Union

from ..fields.column import ColumnInfo
from .base import Query

if TYPE_CHECKING:
    from ..entities.table import Table

class InsertQuery(Query):
    def __init__(self, table: 'Table'):
        super().__init__(table)
        self._values = []
        self._returning = []

    def values(self, *columns: Union[str, Any]) -> 'InsertQuery':
        """Set values to insert"""

        for column in columns:
            if isinstance(column, ColumnInfo):
                column = column._source_field_name
            
            if column not in self.table.__pgdantic_fields__:
                raise ValueError(f"Column '{column}' does not exist in table '{self.table.__name__}'")
            
            self._values.append(column)

        return self

    def returning(self, *columns: Union[str, Any]) -> 'InsertQuery':
        """Add RETURNING clause"""

        for column in columns:
            if isinstance(column, ColumnInfo):
                column = column._source_field_name

            if column not in self.table.__pgdantic_fields__:
                raise ValueError(f"Column '{column}' does not exist in table '{self.table.__name__}'")

            self._returning.append(column)

        return self

    def build(self) -> Tuple[str, List[Any]]:
        if not self._values:
            raise ValueError("No values provided for INSERT")
        
        columns = self._values
        placeholders = ['%s'] * len(columns)
        params = [getattr(self.table, column) for column in columns]

        sql = f"INSERT INTO {self.table.Meta.table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        if self._returning:
            sql += f" RETURNING {', '.join(self._returning)}"
        
        return sql, params