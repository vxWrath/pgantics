from typing import TYPE_CHECKING, Any, List, Optional, Self, Tuple, Union

from ..entities.column import ColumnInfo
from .base import Query

if TYPE_CHECKING:
    from ..entities.table import Table

__all__ = ["Insert"]

class Insert(Query):
    """SQL INSERT query builder with fluent interface."""
    
    def __init__(self, table: 'Table'):
        self.table = table

        self._columns: List[str] = []
        self._returning: Optional[List[str]] = None

    def build(self) -> Tuple[str, List[Any]]:
        if not self._columns:
            dump = self.table.model_dump(mode='json')
        else:
            dump = self.table.model_dump(mode='json', include=set(self._columns))

        placeholders = ['%s'] * len(dump)

        parts = [
            f"INSERT INTO {self.table.Meta.table_name} "
            f"({', '.join(dump)})", 
            f"VALUES ({', '.join(placeholders)})"
        ]

        if self._returning:
            parts.append(f"RETURNING {', '.join(self._returning)}")

        return " ".join(parts), list(dump.values())

    def insert(self, *columns: Union[str, ColumnInfo]) -> Self:
        """Set the columns to insert values into."""

        for col in columns:
            if isinstance(col, str):
                if '.' in col:
                    _, column = col.split('.', 1)
                else:
                    column = col
            elif isinstance(col, ColumnInfo):
                column = col._source_field
            else:
                raise TypeError(f"Expected string or ColumnInfo, instead got {type(col).__name__}")

            if column not in self.table.__pgantics_fields__:
                raise ValueError(f"Column '{column}' does not exist in table '{self.table.Meta.table_name}'")
            
            self._columns.append(column)

        return self

    def returning(self, *columns: Union[str, ColumnInfo]) -> Self:
        """Set the columns to return after the insert.
        
        Args:
            *columns: Column names or expressions to return
            
        Example:
        ```
            User.insert().returning(User.id, 'created_at')
            User.insert().returning() # returns all columns
        ```
        """
        if not columns:
            self._returning = None
            return self

        self._returning = []
        for col in columns:
            if isinstance(col, str):
                if '.' in col:
                    _, column = col.split('.', 1)
                else:
                    column = col
            elif isinstance(col, ColumnInfo):
                column = col._source_field
            else:
                raise TypeError(f"Expected string or ColumnInfo, instead got {type(col).__name__}")

            if column not in self.table.__pgantics_fields__:
                raise ValueError(f"Column '{column}' does not exist in table '{self.table.Meta.table_name}'")

            self._returning.append(column)

        return self