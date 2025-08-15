from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    List,
    Optional,
    Self,
    Tuple,
    Union,
)

from ..entities.column import ColumnInfo
from ..entities.expression import BaseExpression
from ..registry import TABLE_REGISTRY
from .base import Query

from..enums import ConflictAction

if TYPE_CHECKING:
    from ..entities.table import Table

__all__ = ["Insert"]

class Insert(Query):
    """SQL INSERT query builder focused on Pydantic model instances."""
    
    def __init__(self, table: 'Table'):
        """Initialize with a Pydantic model instance."""
        self.table = table
        
        self._columns: Optional[List[str]] = None  # None means all columns

        self._returning: Optional[List[str]] = None
        self._on_conflict_target: Optional[Union[str, List[str]]] = None
        self._on_conflict_action: Optional[ConflictAction] = None
        self._on_conflict_update: Optional[Dict[str, Any]] = None

    def build(self) -> Tuple[str, List[Any]]:
        """Build the complete SQL INSERT statement."""
        params = []
        
        # Get data from the first instance to determine columns
        if self._columns:
            # Only include specified columns
            first_dump = self.table.model_dump(mode='json', include=set(self._columns))
            columns = self._columns
        else:
            # Include all columns from the model
            first_dump = self.table.model_dump(mode='json')
            # Filter to only include columns that are actually database columns
            columns = [col for col in first_dump.keys() if col in self.table.__pgantics_fields__]
            first_dump = {col: first_dump[col] for col in columns}
        
        if not columns:
            raise ValueError("No valid database columns found to insert")

        # Build the main INSERT statement
        sql_parts = [
            f"INSERT INTO {self.table.Meta.table_name}",
            f"({', '.join(columns)})"
        ]

        values = self._build_values(self.table, columns)
        
        # Handle single vs multiple rows
        if len(values) == 1:
            placeholders = ', '.join(['%s'] * len(columns))
            sql_parts.append(f"VALUES ({placeholders})")
            params.extend(values[0])
        else:
            value_groups = []
            for row_values in values:
                placeholders = ', '.join(['%s'] * len(row_values))
                value_groups.append(f"({placeholders})")
                params.extend(row_values)
            sql_parts.append(f"VALUES {', '.join(value_groups)}")

        # ON CONFLICT clause
        if self._on_conflict_target is not None:
            if isinstance(self._on_conflict_target, str):
                conflict_target = self._on_conflict_target
            else:
                conflict_target = f"({', '.join(self._on_conflict_target)})"
            
            sql_parts.append(f"ON CONFLICT {conflict_target}")
            
            if self._on_conflict_action == "DO NOTHING":
                sql_parts.append("DO NOTHING")
            elif self._on_conflict_action == "DO UPDATE":
                if not self._on_conflict_update:
                    raise ValueError("ON CONFLICT DO UPDATE requires SET clause")
                
                set_clauses = []
                for col, val in self._on_conflict_update.items():
                    if isinstance(val, BaseExpression):
                        val_sql, val_params = val.build()
                        set_clauses.append(f"{col} = {val_sql}")
                        params.extend(val_params)
                    else:
                        set_clauses.append(f"{col} = %s")
                        params.append(val)
                
                sql_parts.append(f"DO UPDATE SET {', '.join(set_clauses)}")

        # RETURNING clause
        if self._returning:
            if '*' in self._returning:
                sql_parts.append("RETURNING *")
            else:
                sql_parts.append(f"RETURNING {', '.join(self._returning)}")

        return " ".join(sql_parts), params

    def _build_values(self, table: 'Table', columns: List[str]) -> List[List[Any]]:
        if self._columns:
            dump = table.model_dump(mode='json', include=set(self._columns))
        else:
            dump = table.model_dump(mode='json')
            dump = {col: dump[col] for col in columns if col in dump}

        # Ensure we have values for all columns in the right order
        return [[dump.get(col) for col in columns]]

    def insert(self, *columns: Union[str, ColumnInfo]) -> Self:
        """Specify columns to insert.

        Args:
            *columns: Column names (strings) or expressions to insert

        Example:
        ```
            User.insert('id', 'name', User.email)
            User.insert() # Inserts all
        ```
        """
        if not columns:
            self._columns = None  # Insert all columns
            return self

        self._columns = []

        for col in columns:
            if isinstance(col, str):
                # Handle string column names
                if '.' in col:
                    table_name, column = col.split('.', 1)
                    table = TABLE_REGISTRY.get(table_name)
                else:
                    table = self.table
                    column = col

                if column not in table.__pgantics_fields__:
                    raise ValueError(f"Column '{column}' does not exist in table '{table.Meta.table_name}'")
                col = table.__pgantics_fields__[column]

            self._columns.append(str(col))

        return self

    def on_conflict(self, target: Union[str, ColumnInfo, Iterable[Union[str, ColumnInfo]]]) -> 'OnConflict[Self]':
        """Add ON CONFLICT clause."""
        if not isinstance(target, Iterable):
            target = [target]

        correct_targets: List[str] = []
        for target_item in target:
            if isinstance(target_item, str):
                if '.' in target_item:
                    table_name, column = target_item.split('.', 1)
                    table = TABLE_REGISTRY.get(table_name)
                else:
                    table = self.table
                    column = target_item

                if column not in table.__pgantics_fields__:
                    raise ValueError(f"Column '{column}' does not exist in table '{table.Meta.table_name}'")
                target_item = table.__pgantics_fields__[column]
                correct_targets.append(str(target_item))

        return OnConflict(self, correct_targets)

    def returning(self, *columns: Union[str, ColumnInfo]) -> Self:
        """Set columns to return after insert."""
        if not columns:
            self._returning = ['*']
            return self

        self._returning = []
        for col in columns:
            if isinstance(col, str):
                if col == '*':
                    self._returning = ['*']
                    return self
                elif '.' in col:
                    _, column = col.split('.', 1)
                else:
                    column = col
            elif isinstance(col, ColumnInfo):
                column = col._source_field
            else:
                raise TypeError(f"Expected string or ColumnInfo, got {type(col).__name__}")

            if column not in self.table.__pgantics_fields__:
                raise ValueError(f"Column '{column}' does not exist in table '{self.table.Meta.table_name}'")

            self._returning.append(column)

        return self
    
class BulkInsert[B: 'Table'](Insert):
    """Alias for Insert to indicate bulk insert usage."""
    def __init__(self, tables: Iterable[B]):
        if not tables:
            raise ValueError("At least one instance is required for bulk insert")

        self._tables = tables
        self._columns: Optional[List[str]] = None  # None means all columns

        self._returning: Optional[List[str]] = None
        self._on_conflict_target: Optional[Union[str, List[str]]] = None
        self._on_conflict_action: Optional[ConflictAction] = None
        self._on_conflict_update: Optional[Dict[str, Any]] = None

    def _build_values(self, columns: List[str]) -> List[List[Any]]:
        values: List[List[Any]] = []
        for table in self._tables:
            table_values = super()._build_values(table, columns)
            values.extend(table_values)

        return values
    
class OnConflict[Q: Insert]:
    def __init__(self, query: Q, target: Union[str, List[str]]):
        self.query = query
        self.target = target

    def do_nothing(self) -> Q:
        """Add ON CONFLICT DO NOTHING clause."""
        self.query._on_conflict_target = self.target
        self.query._on_conflict_action = ConflictAction.DO_NOTHING
        return self.query

    def do_update(self, updates: Dict[Union[str, ColumnInfo], Any]) -> Q:
        """Add ON CONFLICT DO UPDATE clause."""
        self.query._on_conflict_target = self.target
        self.query._on_conflict_action = ConflictAction.DO_UPDATE

        if not self.query._on_conflict_update:
            self.query._on_conflict_update = {}

        for key, val in updates.items():
            if isinstance(key, ColumnInfo):
                key = str(key)
            self.query._on_conflict_update[key] = val

        return self.query