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
    from .select import Select

__all__ = ["Insert", "BulkInsert"]

class Insert(Query):
    """SQL INSERT query builder focused on Pydantic model instances."""
    
    def __init__(self, table: 'Table'):
        """Initialize with a Pydantic model instance."""
        self.table = table
        
        self._columns: Optional[List[str]] = None  # None means all columns
        self._overrides: Dict[str, Any] = {}  # Manual overrides for specific columns

        self._returning: Optional[List[str]] = None
        self._on_conflict_target: Optional[List[str]] = None
        self._on_conflict_action: Optional[ConflictAction] = None
        self._on_conflict_update: Optional[Dict[str, Any]] = None
        self._select_query: Optional['Select'] = None

    def build(self) -> Tuple[str, List[Any]]:
        """Build the complete SQL INSERT statement."""
        params = []
        
        if self._columns:
            # Only include specified columns
            model_data = self.table.model_dump(mode='json', include=set(self._columns))
        else:
            # Include all columns from the model
            model_data = self.table.model_dump(mode='json')

        model_data.update(self._overrides)
        columns = [col for col in model_data.keys() if col in self.table.__pgantics_fields__]

        if not columns:
            raise ValueError("No valid database columns found to insert")

        # Build the main INSERT statement
        sql_parts = [
            f"INSERT INTO {self.table.Meta.table_name}",
            f"({', '.join(columns)})"
        ]

        if self._select_query:
            select_sql, select_params = self._select_query.build()
            sql_parts.append(select_sql)  # No VALUES keyword needed
            params.extend(select_params)
        else:
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

        dump.update(self._overrides)

        # Ensure we have values for all columns in the right order
        return [[dump.get(col) for col in columns]]

    def insert(self, *columns: Union[str, ColumnInfo]) -> Self:
        """Specify columns to insert.

        Args:
            *columns: Column names (strings) or ColumnInfo objects to update

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

    def override(self, updates: Dict[Union[str, ColumnInfo], Any]) -> Self:
        """Override specific columns with new values.

        Args:
            updates: Dictionary of column names or ColumnInfo objects to their new values

        Example:
        ```
            user.insert().override({'email': 'new@example.com', 'updated_at': funcs.Now()}) # inserts all columns, but overrides email and updated at
            user.insert(User.id, "email").override({User.last_login: funcs.Now()})
        ```
        """

        for key, val in updates.items():
            if isinstance(key, ColumnInfo):
                column_name = key._source_field
            elif isinstance(key, str):
                if '.' in key:
                    _, column_name = key.split('.', 1)
                else:
                    column_name = key
            else:
                raise TypeError(f"Expected string or ColumnInfo key, got {type(key).__name__}")

            if column_name not in self.table.__pgantics_fields__:
                raise ValueError(f"Column '{column_name}' does not exist in table '{self.table.Meta.table_name}'")

            self._overrides[column_name] = val

        return self

    def on_conflict(self, targets: Union[str, ColumnInfo, Iterable[Union[str, ColumnInfo]]]) -> 'OnConflict[Self]':
        """Add ON CONFLICT clause."""
        if not isinstance(targets, Iterable):
            targets = [targets]

        correct_targets: List[str] = []
        for target_item in targets:
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

            correct_targets.append(target_item._source_field)

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
    
    def from_select(self, query: 'Select') -> Self:
        """Specifies a SELECT query as the source for an INSERT statement, rather than the pydantic model dump.
        
        Example:
        ```
            # Simple SELECT insert
            User.insert('email', 'name', 'created_at').from_select(
                LegacyUser.select('email_address', 'full_name', 'signup_date')
                .where(LegacyUser.active == True)
            )

            # With ON CONFLICT (works with both patterns)
            User.insert('email', 'name').from_select(
                ImportedUser.select('email', 'name')
            ).on_conflict('email').do_update({'name': 'EXCLUDED.name'})

            # Mixed with expressions in SELECT
            User.insert('email', 'name', 'created_at').from_select(
                LegacyUser.select(
                    'email_address',
                    funcs.Concat(LegacyUser.first_name, ' ', LegacyUser.last_name),
                    funcs.Now()
                )
            )
        ```    
        """
        self._select_query = query
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

    def from_select(self, query: 'Select') -> Self:
        raise NotImplementedError("BulkInsert does not support from_select method")

class OnConflict[Q: Insert]:
    def __init__(self, query: Q, target: List[str]):
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