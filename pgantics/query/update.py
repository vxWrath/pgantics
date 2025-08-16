from typing import TYPE_CHECKING, Any, Dict, List, Optional, Self, Tuple, Type, Union

from ..entities.column import ColumnInfo
from ..entities.expression import BaseExpression, to_expression
from ..registry import TABLE_REGISTRY
from .base import Query

if TYPE_CHECKING:
    from ..entities.table import Table

__all__ = ["Update"]

class Update(Query):
    """SQL UPDATE query builder focused on Pydantic model instances."""

    def __init__(self, table: 'Table'):
        self.table = table

        self._columns: Optional[List[str]] = None  # None means all columns
        self._overrides: Dict[str, Any] = {}  # Manual overrides for specific columns

        self._joins: List['UpdateJoin[Self]'] = []
        self._where_conditions: List[BaseExpression] = []
        self._returning: Optional[List[str]] = None

    def build(self) -> Tuple[str, List[Any]]:
        params = []
        
        if self._columns:
            # Only include specified columns
            model_data = self.table.model_dump(mode='json', include=set(self._columns))
        else:
            # Include all columns except primary keys by default
            pks = {k for k, v in self.table.__pgantics_fields__.items() if v.sql_data.get('primary_key', False)}
            model_data = self.table.model_dump(mode='json', exclude=pks)
            
        model_data.update(self._overrides)
        
        if not model_data:
            raise ValueError("No columns found to update")

        # Build the main UPDATE statement
        sql_parts = [f"UPDATE {self.table.Meta.table_name}"]
        
        # SET clause
        set_clauses = []
        for col, val in model_data.items():
            if not isinstance(val, BaseExpression):
                val = to_expression(val)

            val_sql, val_params = val.build()
            set_clauses.append(f"{col} = {val_sql}")
            params.extend(val_params)
        
        sql_parts.append(f"SET {', '.join(set_clauses)}")
        
        # FROM clause (for JOINs)
        if self._joins:
            from_tables = [join.table.Meta.table_name for join in self._joins]
            sql_parts.append(f"FROM {', '.join(from_tables)}")
            
            # Add all JOIN conditions to WHERE clause
            for join in self._joins:
                if join.on_condition:
                    self._where_conditions.append(join.on_condition)
        
        # WHERE clause
        if self._where_conditions:
            where_sqls = []
            for condition in self._where_conditions:
                cond_sql, cond_params = condition.build()
                where_sqls.append(f"({cond_sql})")
                params.extend(cond_params)
            sql_parts.append(f"WHERE {' AND '.join(where_sqls)}")
        else:
            # Safety check - require WHERE clause to prevent accidental full table update
            raise ValueError("UPDATE query must include a WHERE clause. Use update_all() for intentional full table update.")
        
        # RETURNING clause
        if self._returning:
            if '*' in self._returning:
                sql_parts.append("RETURNING *")
            else:
                sql_parts.append(f"RETURNING {', '.join(self._returning)}")

        return " ".join(sql_parts), params

    def update(self, *columns: Union[str, ColumnInfo]) -> Self:
        """Specify columns to update.

        Args:
            *columns: Column names (strings) or ColumnInfo objects to update

        Example:
        ```
            user.update('email', 'first_name')  # Only update these columns
            user.update()  # Update all non-primary key columns
        ```
        """
        if not columns:
            self._columns = None  # Update all columns
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
            user.update().override({'email': 'new@example.com', 'updated_at': funcs.Now()})
            user.update().override({User.last_login: funcs.Now()})
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
    
    def join(self, table: Union[Type['Table'], str]) -> 'UpdateJoin[Self]':
        """Start building a JOIN clause for UPDATE with FROM.
        
        Args:
            table: The table to join
            
        Returns:
            Join object for chaining .on() condition
            
        Example:
        ```
            # Update posts based on user data
            post.update().join(User).on(Post.user_id == User.id).where(User.active == False)
        ```
        """
        if isinstance(table, str):
            table = TABLE_REGISTRY.get(table)

        join = UpdateJoin(self, table)
        self._joins.append(join)

        return join
    
    def where(self, condition: BaseExpression) -> Self:
        """Add WHERE condition.
        
        Args:
            condition: Boolean expression for filtering
            
        Example:
        ```
            user.update().where(User.id == 123)
            user.update().where(User.email.like('%@example.com'))
        ```
        """
        self._where_conditions.append(condition)
        return self
    
    def returning(self, *columns: Union[str, ColumnInfo]) -> Self:
        """Set columns to return after update.
        
        Args:
            *columns: Column names or ColumnInfo objects to return
            
        Example:
        ```
            user.update().where(User.id == 123).returning('id', 'email', 'updated_at')
            user.update().where(User.active == True).returning('*')
        ```
        """
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
    
class UpdateJoin[Q: Update]:
    def __init__(self, query: Q, table: Type['Table']):
        self.query = query
        self.table = table
        self.on_condition: Optional[BaseExpression] = None

    def on(self, condition: BaseExpression) -> Q:
        """Specify the JOIN condition.

        Args:
            condition: Boolean expression for the join condition

        Returns:
            The original query for method chaining

        Example:
        ```
            post.update().join(User).on(Post.user_id == User.id)
        ```
        """
        self.on_condition = condition
        return self.query