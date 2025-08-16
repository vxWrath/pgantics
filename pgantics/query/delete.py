from typing import TYPE_CHECKING, Any, List, Optional, Self, Tuple, Type, Union

from ..entities.column import ColumnInfo
from ..entities.expression import BaseExpression
from ..registry import TABLE_REGISTRY
from .base import Query

if TYPE_CHECKING:
    from ..entities.table import Table

__all__ = ["Delete"]

class Delete(Query):
    """SQL DELETE query builder with fluent interface."""

    def __init__(self, table: Type['Table']):
        self.table = table

        self._where_conditions: List[BaseExpression] = []
        self._joins: List['DeleteJoin[Self]'] = []
        self._returning: Optional[List[str]] = None

    def build(self) -> Tuple[str, List[Any]]:
        """
        Build the complete SQL DELETE statement.

        Example:
        ```
            sql, params = User.delete().where(User.age < 18).build()
            sql, params = User.delete().where(User.email.like('%@spam.com')).build()
        ```
        """

        sql_parts = []
        params = []
        
        # DELETE FROM clause
        if self._joins:
            # DELETE with JOINs using USING clause
            sql_parts.append(f"DELETE FROM {self.table.Meta.table_name}")
            
            # Add USING clause with all joined tables
            using_tables = [join.table.Meta.table_name for join in self._joins]
            sql_parts.append(f"USING {', '.join(using_tables)}")
            
            # Add all JOIN conditions to WHERE clause
            for join in self._joins:
                if join.on_condition:
                    self._where_conditions.append(join.on_condition)
        else:
            # Simple DELETE
            sql_parts.append(f"DELETE FROM {self.table.Meta.table_name}")
        
        # WHERE clause
        if self._where_conditions:
            where_sqls = []
            for condition in self._where_conditions:
                cond_sql, cond_params = condition.build()
                where_sqls.append(f"({cond_sql})")
                params.extend(cond_params)
            sql_parts.append(f"WHERE {' AND '.join(where_sqls)}")
        else:
            # Safety check - require WHERE clause to prevent accidental full table deletion
            raise ValueError("DELETE query must include a WHERE clause. Use delete_all() for intentional full table deletion.")
        
        # RETURNING clause
        if self._returning:
            if '*' in self._returning:
                sql_parts.append("RETURNING *")
            else:
                sql_parts.append(f"RETURNING {', '.join(self._returning)}")

        return " ".join(sql_parts), params
    
    def join(self, table: Union[Type['Table'], str]) -> 'DeleteJoin[Self]':
        """Start building a JOIN clause.
        
        Args:
            table: The table to join
            
        Returns:
            Join object for chaining .on() condition
            
        Example:
        ```
            # Delete users who have no posts
            User.delete().join(Post).on(Post.user_id == User.id).where(Post.id.is_null())
            
            # Delete posts from inactive users
            Post.delete().join(User).on(Post.user_id == User.id).where(User.active == False)
        ```
        """

        if isinstance(table, str):
            table = TABLE_REGISTRY.get(table)

        join = DeleteJoin(self, table)
        self._joins.append(join)

        return join

    def where(self, condition: BaseExpression) -> Self:
        """Add WHERE condition.
        
        Args:
            condition: Boolean expression for filtering
            
        Example:
        ```
            User.delete().where(User.age < 18)
            User.delete().where(User.email.like('%@spam.com'))
            User.delete().where((User.last_login < datetime.now() - timedelta(days=365)) & (User.active == False))
        ```
        """
        self._where_conditions.append(condition)
        return self
    
    def delete_all(self) -> Self:
        """Allow deletion of all rows in the table without WHERE clause.
        
        This is a safety method to explicitly allow full table deletion.
        
        Example:
        ```
            # Dangerous - deletes all users!
            User.delete().delete_all()
        ```
        """
        # Override the WHERE clause requirement by adding a always-true condition
        from ..entities.expression import LiteralExpression
        self._where_conditions.append(LiteralExpression(True))
        return self
    
    def returning(self, *columns: Union[str, ColumnInfo]) -> Self:
        """Set columns to return after delete.
        
        Args:
            *columns: Column names or ColumnInfo objects to return
            
        Example:
        ```
            User.delete().where(User.age < 18).returning('id', 'email')
            User.delete().where(User.active == False).returning('*')
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
    
class DeleteJoin[Q: Delete]:
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
            User.delete().join(Post).on(Post.user_id == User.id)
            User.delete().join(Post).on((Post.user_id == User.id) & (Post.status == 'inactive'))
        ```
        """
        self.on_condition = condition
        return self.query