from typing import (
    TYPE_CHECKING,
    Any,
    List,
    Literal,
    Optional,
    Self,
    Tuple,
    Type,
    Union,
    overload,
)

from ..entities.column import ColumnInfo
from ..entities.expression import BaseExpression, Expression, OrderExpression
from ..enums import JoinType
from ..registry import TABLE_REGISTRY
from .base import Query

if TYPE_CHECKING:
    from ..entities.table import Table

__all__ = ["Select"]

class Select(Query):
    """SQL SELECT query builder with fluent interface."""
    
    def __init__(self, table: Type['Table']):
        self.table = table
        self._select_columns: List[BaseExpression] = []
        self._joins: List['Join'] = []
        self._where_conditions: List[BaseExpression] = []
        self._order_by_expressions: List[OrderExpression] = []
        self._limit_value: Optional[int] = None
        self._offset_value: Optional[int] = None
        self._distinct: bool = False
        self._group_by_expressions: List[BaseExpression] = []
        self._having_conditions: List[BaseExpression] = []

    def build(self) -> Tuple[str, List[Any]]:
        """
        Build the complete SQL SELECT statement.

        Example:
        ```
            sql, params = User.select().where(User.age > 18).build()
            sql, params = User.select().where(User.email.like('%@example.com')).build()
            sql, params = User.select().order_by(User.name).limit(10).build()
        ```
        """
        sql_parts = []
        params = []
        
        # SELECT clause
        select_part = "SELECT"
        if self._distinct:
            select_part += " DISTINCT"
        
        if self._select_columns:
            column_sqls = []
            for col in self._select_columns:
                if isinstance(col, ColumnInfo):
                    column_sqls.append(str(col))
                elif isinstance(col, BaseExpression):
                    col_sql, col_params = col.build()

                    column_sqls.append(col_sql)
                    params.extend(col_params)
                else:
                    raise TypeError(f"Invalid column type: {type(col)}")
            
            select_part += f" {', '.join(column_sqls)}"
        else:
            # Default to all columns from main table
            select_part += " *"
        
        sql_parts.append(select_part)
        
        # FROM clause
        sql_parts.append(f"FROM {self.table.Meta.table_name}")
        
        # JOIN clauses
        for join in self._joins:
            join_sql, join_params = join._build()
            sql_parts.append(join_sql)
            params.extend(join_params)
        
        # WHERE clause
        if self._where_conditions:
            where_sqls = []
            for condition in self._where_conditions:
                cond_sql, cond_params = condition.build()
                where_sqls.append(f"({cond_sql})")
                params.extend(cond_params)
            sql_parts.append(f"WHERE {' AND '.join(where_sqls)}")
        
        # GROUP BY clause
        if self._group_by_expressions:
            group_sqls = []
            for expr in self._group_by_expressions:
                expr_sql, expr_params = expr.build()
                group_sqls.append(expr_sql)
                params.extend(expr_params)
            sql_parts.append(f"GROUP BY {', '.join(group_sqls)}")
        
        # HAVING clause
        if self._having_conditions:
            having_sqls = []
            for condition in self._having_conditions:
                cond_sql, cond_params = condition.build()
                having_sqls.append(f"({cond_sql})")
                params.extend(cond_params)
            sql_parts.append(f"HAVING {' AND '.join(having_sqls)}")
        
        # ORDER BY clause
        if self._order_by_expressions:
            order_sqls = []
            for order_expr in self._order_by_expressions:
                order_sql, order_params = order_expr.build()
                order_sqls.append(order_sql)
                params.extend(order_params)
            sql_parts.append(f"ORDER BY {', '.join(order_sqls)}")
        
        # LIMIT clause
        if self._limit_value is not None:
            sql_parts.append("LIMIT %s")
            params.append(self._limit_value)
        
        # OFFSET clause
        if self._offset_value is not None:
            sql_parts.append("OFFSET %s")
            params.append(self._offset_value)
        
        return " ".join(sql_parts), params

    def select(self, *columns: Union[str, BaseExpression]) -> Self:
        """Specify columns to select.
        
        Args:
            *columns: Column names (strings) or expressions to select
            
        Example:
        ```
            User.select('id', 'name', User.email, func.Count())
        ```
        """
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

            self._select_columns.append(col)

        return self

    def distinct(self) -> Self:
        """Add DISTINCT modifier to SELECT."""
        self._distinct = True
        return self

    @overload
    def join(self, table: Union[Type['Table'], str], join_type: JoinType = JoinType.INNER) -> 'Join':
        """Start building an INNER JOIN."""
        ...

    @overload  
    def join(self, table: Union[Type['Table'], str], join_type: Literal[JoinType.NATURAL, JoinType.CROSS]) -> Self:
        """Start building a JOIN with specified type."""
        ...

    def join(self, table: Union[Type['Table'], str], join_type: JoinType = JoinType.INNER) -> Union['Join', Self]:
        """Start building a JOIN clause.
        
        Args:
            table: The table to join
            join_type: Type of join (INNER, LEFT, etc.)
            
        Returns:
            Join object for chaining .on() condition
            
        Example:
        ```
            User.select().join(Post).on(Post.user_id == User.id)
            User.select().join(Profile, JoinType.LEFT).on(Profile.user_id == User.id)
        ```
        """

        if isinstance(table, str):
            table = TABLE_REGISTRY.get(table)

        join = Join(self, table, join_type)
        self._joins.append(join)

        if join_type in (JoinType.NATURAL, JoinType.CROSS):
            # For NATURAL or CROSS joins, we don't need an ON condition
            return self
        return join

    def where(self, condition: BaseExpression) -> Self:
        """Add WHERE condition.
        
        Args:
            condition: Boolean expression for filtering
            
        Example:
        ```
            User.select().where(User.age > 18)
            User.select().where(User.email.like('%@example.com'))
        ```
        """
        self._where_conditions.append(condition)
        return self

    def group_by(self, *expressions: Union[str, BaseExpression]) -> Self:
        """Add GROUP BY expressions.
        
        Args:
            *expressions: Column names or expressions to group by
            
        Example:
        ```
            User.select().group_by('department', User.age)
        ```
        """
        for expr in expressions:
            if isinstance(expr, str):
                # Convert string to expression
                if '.' in expr:
                    self._group_by_expressions.append(Expression(expr))
                else:
                    self._group_by_expressions.append(Expression(f"{self.table.Meta.table_name}.{expr}"))
            else:
                self._group_by_expressions.append(expr)
        return self

    def having(self, condition: BaseExpression) -> Self:
        """Add HAVING condition (used with GROUP BY).
        
        Args:
            condition: Boolean expression for filtering grouped results
            
        Example:
        ```
            User.select().group_by('department').having(func.Count() > 5)
        ```
        """
        self._having_conditions.append(condition)
        return self

    def order_by(self, *expressions: Union[str, BaseExpression]) -> Self:
        """Add ORDER BY expressions.
        
        Args:
            *expressions: Column names and/or expressions to order by
            
        Example:
        ```
            User.select().order_by('name')  # ASC by default
            User.select().order_by(User.created_at.desc())
            User.select().order_by(User.name.asc(), User.age.desc())
        ```
        """
        for expr in expressions:
            if isinstance(expr, OrderExpression):
                self._order_by_expressions.append(expr)
            elif isinstance(expr, str):
                # Convert string to ascending order expression
                if '.' in expr:
                    base_expr = Expression(expr)
                else:
                    base_expr = Expression(f"{self.table.Meta.table_name}.{expr}")
                self._order_by_expressions.append(base_expr.asc())
            elif isinstance(expr, Expression):
                # Default to ascending order
                self._order_by_expressions.append(expr.asc())
            else:
                raise TypeError(f"Invalid order expression type: {type(expr)}")
        return self

    def limit(self, count: int) -> Self:
        """Add LIMIT clause.
        
        Args:
            count: Maximum number of rows to return
            
        Example:
        ```
            User.select().limit(10)
        ```
        """
        if count < 0:
            raise ValueError("LIMIT count must be non-negative")
        self._limit_value = count
        return self

    def offset(self, count: int) -> Self:
        """Add OFFSET clause.
        
        Args:
            count: Number of rows to skip
            
        Example:
        ```
            User.select().offset(20)
        ```
        """
        if count < 0:
            raise ValueError("OFFSET count must be non-negative")
        self._offset_value = count
        return self

    def count(self) -> Self:
        """Convert query to COUNT(*) query."""
        self._select_columns = [Expression("COUNT(*)")]
        return self


class Join:
    """Represents a JOIN clause in a SELECT query."""
    
    def __init__(self, select_query: Select, table: Type['Table'], join_type: JoinType):
        self.select_query = select_query
        self.table = table
        self.join_type = join_type
        self.on_condition: Optional[BaseExpression] = None

    def _build(self) -> Tuple[str, List[Any]]:
        """Build the JOIN SQL clause."""

        if self.join_type in (JoinType.NATURAL, JoinType.CROSS):
            # For NATURAL or CROSS joins, we don't need an ON condition
            return f"{self.join_type.value} JOIN {self.table.Meta.table_name}", []

        if self.on_condition is None:
            raise ValueError(f"JOIN with {self.table.Meta.table_name} is missing ON condition")
        
        condition_sql, params = self.on_condition.build()
        sql = f"{self.join_type.value} JOIN {self.table.Meta.table_name} ON {condition_sql}"
        return sql, params

    def on(self, condition: BaseExpression) -> Select:
        """Specify the JOIN condition.
        
        Args:
            condition: Boolean expression for the join condition
            
        Returns:
            The original Select query for method chaining
            
        Example:
        ```
            User.select().join(Post).on(Post.user_id == User.id)
        ```
        """
        self.on_condition = condition
        return self.select_query