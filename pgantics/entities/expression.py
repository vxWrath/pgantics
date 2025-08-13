from abc import ABC, abstractmethod
from typing import Any, List, Optional, Sequence, Tuple, Union

from ..enums import BinaryOperator, Operator, Order

__all__ = [
    'funcs',
    'to_expression',
    'literal',
    'null',
    'case',
    'func'
]

class BaseExpression(ABC):
    """Abstract base class for all SQL expressions."""
    
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def build(self) -> Tuple[str, List[Any]]:
        """Build SQL string and parameter list."""
        pass

    def __str__(self) -> str:
        sql, _ = self.build()
        return f"{self.__class__.__name__}({sql})"
    
class Expression(BaseExpression):
    """Base concrete expression class with operator overloading."""
    
    def __init__(self, sql: Optional[str] = None, params: Optional[List[Any]] = None):
        self.sql = sql or str()
        self.params = params or []
    
    def build(self) -> Tuple[str, List[Any]]:
        return self.sql, self.params.copy()
    
    def as_alias(self, alias: str) -> 'Alias':
        """Create an aliased version of this expression."""
        return Alias(self, alias)

    # Arithmetic operators
    def __add__(self, other) -> 'BinaryExpression':
        return BinaryExpression(self, BinaryOperator.ADD, to_expression(other))

    def __sub__(self, other) -> 'BinaryExpression':
        return BinaryExpression(self, BinaryOperator.SUBTRACT, to_expression(other))

    def __mul__(self, other) -> 'BinaryExpression':
        return BinaryExpression(self, BinaryOperator.MULTIPLY, to_expression(other))

    def __truediv__(self, other) -> 'BinaryExpression':
        return BinaryExpression(self, BinaryOperator.DIVIDE, to_expression(other))

    def __mod__(self, other) -> 'BinaryExpression':
        return BinaryExpression(self, BinaryOperator.MODULUS, to_expression(other))
    
    def __pow__(self, other) -> 'BinaryExpression':
        return BinaryExpression(self, BinaryOperator.POWER, to_expression(other))
    
    # Reverse arithmetic operators
    def __radd__(self, other) -> 'BinaryExpression':
        return BinaryExpression(to_expression(other), BinaryOperator.ADD, self)
    
    def __rsub__(self, other) -> 'BinaryExpression':
        return BinaryExpression(to_expression(other), BinaryOperator.SUBTRACT, self)
    
    def __rmul__(self, other) -> 'BinaryExpression':
        return BinaryExpression(to_expression(other), BinaryOperator.MULTIPLY, self)
    
    def __rtruediv__(self, other) -> 'BinaryExpression':
        return BinaryExpression(to_expression(other), BinaryOperator.DIVIDE, self)
    
    def __rmod__(self, other) -> 'BinaryExpression':
        return BinaryExpression(to_expression(other), BinaryOperator.MODULUS, self)
    
    def __rpow__(self, other) -> 'BinaryExpression':
        return BinaryExpression(to_expression(other), BinaryOperator.POWER, self)
    
    # Comparison operators
    def __eq__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.EQ, to_expression(other))
    
    def __ne__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.NEQ, to_expression(other))
    
    def __lt__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.LT, to_expression(other))

    def __le__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.LTE, to_expression(other))

    def __gt__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.GT, to_expression(other))

    def __ge__(self, other: Any) -> 'Condition':
        return Condition(self, Operator.GTE, to_expression(other))

    # SQL-specific methods
    def in_(self, values: Union[Sequence[Any], 'BaseExpression']) -> 'Condition':
        """IN operator - check if expression value is in a list of values."""
        if isinstance(values, BaseExpression):
            return Condition(self, Operator.IN, values)
        elif isinstance(values, (list, tuple)):
            if not values:
                # Empty IN clause should be FALSE
                return Condition(LiteralExpression('1'), Operator.EQ, LiteralExpression('0'))
            placeholders = ', '.join(['%s'] * len(values))
            return Condition(self, Operator.IN, Expression(f'({placeholders})', list(values)))
        else:
            raise TypeError(f"IN operator requires a sequence or expression, got {type(values)}")

    def not_in_(self, values: Union[Sequence[Any], 'BaseExpression']) -> 'Condition':
        """NOT IN operator."""
        if isinstance(values, BaseExpression):
            return Condition(self, Operator.NOT_IN, values)
        elif isinstance(values, (list, tuple)):
            if not values:
                # Empty NOT IN clause should be TRUE
                return Condition(LiteralExpression('1'), Operator.EQ, LiteralExpression('1'))
            placeholders = ', '.join(['%s'] * len(values))
            return Condition(self, Operator.NOT_IN, Expression(f'({placeholders})', list(values)))
        else:
            raise TypeError(f"NOT IN operator requires a sequence or expression, got {type(values)}")

    def like(self, pattern: Union[str, 'BaseExpression']) -> 'Condition':
        """LIKE pattern matching."""
        return Condition(self, Operator.LIKE, to_expression(pattern))

    def ilike(self, pattern: Union[str, 'BaseExpression']) -> 'Condition':
        """ILIKE case-insensitive pattern matching."""
        return Condition(self, Operator.ILIKE, to_expression(pattern))
    
    def is_null(self) -> 'Condition':
        """IS NULL check."""
        return Condition(self, Operator.IS_NULL, NullExpression())

    def is_not_null(self) -> 'Condition':
        """IS NOT NULL check."""
        return Condition(self, Operator.IS_NOT_NULL, NullExpression())
    
    def between(self, start: Any, end: Any) -> 'Condition':
        """BETWEEN operator."""
        start_expr = to_expression(start)
        end_expr = to_expression(end)

        start_sql, start_params = start_expr.build()
        end_sql, end_params = end_expr.build()
        
        between_value = Expression(f'{start_sql} AND {end_sql}', start_params + end_params)
        return Condition(self, Operator.BETWEEN, between_value)
    
    def not_between(self, start: Any, end: Any) -> 'NotCondition':
        """NOT BETWEEN operator."""
        return ~self.between(start, end)
    
    # Ordering
    def asc(self) -> 'OrderExpression':
        """Create ascending order expression."""
        return OrderExpression(self, Order.ASC)
    
    def desc(self) -> 'OrderExpression':
        """Create descending order expression."""
        return OrderExpression(self, Order.DESC)

    # Unary operators
    def __neg__(self) -> 'UnaryExpression':
        """Unary minus (-expression)."""
        return UnaryExpression('-', self)
    
    def __pos__(self) -> 'UnaryExpression':
        """Unary plus (+expression)."""
        return UnaryExpression('+', self)

class LiteralExpression(Expression):
    """Expression for literal values (numbers, strings, etc.)."""
    
    def __init__(self, value: Any):
        self.value = value
    
    def build(self) -> Tuple[str, List[Any]]:
        if self.value is None:
            return 'NULL', []
        return '%s', [self.value]
    
class NullExpression(Expression):
    """Special expression for NULL values."""
    
    def __init__(self):
        pass
    
    def build(self) -> Tuple[str, List[Any]]:
        return 'NULL', []
        
class Alias(BaseExpression):
    """Aliased expression (expression AS alias)."""
    
    def __init__(self, expression: BaseExpression, alias: str):
        self.expression = expression
        self.alias = alias

    def build(self) -> Tuple[str, List[Any]]:
        expr_sql, params = self.expression.build()
        return f'{expr_sql} AS {self.alias}', params
    
class BinaryExpression(Expression):
    """Binary operation between two expressions."""
    
    def __init__(self, left: BaseExpression, operator: BinaryOperator, right: BaseExpression):
        self.left = left
        self.operator = operator
        self.right = right

    def build(self) -> Tuple[str, List[Any]]:
        left_sql, left_params = self.left.build()
        right_sql, right_params = self.right.build()

        sql = f'({left_sql} {self.operator.value} {right_sql})'
        return sql, left_params + right_params
    
class UnaryExpression(Expression):
    """Unary operation on an expression."""
    
    def __init__(self, operator: str, operand: BaseExpression):
        self.operator = operator
        self.operand = operand
    
    def build(self) -> Tuple[str, List[Any]]:
        operand_sql, params = self.operand.build()
        return f'{self.operator}({operand_sql})', params
    
class FunctionExpression(Expression):
    """SQL function call expression."""
    
    def __init__(self, name: str, *args: BaseExpression, distinct: bool = False, 
                 filter_condition: Optional['Condition'] = None, 
                 over_clause: Optional[str] = None):
        self.name = name.upper()
        self.args = list(args)
        self.distinct = distinct
        self.filter_condition = filter_condition
        self.over_clause = over_clause
    
    def build(self) -> Tuple[str, List[Any]]:
        params = []
        
        # Build arguments
        if self.args:
            arg_sqls = []
            for arg in self.args:
                arg_sql, arg_params = arg.build()
                arg_sqls.append(arg_sql)
                params.extend(arg_params)
            args_part = ', '.join(arg_sqls)
        else:
            args_part = ''
        
        # Add DISTINCT if specified
        if self.distinct and args_part:
            args_part = f'DISTINCT {args_part}'
        
        # Build base function call
        sql = f'{self.name}({args_part})'
        
        # Add FILTER clause if specified
        if self.filter_condition:
            filter_sql, filter_params = self.filter_condition.build()
            sql = f'{sql} FILTER (WHERE {filter_sql})'
            params.extend(filter_params)
        
        # Add OVER clause for window functions
        if self.over_clause:
            sql = f'{sql} OVER ({self.over_clause})'
        
        return sql, params
    
    def with_distinct(self) -> 'FunctionExpression':
        """Return a copy of this function with DISTINCT modifier."""
        return FunctionExpression(
            self.name, *self.args, 
            distinct=True, 
            filter_condition=self.filter_condition,
            over_clause=self.over_clause
        )
    
    def filter(self, condition: 'Condition') -> 'FunctionExpression':
        """Add a FILTER clause for aggregate functions."""
        return FunctionExpression(
            self.name, *self.args,
            distinct=self.distinct,
            filter_condition=condition,
            over_clause=self.over_clause
        )
    
    def over(self, clause: str) -> 'FunctionExpression':
        """Add an OVER clause for window functions."""
        return FunctionExpression(
            self.name, *self.args,
            distinct=self.distinct,
            filter_condition=self.filter_condition,
            over_clause=clause
        )


class CaseExpression(Expression):
    """SQL CASE expression."""
    
    def __init__(self):
        self.when_clauses: List[Tuple['Condition', BaseExpression]] = []
        self.else_clause: Optional[BaseExpression] = None
    
    def when(self, condition: 'Condition', value: Any) -> 'CaseExpression':
        """Add a WHEN clause."""
        self.when_clauses.append((condition, to_expression(value)))
        return self
    
    def else_(self, value: Any) -> 'CaseExpression':
        """Add an ELSE clause."""
        self.else_clause = to_expression(value)
        return self
    
    def build(self) -> Tuple[str, List[Any]]:
        if not self.when_clauses:
            raise ValueError("CASE expression must have at least one WHEN clause")
        
        sql_parts = ['CASE']
        params = []
        
        for condition, value in self.when_clauses:
            cond_sql, cond_params = condition.build()
            val_sql, val_params = value.build()
            
            sql_parts.append(f'WHEN {cond_sql} THEN {val_sql}')
            params.extend(cond_params)
            params.extend(val_params)
        
        if self.else_clause:
            else_sql, else_params = self.else_clause.build()
            sql_parts.append(f'ELSE {else_sql}')
            params.extend(else_params)
        
        sql_parts.append('END')
        
        return ' '.join(sql_parts), params


class Condition(BaseExpression):
    """SQL condition/predicate expression."""
    
    def __init__(self, left: BaseExpression, operator: Operator, right: BaseExpression):
        self.left = left
        self.operator = operator
        self.right = right

    def build(self) -> Tuple[str, List[Any]]:
        left_sql, left_params = self.left.build()
        right_sql, right_params = self.right.build()

        # Handle special operators
        if self.operator in [Operator.IS_NULL, Operator.IS_NOT_NULL]:
            # For IS NULL/IS NOT NULL, we don't need the right side
            op_str = 'IS NULL' if self.operator == Operator.IS_NULL else 'IS NOT NULL'
            sql = f'{left_sql} {op_str}'
            return sql, left_params
        
        sql = f'{left_sql} {self.operator.value} {right_sql}'
        return sql, left_params + right_params
    
    def __and__(self, other: 'Condition') -> 'ConditionTree':
        """Combine conditions with AND."""
        return ConditionTree('AND', self, other)
    
    def __or__(self, other: 'Condition') -> 'ConditionTree':
        """Combine conditions with OR."""
        return ConditionTree('OR', self, other)
    
    def __invert__(self) -> 'NotCondition':
        """Negate condition with NOT."""
        return NotCondition(self)
    
class ConditionTree(BaseExpression):
    """Tree of conditions combined with AND/OR."""
    
    def __init__(self, operator: str, left: BaseExpression, right: BaseExpression):
        self.operator = operator
        self.left = left
        self.right = right
    
    def build(self) -> Tuple[str, List[Any]]:
        left_sql, left_params = self.left.build()
        right_sql, right_params = self.right.build()
        
        sql = f'({left_sql}) {self.operator} ({right_sql})'
        return sql, left_params + right_params
    
    def __and__(self, other: BaseExpression) -> 'ConditionTree':
        return ConditionTree('AND', self, other)
    
    def __or__(self, other: BaseExpression) -> 'ConditionTree':
        return ConditionTree('OR', self, other)


class NotCondition(BaseExpression):
    """Negated condition."""
    
    def __init__(self, condition: BaseExpression):
        self.condition = condition
    
    def build(self) -> Tuple[str, List[Any]]:
        cond_sql, params = self.condition.build()
        return f'NOT ({cond_sql})', params
    
class OrderExpression(BaseExpression):
    """ORDER BY expression with direction."""

    def __init__(self, expression: BaseExpression, direction: Order):
        self.expression = expression
        self.direction = direction

    def build(self) -> Tuple[str, List[Any]]:
        expr_sql, params = self.expression.build()
        return f'{expr_sql} {self.direction}', params
    
class funcs:
    """Function factory for common SQL functions."""
    
    @staticmethod
    def Count(expr: Any = None) -> FunctionExpression:
        if expr is None:
            return FunctionExpression('COUNT', Expression('*'))
        return FunctionExpression('COUNT', to_expression(expr))
    
    @staticmethod
    def Sum(expr: Any) -> FunctionExpression:
        return FunctionExpression('SUM', to_expression(expr))
    
    @staticmethod
    def Avg(expr: Any) -> FunctionExpression:
        return FunctionExpression('AVG', to_expression(expr))
    
    @staticmethod
    def Min(expr: Any) -> FunctionExpression:
        return FunctionExpression('MIN', to_expression(expr))

    @staticmethod
    def Max(expr: Any) -> FunctionExpression:
        return FunctionExpression('MAX', to_expression(expr))
    
    @staticmethod
    def Concat(*args: Any) -> FunctionExpression:
        return FunctionExpression('CONCAT', *[to_expression(arg) for arg in args])

    @staticmethod
    def Upper(expr: Any) -> FunctionExpression:
        return FunctionExpression('UPPER', to_expression(expr))

    @staticmethod
    def Lower(expr: Any) -> FunctionExpression:
        return FunctionExpression('LOWER', to_expression(expr))
    
    @staticmethod
    def Length(expr: Any) -> FunctionExpression:
        return FunctionExpression('LENGTH', to_expression(expr))

    @staticmethod
    def Substring(expr: Any, start: int, length: Optional[int] = None) -> FunctionExpression:
        args = [to_expression(expr), LiteralExpression(start)]
        if length is not None:
            args.append(LiteralExpression(length))
        return FunctionExpression('SUBSTRING', *args)
    
    @staticmethod
    def Now() -> FunctionExpression:
        return FunctionExpression('NOW')
    
    @staticmethod
    def CurrentDate() -> FunctionExpression:
        return FunctionExpression('CURRENT_DATE')
    
    @staticmethod
    def CurrentTimestamp() -> FunctionExpression:
        return FunctionExpression('CURRENT_TIMESTAMP')
    
    @staticmethod
    def Extract(field: str, expr: Any) -> FunctionExpression:
        return FunctionExpression('EXTRACT', Expression(f'{field} FROM'), to_expression(expr))
    
    @staticmethod
    def DateTrunc(precision: str, expr: Any) -> FunctionExpression:
        return FunctionExpression('DATE_TRUNC', LiteralExpression(precision), to_expression(expr))
    
    @staticmethod
    def Coalesce(*args: Any) -> FunctionExpression:
        return FunctionExpression('COALESCE', *[to_expression(arg) for arg in args])

    @staticmethod
    def Abs(expr: Any) -> FunctionExpression:
        return FunctionExpression('ABS', to_expression(expr))

    @staticmethod
    def Round(expr: Any, precision: Optional[int] = None) -> FunctionExpression:
        args = [to_expression(expr)]
        if precision is not None:
            args.append(LiteralExpression(precision))
        return FunctionExpression('ROUND', *args)
    
    @staticmethod
    def Case() -> CaseExpression:
        """Create a new CASE expression."""
        return CaseExpression()
    
    # Window functions
    @staticmethod
    def RowNumber() -> FunctionExpression:
        return FunctionExpression('ROW_NUMBER')
    
    @staticmethod
    def Rank() -> FunctionExpression:
        return FunctionExpression('RANK')
    
    @staticmethod
    def DenseRank() -> FunctionExpression:
        return FunctionExpression('DENSE_RANK')
    
    @staticmethod
    def Lag(expr: Any, offset: int = 1, default: Any = None) -> FunctionExpression:
        args = [to_expression(expr), LiteralExpression(offset)]
        if default is not None:
            args.append(to_expression(default))
        return FunctionExpression('LAG', *args)
    
    @staticmethod
    def Lead(expr: Any, offset: int = 1, default: Any = None) -> FunctionExpression:
        args = [to_expression(expr), LiteralExpression(offset)]
        if default is not None:
            args.append(to_expression(default))
        return FunctionExpression('LEAD', *args)

def to_expression(value: Any) -> BaseExpression:
    if isinstance(value, BaseExpression):
        return value
    else:
        return LiteralExpression(value)

# Convenience functions
def literal(value: Any) -> LiteralExpression:
    """Create a literal expression."""
    return LiteralExpression(value)

def null() -> NullExpression:
    """Create a NULL expression."""
    return NullExpression()

def case() -> CaseExpression:
    """Create a CASE expression."""
    return CaseExpression()

def func(name: str, *args: Any) -> FunctionExpression:
    """Create a custom function expression."""
    return FunctionExpression(name, *[to_expression(arg) for arg in args])