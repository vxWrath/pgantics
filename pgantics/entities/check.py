import re
from typing import Any, AnyStr, Iterable, List, Optional, Union

__all__ = (
    "Check",
    "check_positive",
    "check_non_negative",
    "check_range",
    "check_length",
    "check_in",
    "check_not_empty",
    "check_email_format",
    "check_regex",
)

class Check:
    """Postgres CHECK constraint."""

    def __init__(self, condition: str, /, *, name: Optional[str] = None):
        """
        Create a CHECK constraint.
        
        Args:
            condition: The condition to check (SQL expression)
            name: Optional constraint name
        """

        self.condition = condition
        self.name = name

    def __str__(self) -> str:
        if self.name:
            return f"CONSTRAINT {self.name} CHECK ({self.condition})"
        return f"CHECK ({self.condition})"
    
    def __repr__(self) -> str:
        if self.name:
            return f"Check(condition={self.condition!r}, name={self.name!r})"
        return f"Check(condition={self.condition!r})"
    
def check_positive(column: str, *, name: Optional[str] = None) -> Check:
    """Check that a column is positive."""
    return Check(f"{column} > 0", name=name)

def check_non_negative(column: str, *, name: Optional[str] = None) -> Check:
    """Check that a column is non-negative."""
    return Check(f"{column} >= 0", name=name)

def check_range(column: str, min_val: Any, max_val: Any, *, name: Optional[str] = None) -> Check:
    """Check that a column is within a range."""
    return Check(f"{column} >= {min_val} AND {column} <= {max_val}", name=name)

def check_length(column: str, min_len: Optional[int] = None, max_len: Optional[int] = None, *, name: Optional[str] = None) -> Check:
    """Check string length constraints."""
    conditions = []
    if min_len is not None:
        conditions.append(f"LENGTH({column}) >= {min_len}")
    if max_len is not None:
        conditions.append(f"LENGTH({column}) <= {max_len}")
    
    if not conditions:
        raise ValueError("Must specify at least one of min_len or max_len")
    
    return Check(" AND ".join(conditions), name=name)

def check_in(column: str, values: Iterable[Any], *, name: Optional[str] = None) -> Check:
    """Check that column value is in a list of values."""

    formatted_values: List[str] = []
    for v in values:
        if isinstance(v, str):
            formatted_values.append(f"'{v.replace("'", "''")}'")
        elif v is None:
            formatted_values.append("NULL")
        else:
            formatted_values.append(str(v))
    
    values_str = ", ".join(formatted_values)
    return Check(f"{column} IN ({values_str})", name=name)

def check_not_empty(column: str, *, name: Optional[str] = None) -> Check:
    """Check that a string column is not empty."""
    return Check(f"{column} != ''", name=name)

def check_email_format(column: str, *, name: Optional[str] = None) -> Check:
    """Basic email format check."""
    return Check(f"{column} ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{{2,}}$'", name=name)

def check_regex(column: str, pattern: Union[str, re.Pattern[AnyStr]], *, name: Optional[str] = None, case_sensitive: bool = True) -> Check:
    """Check that column matches a regex pattern."""
    operator = "~" if case_sensitive else "~*"

    if isinstance(pattern, re.Pattern):
        p = pattern.pattern

        if isinstance(p, bytes):
            p = p.decode('utf-8')
    else:
        p = pattern

    return Check(f"{column} {operator} '{p}'", name=name)