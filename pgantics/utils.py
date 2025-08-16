from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .query.base import Query

__all__ = [
    "MISSING",
    "format_query"
]

class __Missing:
    __slots__ = ()

    def __eq__(self, other) -> bool:
        return False

    def __bool__(self) -> bool:
        return False

    def __hash__(self) -> int:
        return 0

    def __repr__(self):
        return '...'
    
MISSING: Any = __Missing()

def format_query(query: 'Query', /) -> str:
    """
    Format SQL query with parameters for display. This is NOT meant to be used for actual query execution.
    """
    sql, params = query.build()

    for param in params:
        sql = sql.replace("%s", repr(param), 1)
    return sql