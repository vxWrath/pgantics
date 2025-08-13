from typing import Any, List

__all__ = [
    "MISSING",
    "format_build"
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

def format_build(sql: str, params: List[Any], /) -> str:
    """
    Format SQL query with parameters for display. This is NOT meant to be used for actual query execution.
    """
    for param in params:
        sql = sql.replace("%s", repr(param), 1)
    return sql