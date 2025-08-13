from typing import Any, Optional

__all__ = ["FuncExpression", "Func"]

class FuncExpression:
    """Represents a SQL function expression. This class is used to construct SQL function expressions in a type-safe manner. This is not validated.

    Example:
        User.select(FuncExpression("COUNT", "*"))
        User.select(FuncExpression("MAX", "age"))
    """
    def __init__(self, func: str, *args: Any, alias: Optional[str]=None):
        self.func = func
        self.args = args
        self.alias = alias

    def __str__(self):
        return f"{self.func}({', '.join(map(str, self.args))})"

def Func(func: str, *args: Any, alias: Optional[str]=None) -> FuncExpression:
    """Alias for FuncExpression constructor."""
    return FuncExpression(func, *args, alias=alias)