from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
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
from ..entities.expression import Condition, Expression
from ..enums import JoinType
from ..registry import TABLE_REGISTRY
from ..utils import MISSING
from .base import Query

if TYPE_CHECKING:
    from ..entities.table import Table

__all__ = ["Select"]

class Select(Query):
    def __init__(self, table: Type['Table']):
        self.table = table

    def build(self) -> Tuple[str, List[Any]]:
        ...

    def select(self, *columns: Union[str, Expression]) -> Self:
        ...

    def join(self) -> Union['Join', Self]:
        ...

    def where(self) -> Self:
        ...

    def order_by(self) -> Self:
        ...

    def limit(self) -> Self:
        ...

    def offset(self) -> Self:
        ...


class Join:
    def __init__(self) -> None:
        ...

    def _build(self) -> Tuple[str, List[Any]]:
        ...

    def on(self) -> Select:
        ...