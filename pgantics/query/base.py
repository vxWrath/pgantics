from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, List, Tuple, Type, Union

if TYPE_CHECKING:
    from ..entities.table import Table

class Query(ABC):
    def __init__(self, table: Union[Type['Table'], 'Table']):
        self.table = table

    @abstractmethod
    def build(self) -> Tuple[str, List[Any]]:
        """Build the SQL query string."""
        pass

    def __str__(self) -> str:
        """Return the SQL query string."""
        sql, _ = self.build()
        return sql