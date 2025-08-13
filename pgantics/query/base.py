from abc import ABC, abstractmethod
from typing import Any, List, Tuple


class Query(ABC):
    @abstractmethod
    def build(self) -> Tuple[str, List[Any]]:
        """Build the SQL query string."""
        pass

    def __str__(self) -> str:
        """Return the SQL query string."""
        sql, _ = self.build()
        return sql