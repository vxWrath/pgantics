from abc import ABC
from typing import TYPE_CHECKING, Type

from ..registry import TYPE_REGISTRY

if TYPE_CHECKING:
    from ..entities.table import Table

__all__ = ["PostgresType"]

class PostgresType(ABC):
    """Base class for all PostgreSQL types."""

    _source_table: Type['Table']
    _source_field: str

    def __init__(self):
        pass

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        TYPE_REGISTRY.register(cls)

    def __repr__(self):
        return f"<{self.__class__.__name__} value={super().__repr__()}>"