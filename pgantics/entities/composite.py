from typing import Any

from ..core.registry import register_custom_type
from ..fields.column import CompositeColumnInfo
from ..types.base import PostgresType
from .base import PGAnticsModel

__all__ = (
    "CompositeType",
)

class CompositeType(PGAnticsModel, PostgresType):
    __entity_name__ = "CompositeType"
    __supported_field_types__ = (CompositeColumnInfo,)

    class Meta:
        type_name: str

    def __init_subclass__(cls, **kwargs: Any):
        register_custom_type(cls)
        return super().__init_subclass__(**kwargs)