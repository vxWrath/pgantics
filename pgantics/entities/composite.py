from typing import Any, Dict

from ..core.exceptions import ValidationError
from ..core.registry import register_custom_type
from ..fields.column import ColumnInfo, CompositeColumnInfo
from ..types.base import PostgresType
from .base import PGAnticsModel

__all__ = (
    "CompositeType",
)

class CompositeType(PGAnticsModel, PostgresType):
    """A composite type in PostgreSQL, representing a user-defined type with multiple fields."""

    __entity_name__ = "CompositeType"
    __supported_field_types__ = (CompositeColumnInfo,)

    class Meta:
        type_name: str

    def __init_subclass__(cls, **kwargs: Any):
        register_custom_type(cls)
        return super().__init_subclass__(**kwargs)
    
    @classmethod
    def get_sql_metadata(cls) -> Dict[str, Any]:
        metadata = {x: getattr(cls.Meta, x) for x in dir(cls.Meta) if not x.startswith('_') and not callable(getattr(cls.Meta, x))}
        metadata.update({
            'columns': {},
        })

        for field_name, field_info in cls.__pgdantic_fields__.items():
            if isinstance(field_info, CompositeColumnInfo):
                meta: Any = field_info.__sql_metadata__
                meta['type'] = field_info.postgres_type

                metadata['columns'][field_name] = meta

            elif isinstance(field_info, ColumnInfo):
                raise ValidationError(f"Regular Column '{field_name}' is not allowed in composite types.")

        return metadata