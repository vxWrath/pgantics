from typing import Any, Dict, List, Optional, Tuple, Union

from ..core.exceptions import ValidationError
from ..core.registry import _TABLE_REGISTRY, get_table_class, register_table
from ..fields.column import ColumnInfo
from ..types.complex import Array
from ..types.foreign_key import ForeignKey
from .base import PGAnticsModel
from .check import Check
from .index import Index

__all__ = (
    "Table",
    "validate"
)

class Table(PGAnticsModel):
    __entity_name__ = "Table"
    __supported_field_types__ = (ColumnInfo,)

    class Meta:
        table_name: str
        schema: Optional[str] = None

        # Constraints
        checks: Optional[List[Check]] = None

        # Indexes
        indexes: Optional[List[Index]] = None

    def __init_subclass__(cls, **kwargs: Any):
        register_table(cls)
        return super().__init_subclass__(**kwargs)
    
    @classmethod
    def full_table_name(cls) -> str:
        """Get the full table name including schema if specified."""

        if hasattr(cls.Meta, 'schema') and cls.Meta.schema:
            return f"{cls.Meta.schema}.{cls.Meta.table_name}"
        return cls.Meta.table_name
    
    @classmethod
    def primary_key(cls) -> Union[str, Tuple[str, ...]]:
        """Return the primary key column if it exists."""

        keys = tuple(x for x, y in cls.__pgdantic_fields__.items() if isinstance(y, ColumnInfo) and y.primary_key)

        if len(keys) == 0:
            raise ValidationError(f"Table '{cls.__name__}' does not have a primary key defined.")
        elif len(keys) == 1:
            return keys[0]
        else:
            return keys
        
    @classmethod
    def get_indexes(cls) -> List[Index]:
        """Return the indexes defined for this table."""

        indexes: Dict[str, Index] = {}

        if hasattr(cls.Meta, 'indexes') and cls.Meta.indexes:
            indexes.update({index.name: index for index in cls.Meta.indexes})

        for field_info in cls.__pgdantic_fields__.values():
            if isinstance(field_info, ColumnInfo) and field_info.index:
                indexes[field_info.index.name] = field_info.index

        return list(indexes.values())

register_table(Table)

def validate() -> None:
    """Validate all registered tables, composite types, foreign keys, etc. This should be called after all models are defined."""

    Table = get_table_class('Table')

    for table in _TABLE_REGISTRY.values():
        if table is Table:
            continue

        for field_name, field_info in table.__pgdantic_fields__.items():
            if isinstance(field_info, ColumnInfo):
                try:
                    postgres_type = field_info.postgres_type # calls the cached_property in order to validate the element type
                except Exception as e:
                    raise ValidationError(
                        f"Error validating field '{field_name}' in table '{table.__name__}': {str(e)}"
                    ) from e
                
                if isinstance(postgres_type, ForeignKey):
                    field_info._check_foreign_key()
                elif isinstance(postgres_type, Array):
                    postgres_type.element_type  # calls the cached_property in order to validate the element type

                field_info._set_index_name()