from typing import Any, List, Optional, Tuple

from ..core.exceptions import ValidationError
from ..core.registry import _TABLE_REGISTRY, get_table_class, register_table
from ..fields.column import ColumnInfo
from ..types.complex import Array
from ..types.foreign_key import ForeignKey
from .base import PGAnticsModel

__all__ = (
    "Table",
    "validate"
)

class Table(PGAnticsModel):
    __entity_name__ = "Table"
    __supported_field_types__ = (ColumnInfo,)

    class Meta:
        table_name: str
        indexes: Optional[List[Tuple[str, ...]]] # for composite indexes
        constraints: Optional[List[Tuple[str, ...]]] # for composite constraints

    def __init_subclass__(cls, **kwargs: Any):
        register_table(cls)
        return super().__init_subclass__(**kwargs)
    
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