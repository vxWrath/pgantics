from typing import Any, Optional, Sequence, Type, Union, Unpack

from pydantic.fields import _FromFieldInfoInputs

from ..core.utils import MISSING
from ..entities.default import Expression, _DefaultValue
from ..entities.index import Index
from ..types.base import PostgresType
from .base import FieldMetadata, PGFieldInfo

__all__ = (
    "Column",
    "CompositeColumn",
)

class ColumnMetadata(FieldMetadata, total=False):
    primary_key: bool
    unique: bool
    index: Optional[Index]
    nullable: bool
    default: _DefaultValue

class ColumnInfo(PGFieldInfo):
    """A field that represents a column in PostgreSQL."""

    def __init__(self, *,
        postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
        primary_key: bool = False,
        unique: bool = False,
        index: Optional[Index] = None,
        nullable: bool = MISSING,
        default: _DefaultValue | Sequence[_DefaultValue] = MISSING,
        pydantic_default: Any = MISSING,
        **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
    ):
        super().__init__(postgres_type=postgres_type, pydantic_default=pydantic_default, **pydantic_kwargs)
        self.__sql_metadata__: ColumnMetadata = {}

        self.primary_key = primary_key
        self.index = index
        self.unique = unique
        self.nullable = nullable

        if isinstance(default, Sequence):
            self.postgres_default = Expression('{' + ','.join(map(str, default)) + '}')
        else:
            self.postgres_default = default

        if self.primary_key:
            self.__sql_metadata__["primary_key"] = self.primary_key

        if self.unique:
            if self.primary_key:
                raise ValueError("A primary key column cannot also be unique, as primary keys are inherently unique.")
            self.__sql_metadata__["unique"] = self.unique

        if self.index:
            if self.primary_key:
                raise ValueError("A primary key column cannot have an index, as primary keys are indexed by default.")
            elif self.unique:
                raise ValueError("A unique column cannot have an index, as unique constraints are indexed by default.")
            
            self.__sql_metadata__["index"] = self.index

        if self.nullable is not MISSING:
            self.__sql_metadata__["nullable"] = self.nullable

        if self.postgres_default is not MISSING:
            self.__sql_metadata__["default"] = self.postgres_default

    def _set_index_name(self) -> None:
        """Set the index name if it is not already set."""
        if self.index and self.index.name is MISSING:
            self.index.name = f"idx_{self._source_class.Meta.table_name}_{self._source_field_name}"

def Column(
    postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
    /, *,
    primary_key: bool = False,
    unique: bool = False,
    index: Optional[Index] = None,
    nullable: bool = MISSING,
    default: _DefaultValue | Sequence[_DefaultValue] = MISSING,
    pydantic_default: Any = MISSING,
    **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
) -> Any:
    """Create a column field with PostgreSQL metadata.
    
    Args:
        postgres_type: The PostgreSQL type for the column.
        primary_key: Whether this column is a primary key.
        unique: Whether this column should have a unique constraint.
        index: Optional index for this column.
        nullable: Whether this column is nullable.
        default: The default value for this column.
        pydantic_default: The Pydantic default value for this column.
        **pydantic_kwargs: Additional Pydantic field arguments.
    Returns:
        A ColumnInfo instance representing the column.
    """

    return ColumnInfo(
        postgres_type=postgres_type,
        primary_key=primary_key,
        unique=unique,
        index=index,
        nullable=nullable,
        default=default,
        pydantic_default=pydantic_default,
        **pydantic_kwargs
    )

class CompositeColumnMetadata(FieldMetadata, total=False):
    pass

class CompositeColumnInfo(PGFieldInfo):
    """A field that represents a composite type in PostgreSQL."""

    def __init__(self, *,
        postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
        pydantic_default: Any = MISSING,
        **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
    ):
        super().__init__(postgres_type=postgres_type, pydantic_default=pydantic_default, **pydantic_kwargs)
        self.__sql_metadata__: CompositeColumnMetadata

def CompositeColumn(
    postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
    /,
    pydantic_default: Any = MISSING,
    **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
) -> Any:
    """Create a composite field with PostgreSQL metadata.
    
    Args:
        postgres_type: The PostgreSQL type for the composite column.
        pydantic_default: The Pydantic default value for the composite column.
        **pydantic_kwargs: Additional Pydantic field arguments.
    Returns:
        A CompositeColumnInfo instance representing the composite column.
    """

    return CompositeColumnInfo(
        postgres_type=postgres_type,
        pydantic_default=pydantic_default,
        **pydantic_kwargs
    )