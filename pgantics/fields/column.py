from typing import Any, Sequence, Type, Union, Unpack

from pydantic.fields import _FromFieldInfoInputs

from ..core.utils import MISSING
from ..entities.default import Expression, _DefaultValue
from ..types.base import PostgresType
from .base import FieldMetadata, PGFieldInfo

__all__ = (
    "Column",
    "CompositeColumn",
)

class ColumnMetadata(FieldMetadata, total=False):
    primary_key: bool
    unique: bool
    index: bool
    nullable: bool
    default: _DefaultValue

class ColumnInfo(PGFieldInfo):
    """A field that represents a column in PostgreSQL."""

    def __init__(self, *,
        postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
        primary_key: bool = False,
        unique: bool = False,
        index: bool = False,
        nullable: bool = MISSING,
        default: _DefaultValue | Sequence[_DefaultValue] = MISSING,
        pydantic_default: Any = MISSING,
        **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
    ):
        super().__init__(postgres_type=postgres_type, pydantic_default=pydantic_default, **pydantic_kwargs)
        self.__sql_metadata__: ColumnMetadata = {}

        self.primary_key: bool = primary_key
        self.index: bool = index
        self.unique: bool = unique
        self.nullable: bool = nullable

        if isinstance(default, Sequence):
            self.postgres_default = Expression('{' + ','.join(map(str, default)) + '}')
        else:
            self.postgres_default = default

        if self.primary_key:
            self.__sql_metadata__["primary_key"] = self.primary_key

        elif self.unique:
            self.__sql_metadata__["unique"] = self.unique

        elif self.index:
            self.__sql_metadata__["index"] = self.index

        if self.nullable is not MISSING:
            self.__sql_metadata__["nullable"] = self.nullable

        if self.postgres_default is not MISSING:
            self.__sql_metadata__["default"] = self.postgres_default

def Column(
    postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
    /, *,
    primary_key: bool = False,
    unique: bool = False,
    index: bool = False,
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
        index: Whether this column should be indexed.
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