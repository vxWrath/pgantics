from typing import Any, Type, Union, Unpack

from pydantic.fields import _FromFieldInfoInputs

from ..core.utils import MISSING
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

class ColumnInfo(PGFieldInfo):
    """A field that represents a column in PostgreSQL."""

    def __init__(self, *,
        postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
        primary_key: bool = False,
        unique: bool = False,
        index: bool = False,
        nullable: bool = MISSING,
        **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
    ):
        super().__init__(postgres_type=postgres_type, **pydantic_kwargs)
        self.__sql_metadata__: ColumnMetadata = {}

        self.primary_key: bool = primary_key
        self.index: bool = index
        self.unique: bool = unique
        self.nullable: bool = nullable

        if self.primary_key:
            self.__sql_metadata__["primary_key"] = self.primary_key

        elif self.unique:
            self.__sql_metadata__["unique"] = self.unique

        elif self.index:
            self.__sql_metadata__["index"] = self.index

        if self.nullable is not MISSING:
            self.__sql_metadata__["nullable"] = self.nullable

def Column(
    postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
    /, *,
    primary_key: bool = False,
    unique: bool = False,
    index: bool = False,
    nullable: bool = MISSING,
    **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
) -> Any:
    """Create a column field with PostgreSQL metadata."""

    return ColumnInfo(
        postgres_type=postgres_type,
        primary_key=primary_key,
        unique=unique,
        index=index,
        nullable=nullable,
        **pydantic_kwargs
    )

class CompositeColumnMetadata(FieldMetadata, total=False):
    pass

class CompositeColumnInfo(PGFieldInfo):
    """A field that represents a composite type in PostgreSQL."""

    def __init__(self, *,
        postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
        **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
    ):
        super().__init__(postgres_type=postgres_type, **pydantic_kwargs)
        self.__sql_metadata__: CompositeColumnMetadata

def CompositeColumn(
    postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
    /,
    **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
) -> Any:
    """Create a composite field with PostgreSQL metadata."""

    return CompositeColumnInfo(
        postgres_type=postgres_type,
        **pydantic_kwargs
    )