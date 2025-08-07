from typing import Any, Iterable, Optional, Sequence, Type, Union, Unpack

from pydantic.fields import _FromFieldInfoInputs

from ..core.enums import Operator
from ..core.registry import register_index
from ..core.utils import MISSING
from ..entities.check import Check
from ..entities.default import DefaultValue, Expression
from ..entities.index import Index
from ..entities.unique import Unique
from ..query.conditions import Condition
from ..types.base import PostgresType
from .base import FieldMetadata, PGFieldInfo

__all__ = (
    "Column",
    "CompositeColumn",
)

class ColumnMetadata(FieldMetadata, total=False):
    primary_key: bool
    unique: Union[bool, Unique]
    index: Optional[Index]
    nullable: bool
    default: DefaultValue
    checks: Optional[Sequence[Check]]

class ColumnInfo(PGFieldInfo):
    """A field that represents a column in PostgreSQL."""

    def __init__(self, *,
        postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
        primary_key: bool = False,
        unique: Union[bool, Unique] = False,
        index: Optional[Index] = None,
        nullable: bool = MISSING,
        default: DefaultValue | Sequence[DefaultValue] = MISSING,
        checks: Optional[Sequence[Check]] = None,
        pydantic_default: Any = MISSING,
        **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
    ):
        super().__init__(postgres_type=postgres_type, pydantic_default=pydantic_default, **pydantic_kwargs)
        self.__sql_metadata__: ColumnMetadata = {}

        self.primary_key = primary_key
        self.index = index
        self.unique = unique
        self.nullable = nullable
        self.checks = checks

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

        if self.checks:
            self.__sql_metadata__["checks"] = self.checks

    def _set_index_name(self) -> None:
        if self.index and self.index.name is MISSING:
            self.index.name = f"idx_{self._source_class.Meta.table_name}_{self._source_field_name}"
            register_index(self.index)

    def __eq__(self, other) -> 'Condition':
        return Condition(self, Operator.EQ, other)

    def __ne__(self, other) -> 'Condition':
        return Condition(self, Operator.NEQ, other)

    def __lt__(self, other) -> 'Condition':
        return Condition(self, Operator.LT, other)

    def __gt__(self, other) -> 'Condition':
        return Condition(self, Operator.GT, other)

    def like(self, pattern: str) -> 'Condition':
        return Condition(self, Operator.LIKE, pattern)

    def ilike(self, pattern: str) -> 'Condition':
        return Condition(self, Operator.ILIKE, pattern)

    def in_(self, values: Iterable[Any]) -> 'Condition':
        return Condition(self, Operator.IN, values)

    def not_in_(self, values: Iterable[Any]) -> 'Condition':
        return Condition(self, Operator.NOT_IN, values)

    def is_null(self) -> 'Condition':
        return Condition(self, Operator.IS_NULL, None)
    
    def is_not_null(self) -> 'Condition':
        return Condition(self, Operator.IS_NOT_NULL, None)
    
    def __str__(self) -> str:
        return f"{self._source_class.Meta.table_name}.{self._source_field_name}"

def Column(
    postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING,
    /, *,
    primary_key: bool = False,
    unique: Union[bool, Unique] = False,
    index: Optional[Index] = None,
    nullable: bool = MISSING,
    default: DefaultValue | Sequence[DefaultValue] = MISSING,
    checks: Optional[Sequence[Check]] = None,
    pydantic_default: Any = MISSING,
    **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
) -> Any:
    """Create a column field with PostgreSQL metadata.
    
    Args:
        postgres_type: The PostgreSQL type for the column.
        primary_key: Whether this column is a primary key.
        unique: Whether this column should have a unique constraint. If you want to use a composite unique constraint, pass a Unique instance.
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
        checks=checks,
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