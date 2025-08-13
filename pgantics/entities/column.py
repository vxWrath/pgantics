from typing import TYPE_CHECKING, Any, Dict, Type, Union, Unpack

from pydantic.fields import FieldInfo, _FromFieldInfoInputs

from ..types.base import PostgresType
from ..utils import MISSING
from .expression import Expression

if TYPE_CHECKING:
    from .table import Table

__all__ = ["Column"]

class pganticsFieldInfo(FieldInfo):
    def __init__(self, *,
        type: Union[Type[PostgresType], PostgresType]=MISSING,
        default: Any = MISSING,
        **kwargs: Unpack[_FromFieldInfoInputs]
    ):
        self._type = type

        if default is MISSING:
            super().__init__(**kwargs)
        else:
            super().__init__(default=default, **kwargs)

class ColumnInfo[T](pganticsFieldInfo, Expression):
    def __init__(self, *,
        type: Union[Type[PostgresType], PostgresType]=MISSING,
        primary_key: bool = False,
        default: Any = MISSING,
        **kwargs: Unpack[_FromFieldInfoInputs]
    ):
        super().__init__(default=default, **kwargs)

        self.sql_data: Dict[str, Any] = {}

        self._source_table: Type[Table] = MISSING
        self._source_field: str = MISSING

        if primary_key:
            self.sql_data['primary_key'] = True
    
    def __str__(self) -> str:
        return f"{self._source_table.Meta.table_name}.{self._source_field}"

def Column(
    type: Union[Type[PostgresType], PostgresType]=MISSING,
    /, *,
    primary_key: bool = False,
    default: Any = MISSING,
    **pydantic_kwargs: Unpack[_FromFieldInfoInputs]
) -> Any:
    """Create a new column.
    
    Args:
        type: The postgres type for the column. If not provided, it will be inferred from the annotation.
        primary_key: Whether the column is a primary key. Defaults to False.
        default: The default value for pydantic fields.
        **pydantic_kwargs: Additional keyword arguments for the column.

    Example:
    ```
        # Basic
        name: Mapped[str] = Column(Text, unique=True)

        # Primary key
        id: Mapped[int] = Column(Bigint(), primary_key=True)
    ```
    """

    return ColumnInfo(
        type=type,
        primary_key=primary_key,
        default=default,
        **pydantic_kwargs
    )