from functools import cached_property
from typing import TYPE_CHECKING, Any, Type, TypedDict, Union, Unpack

from pydantic.fields import FieldInfo, _FromFieldInfoInputs

from ..core.exceptions import ForeignKeyError, MissingAnnotation
from ..core.registry import get_type_class
from ..core.utils import MISSING
from ..types.base import PostgresType, to_postgres_type
from ..types.foreign_key import ForeignKey

if TYPE_CHECKING:
    from ..entities.table import Table

__all__ = (
    "PGFieldInfo",
    "FieldMetadata",
)

class FieldMetadata(TypedDict, total=False):
    type: Union[Type[Any], Any]

class PGFieldInfo(FieldInfo):
    def __init__(self, *, postgres_type: Union[Type[PostgresType], PostgresType, str] = MISSING, **kwargs: Unpack[_FromFieldInfoInputs]):
        super().__init__(**kwargs)
        self._postgres_type = postgres_type
        self.__sql_metadata__: FieldMetadata = {}

        self._source_class: Type['Table'] = MISSING
        self._source_field_name: str = MISSING
    
    @cached_property
    def postgres_type(self) -> Union[Type[PostgresType], PostgresType]:
        CompositeType = get_type_class('CompositeType')

        if not self.has_type():
            if self.annotation is None:
                raise MissingAnnotation(f"Annotation is missing for field '{self._source_field_name}' in class '{self._source_class}'.")

            postgres_type = to_postgres_type(self.annotation)
        else:
            if isinstance(self._postgres_type, str):
                postgres_type = get_type_class(self._postgres_type)
            elif isinstance(self._postgres_type, type) and not issubclass(self._postgres_type, CompositeType):
                postgres_type = self._postgres_type()
            else:
                postgres_type = self._postgres_type

        self.__sql_metadata__["type"] = postgres_type
        return postgres_type
    
    def _check_foreign_key(self) -> None:
        foreign_key = self._postgres_type

        if not isinstance(foreign_key, ForeignKey):
            return

        foreign_table = foreign_key.table
        field = foreign_table.pgdantic_fields()[foreign_key.column]

        if field.annotation != self.annotation:
            raise ForeignKeyError(
                f"Foreign key type mismatch: expected '{self.annotation}', "
                f"but referenced field has type '{field.annotation}' field '{self._source_field_name}' in class '{self._source_class.__name__}'."
            )
    
    def has_type(self) -> bool:
        return self._postgres_type is not MISSING