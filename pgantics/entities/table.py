from typing import (
    Any,
    Dict,
    Optional,
    Tuple,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass

from ..query.select import Select
from ..registry import TABLE_REGISTRY
from .column import ColumnInfo
from .expression import BaseExpression
from .mapped import Mapped

__all__ = ["Table"]

class TableMeta(ModelMetaclass):
    def __new__(mcls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any], **kwargs: Any) -> 'TableMeta':
        original_annotations = attrs.get('__annotations__', {}).copy()

        for key, annotation in attrs.get('__annotations__', {}).items():
            field = attrs.get(key, None)

            if not field:
                continue

            origin = get_origin(annotation)
            args   = get_args(annotation)
            
            if origin is Mapped or annotation is Mapped:
                attrs['__annotations__'][key] = args[0]

        cls = super().__new__(mcls, name, bases, attrs, **kwargs)

        fields: Dict[str, ColumnInfo] = {}
        annotations = get_type_hints(cls)

        for key, field in attrs.items():
            if not isinstance(field, ColumnInfo):
                continue

            field.annotation = field.annotation or annotations.get(key, None)

            if field.annotation is None:
                raise TypeError(f"Field '{key}' in class '{cls.__name__}' has no type annotation.")

            origin = get_origin(original_annotations[key])

            if origin is not Mapped and original_annotations[key] is not Mapped:
                raise TypeError(
                    f"Column '{key}' in class '{cls.__name__}' must be a Mapped annotation. Example: `id: Mapped[int] = Column()`"
                )

            field._source_field = key
            field._source_table = cls

            fields[key] = field

        setattr(cls, '__pgantics_fields__', fields)
        return cls
    
    def __getattr__(cls, name: str) -> Any:
        if name in cls.__dict__.get('__pgantics_fields__', {}):
            return cls.__pgantics_fields__[name]
        return super().__getattribute__(name)

class Table(BaseModel, metaclass=TableMeta):
    """Represents a database table. Built on Pydantic's BaseModel with type-safe columns.

    This class automatically handles:
    - Everything pydantic does
    - Foreign key relationships
    - Indexes
    - Query building

    Example:
    ```
    class User(Table):
        # Nested meta class
        class Meta:
            table_name = "users"

        # Postgres columns
        id: Mapped[int] = Column(types.BigSerial(), primary_key=True)
        email: Mapped[str] = Column(types.VarChar(100))
        created_at: Mapped[datetime.datetime] = Column(types.TimestampTZ())

        # Pydantic fields
        in_cache: bool = Field(default=False)
    ```
    """

    __pgantics_fields__: Dict[str, ColumnInfo[Any]]

    def __init_subclass__(cls, **kwargs: Any):
        super().__init_subclass__(**kwargs)

        if not hasattr(cls, 'Meta'):
            raise TypeError(f"Table '{cls.__name__}' is missing a nested Meta class. See the docstring for an example.")
        
        base = cls.__base__

        if not base or not issubclass(base, Table):
            raise TypeError(f"Table '{cls.__name__}' must inherit from the Table class.")

        args = get_type_hints(base.Meta)

        for arg, arg_type in args.items():
            origin = get_origin(arg_type)

            if origin is not Optional and not hasattr(cls.Meta, arg):
                raise TypeError(f"Table '{cls.__name__}' is missing required Meta attribute '{arg}'.")

        TABLE_REGISTRY.register(cls)

    class Meta:
        table_name: str

    @classmethod
    def select(cls, *columns: Union[str, BaseExpression]) -> Select:
        query = Select(cls)
        query.select(*columns)
        return query