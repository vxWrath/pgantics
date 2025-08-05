from functools import cache
from typing import Any, Dict, Tuple, Type, Union, get_args, get_origin, get_type_hints

from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass

from ..core.exceptions import MetaClassError, MissingAnnotation, UnsupportedType
from ..core.registry import get_type_class
from ..fields.column import (
    ColumnInfo,
    ColumnMetadata,
    CompositeColumnInfo,
    CompositeColumnMetadata,
)
from ..types.foreign_key import ForeignKey

__all__ = (
    "PGAnticsModel",
)

class PGAnticsModelMeta(ModelMetaclass):
    def __new__(cls, name: str, bases: Tuple[type, ...], attrs: Dict[str, Any]) -> "PGAnticsModelMeta":
        t = super().__new__(cls, name, bases, attrs)
        annotations = get_type_hints(t)

        if bases:
            fields = {}

            for field_name, field in attrs.items():
                if isinstance(field, (ColumnInfo, CompositeColumnInfo)):
                    supported = getattr(t, '__supported_field_types__', tuple())
                    if not isinstance(field, supported):
                        raise UnsupportedType(
                            f"Unsupported field type '{type(field).__name__}' for field '{field_name}' in class '{name}'. Supported types: {supported}"
                        )
                    
                    if field.annotation is None:
                        field.annotation = annotations.get(field_name, None)

                    if field.annotation is None:
                        raise MissingAnnotation(f"Field '{field_name}' in class '{name}' must have an annotation. Example: '{field_name}: str = Column(...)'")

                    field._source_class = t
                    field._source_field_name = field_name

                    fields[field_name] = field
                    
            setattr(t, '__pgdantic_fields__', fields)

        return t
    
    def __repr__(self) -> str:
        if hasattr(self, '__entity_name__'):
            return f"<class '{self.__entity_name__}({self.__name__})'>" # type: ignore
        return super().__repr__()

class PGAnticsModel(BaseModel, metaclass=PGAnticsModelMeta):
    __pgdantic_fields__: Dict[str, Union[ColumnInfo, CompositeColumnInfo]] = {}
    __supported_field_types__: Tuple[Type[Any], ...] = ()
    __entity_name__: str

    class Meta:
        ...

    def __init_subclass__(cls, **kwargs: Any):
        init_subcls = super().__init_subclass__(**kwargs)

        if not hasattr(cls, 'Meta'):
            raise MetaClassError(f"Class '{cls.__name__}' must define a nested Meta class. Example: 'class Meta: table_name = \"my_table\"'")
        
        base = cls.__base__

        if not base:
            raise MetaClassError(f"Class '{cls.__name__}' must inherit from a PGDantic model.")

        args = get_type_hints(base.Meta)

        for arg_name, arg_type in args.items():
            origin = get_origin(arg_type)

            if origin is Union and type(None) in get_args(arg_type):
                continue

            if not hasattr(cls.Meta, arg_name):
                raise MetaClassError(f"Class '{cls.__name__}' must define '{arg_name}' in its Meta class. See the base class '{base.__name__}' for required fields.")
            
        return init_subcls

    @classmethod
    @cache
    def get_sql_metadata(cls) -> Dict[str, Union[ColumnMetadata, CompositeColumnMetadata]]:
        """Retrieve SQL metadata for the model."""

        CompositeType = get_type_class('CompositeType')

        metadata = {x: getattr(cls.Meta, x) for x in dir(cls.Meta) if not x.startswith('_') and not callable(getattr(cls.Meta, x))}
        columns: Dict[str, ColumnMetadata | CompositeColumnMetadata] = {}

        for field_name, field_info in cls.__pgdantic_fields__.items():
            if isinstance(field_info, ColumnInfo):
                meta: Any = field_info.__sql_metadata__
                meta['type'] = field_info.postgres_type

                print(meta)

                if issubclass(meta['type'], CompositeType):
                    meta['type'] = meta['type'].get_sql_metadata()

                columns[field_name] = meta

        metadata['columns'] = columns
        return metadata

    @classmethod
    def pgdantic_fields(cls) -> Dict[str, Union[ColumnInfo, CompositeColumnInfo]]:
        """Retrieve all fields defined in the model. Call this over `.model_fields` to ensure proper attribute access."""

        return cls.__pgdantic_fields__
    
    @classmethod
    def get_foreign_keys(cls) -> Dict[str, ColumnInfo]:
        """Retrieve foreign keys defined in the model."""

        return {
            name: field for name, field in cls.__pgdantic_fields__.items()
            if isinstance(field, ColumnInfo) and isinstance(field.postgres_type, ForeignKey)
        }