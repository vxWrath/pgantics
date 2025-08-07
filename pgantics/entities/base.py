from functools import cache
from typing import Any, Dict, Tuple, Type, Union, get_args, get_origin, get_type_hints

from pydantic import BaseModel
from pydantic._internal._model_construction import ModelMetaclass

from ..core.exceptions import MetaClassError, MissingAnnotation, UnsupportedType
from ..fields.column import ColumnInfo, CompositeColumnInfo

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
            return f"<class '{self.__entity_name__}({self.__name__})'>"
        return super().__repr__()
    
    def __getattr__(self, name: str) -> Any:
        try:
            return super().__getattribute__(name)
        except AttributeError:
            if name in self.__pgdantic_fields__:
                return self.__pgdantic_fields__[name]
            raise

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
    def get_sql_metadata(cls) -> Dict[str, Any]:
        """Retrieve SQL metadata for the model."""
        raise NotImplementedError("Subclasses must implement 'get_sql_metadata' method to return SQL metadata.")

    @classmethod
    def pgdantic_fields(cls) -> Dict[str, Union[ColumnInfo, CompositeColumnInfo]]:
        """Retrieve all fields defined in the model. Call this over `.model_fields` to ensure proper attribute access."""

        return cls.__pgdantic_fields__