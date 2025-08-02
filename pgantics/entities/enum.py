import enum
from typing import Union, get_args, get_origin, get_type_hints

from ..core.exceptions import MetaClassError
from ..types.base import PostgresType

__all__ = (
    "EnumType",
)

class EnumTypeMeta(enum.EnumMeta):
    """Meta class for EnumType to ensure it has a Meta class defined."""

    def __str__(cls) -> str:
        return cls.Meta.type_name # type: ignore

class EnumType(PostgresType, enum.StrEnum, metaclass=EnumTypeMeta):
    class Meta:
        type_name: str

    def __init_subclass__(cls, **kwargs):
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