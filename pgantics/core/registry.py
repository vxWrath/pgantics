from typing import TYPE_CHECKING, Dict, Literal, Type, overload

from .exceptions import AlreadyRegistered, UnknownReferenceError

if TYPE_CHECKING:
    from ..entities.composite import CompositeType
    from ..entities.enum import EnumType
    from ..entities.table import Table
    from ..types.base import PostgresType

_TABLE_REGISTRY: Dict[str, Type['Table']] = {}
_TYPE_REGISTRY: Dict[str, Type['PostgresType']] = {}

def register_table(table_class: Type['Table']) -> None:
    """Register a table class in the global registry."""

    if table_class.__name__ in _TABLE_REGISTRY:
        raise AlreadyRegistered(f"Table class '{table_class.__name__}' is already registered.")
    
    _TABLE_REGISTRY[table_class.__name__] = table_class

def register_type(type_class: Type['PostgresType']) -> None:
    """Register a Postgres type class in the global registry."""

    if type_class.__name__ in _TYPE_REGISTRY:
        raise AlreadyRegistered(f"Type class '{type_class.__name__}' is already registered.")
    
    _TYPE_REGISTRY[type_class.__name__] = type_class

def get_table_class(name: str) -> Type['Table']:
    """Retrieve a table class by its name from the global registry."""
    cls = _TABLE_REGISTRY.get(name)

    if not cls:
        return get_table_class_by_table_name(name)
    return cls

def get_table_class_by_table_name(table_name: str) -> Type['Table']:
    """Retrieve a table class by its table name from the global registry."""

    table_class = next((cls for cls in _TABLE_REGISTRY.values() if hasattr(cls.Meta, 'table_name') and cls.Meta.table_name == table_name), None)

    if table_class:
        return table_class
    raise UnknownReferenceError(f"No table class found with table name '{table_name}'.")

@overload
def get_type_class(name: Literal['CompositeType']) -> Type['CompositeType']:
    ...

@overload
def get_type_class(name: Literal['EnumType']) -> Type['EnumType']:
    ...

@overload
def get_type_class(name: str) -> Type['PostgresType']:
    ...

def get_type_class(name: str) -> Type['PostgresType']:
    """Retrieve a Postgres type class by its name from the global registry."""

    if name not in _TYPE_REGISTRY:
        raise UnknownReferenceError(f"Type class '{name}' is not registered.")

    return _TYPE_REGISTRY[name]