from typing import TYPE_CHECKING, Dict, Literal, Type, overload

from .exceptions import AlreadyRegistered, UnknownReferenceError

if TYPE_CHECKING:
    from ..entities.composite import CompositeType
    from ..entities.enum import EnumType
    from ..entities.index import Index
    from ..entities.table import Table
    from ..types.base import PostgresType
    from ..types.domain import Domain

_TABLE_REGISTRY: Dict[str, Type['Table']] = {}
_TYPE_REGISTRY: Dict[str, Type['PostgresType']] = {}
_CUSTOM_TYPE_REGISTRY: Dict[str, Type['CompositeType']] = {}
_DOMAIN_REGISTRY: Dict[str, Type['Domain']] = {}
_INDEX_REGISTRY: Dict[str, 'Index'] = {}

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

def register_custom_type(custom_type_class: Type['CompositeType']) -> None:
    """Register a custom Postgres type class in the global registry."""

    if custom_type_class.__name__ in _CUSTOM_TYPE_REGISTRY:
        raise AlreadyRegistered(f"Custom type class '{custom_type_class.__name__}' is already registered.")
    
    _CUSTOM_TYPE_REGISTRY[custom_type_class.__name__] = custom_type_class

def register_domain(domain_class: Type['Domain']) -> None:
    """Register a domain class in the global registry."""

    if domain_class.__name__ in _DOMAIN_REGISTRY:
        raise AlreadyRegistered(f"Domain class '{domain_class.__name__}' is already registered.")
    
    _DOMAIN_REGISTRY[domain_class.__name__] = domain_class

def register_index(index: 'Index') -> None:
    """Register an index in the global registry."""

    if index.name in _INDEX_REGISTRY:
        raise AlreadyRegistered(f"Index '{index.name}' is already registered.")
    
    _INDEX_REGISTRY[index.name] = index

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

def get_custom_type_class(name: str) -> Type['CompositeType']:
    """Retrieve a custom Postgres type class by its name from the global registry."""

    if name not in _CUSTOM_TYPE_REGISTRY:
        raise UnknownReferenceError(f"Custom type class '{name}' is not registered.")

    return _CUSTOM_TYPE_REGISTRY[name]

def get_domain_class(name: str) -> Type['Domain']:
    """Retrieve a domain class by its name from the global registry."""

    if name not in _DOMAIN_REGISTRY:
        raise UnknownReferenceError(f"Domain class '{name}' is not registered.")

    return _DOMAIN_REGISTRY[name]

def get_index(name: str) -> 'Index':
    """Retrieve an index by its name from the global registry."""

    if name not in _INDEX_REGISTRY:
        raise UnknownReferenceError(f"Index '{name}' is not registered.")

    return _INDEX_REGISTRY[name]