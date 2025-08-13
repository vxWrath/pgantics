from typing import TYPE_CHECKING, Dict, Literal, Type, overload

from .exceptions import AlreadyRegisteredError, NotRegisteredError

if TYPE_CHECKING:
    from .entities import Table
    from .types import PostgresType

class Register[C]:
    def __init__(self, registry_type: str):
        self.registry_type = registry_type
        self._registry: Dict[str, Type[C]] = {}

    def register(self, cls: Type[C]):
        if cls.__name__ in self._registry:
            raise AlreadyRegisteredError(f"{self.registry_type} class '{cls.__name__}' is already registered.")
        
        self._registry[cls.__name__] = cls

    def get(self, cls_name: str) -> Type[C]:
        cls = self._registry.get(cls_name)
        if cls is None:
            raise NotRegisteredError(f"{self.registry_type} class '{cls_name}' is not registered.")
        return cls

class TableRegister(Register['Table']):
    def get(self, cls_name: str) -> Type['Table']:
        cls = self._registry.get(cls_name)

        if cls is None:
            return self.get_by_name(cls_name)
        return cls

    def get_by_name(self, table_name: str) -> Type['Table']:
        cls = next((cls for cls in self._registry.values() if cls.Meta.table_name == table_name), None)
        if cls is None:
            raise NotRegisteredError(f"Table class with name '{table_name}' is not registered.")
        return cls

class TypeRegister(Register['PostgresType']):
    @overload
    def get(self, cls_name: Literal['CompositeType']) -> Type['CompositeType']:
        ...

    @overload
    def get(self, cls_name: Literal['EnumType']) -> Type['EnumType']:
        ...

    def get(self, cls_name: str) -> Type['PostgresType']:
        return super().get(cls_name)

TABLE_REGISTRY: Register['Table']            = TableRegister('Table')
TYPE_REGISTRY: Register['PostgresType']      = TypeRegister('PostgresType')
CUSTOM_TYPE_REGISTRY: Register['CustomType'] = Register('CustomType')
DOMAIN_REGISTRY: Register['Domain']          = Register('Domain')
INDEX_REGISTRY: Register['Index']            = Register('Index')