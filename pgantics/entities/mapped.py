from typing import TYPE_CHECKING, Any, Optional, Type, Union, overload

if TYPE_CHECKING:
    from .column import ColumnInfo

__all__ = ["Mapped"]

class Mapped[V]:
    """This class is used to define a column that is mapped to a field in a table.

    Example:
    ```
        id: Mapped[uuid.UUID] = Column()
        name: Mapped[str] = Column()
        age: Mapped[int] = Column()
    ```
    """

    @overload
    def __get__(self, instance: None, owner: Type[Any]) -> 'ColumnInfo':
        ...

    @overload
    def __get__(self, instance: Any, owner: Type[Any]) -> V:
        ...

    def __get__(self, instance: Optional[Any], owner: Type[Any]) -> Union[V, 'ColumnInfo']:
        ...