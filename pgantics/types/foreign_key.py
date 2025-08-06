from functools import cached_property
from typing import TYPE_CHECKING, Optional, Type, Union

from ..core.enums import Action
from ..core.exceptions import ForeignKeyReferenceError, InvalidType
from ..core.registry import get_table_class
from .base import PostgresType

if TYPE_CHECKING:
    from ..entities.table import Table

__all__ = (
    "ForeignKey",
)

class ForeignKey(PostgresType):
    def __init__(self, 
        *reference: Union[Type['Table'], str], 
        on_delete: Optional[Action] = None, 
        on_update: Optional[Action] = None
    ):
        """Initialize a ForeignKey type.
        
        Args:
            reference (Union[Type['Table'], str]): The reference to the foreign key. This can be an actual table class or a table name, then a column name.
                Example: `ForeignKey(User, 'id')` or `ForeignKey("users.id")`.
            on_delete (Optional[Action]): Action to take on delete. Defaults to None.
            on_update (Optional[Action]): Action to take on update. Defaults to None.
        """

        if not reference:
            raise ForeignKeyReferenceError("You must provide a reference")

        if len(reference) == 1:
            ref = reference[0]

            if not isinstance(ref, str):
                raise ForeignKeyReferenceError(f"Could not parse foreign key '{ref}'.")
            
            ref = ref.split('.')

            if len(ref) != 2:
                raise ForeignKeyReferenceError(f"Foreign key reference must be in the format '<table-name or class>.column', got '{reference}'.")
            
            table, column = tuple(ref)

        elif len(reference) == 2:
            table, column = reference

        else:
            raise ForeignKeyReferenceError(f"Invalid foreign key reference format, '{reference}'. Expected '<table or class>.column'.")

        if not isinstance(column, str):
            raise ForeignKeyReferenceError(f"Foreign key column must be a string, got '{type(column).__name__}'.")

        self._table = table
        self.column = column

        self.on_delete = on_delete
        self.on_update = on_update

    @cached_property
    def table(self) -> Type['Table']:
        """Get the reference table type."""
        Table = get_table_class('Table')

        if isinstance(self._table, str):
            reference_table = get_table_class(self._table)
        elif issubclass(self._table, Table):
            reference_table = self._table
        else:
            raise InvalidType(f"Expect type 'Table' but got '{type(self._table).__name__}'.")

        if reference_table.pgdantic_fields().get(self.column) is None:
            raise ForeignKeyReferenceError(f"Reference column '{self.column}' does not exist on table class '{reference_table}'.")

        return reference_table

    def __str__(self) -> str:
        """Return the string representation of the FOREIGN KEY type."""
        ref_table = self.table

        on_delete_str = f" ON DELETE {self.on_delete.value}" if self.on_delete else ""
        on_update_str = f" ON UPDATE {self.on_update.value}" if self.on_update else ""

        return f"REFERENCES {ref_table.Meta.table_name}({self.column}){on_delete_str}{on_update_str}"