from functools import cache, cached_property
from typing import TYPE_CHECKING, Any, Optional, Type, Union

from ..core.enums import Action
from ..core.exceptions import ForeignKeyReferenceError, InvalidType
from ..core.registry import get_table_class
from ..core.utils import MISSING
from .base import PostgresType

if TYPE_CHECKING:
    from ..entities.table import Table
    from ..fields.column import ColumnInfo

__all__ = (
    "ForeignKey",
)

@cache
def _get_column_class() -> Type['ColumnInfo']:
    """Avoids circular import."""
    from ..fields.column import ColumnInfo
    return ColumnInfo

class ForeignKey(PostgresType):
    """
    Represents a PostgreSQL FOREIGN KEY constraint.

    This type allows you to specify a reference to another table's column,
    supporting multiple input formats for flexibility.

    Args:
        reference (Union[Type['Table'], str, Any]):
            The reference to the foreign key. Acceptable formats:
                - ForeignKey(User.id)
                - ForeignKey(User, 'id')
                - ForeignKey("User.id")
                - ForeignKey("users.id") # using table name
        on_delete (Optional[Action]): Action to take on delete (e.g., CASCADE, SET NULL).
        on_update (Optional[Action]): Action to take on update.

    Raises:
        ForeignKeyReferenceError: If the reference is missing or invalid.
        InvalidType: If the table type is incorrect.
    """

    def __init__(self, 
        *reference: Union[Type['Table'], str, Any], 
        on_delete: Optional[Action] = None, 
        on_update: Optional[Action] = None
    ):
        ColumnInfo = _get_column_class()

        self._info: Optional['ColumnInfo'] = None

        table: Union[Type['Table'], str] = MISSING
        column: str = MISSING

        if not reference:
            raise ForeignKeyReferenceError(
                "ForeignKey requires a reference to a table and column. "
                "Example: ForeignKey(User.id), ForeignKey(User, 'id'), or ForeignKey('users.id')."
            )

        if len(reference) == 1:
            ref = reference[0]

            if not isinstance(ref, (str, ColumnInfo)):
                raise ForeignKeyReferenceError(
                    f"Could not parse foreign key reference '{ref}'. "
                    "Expected a string in the format '<table>.<column>'."
                )

            if isinstance(ref, str):
                ref = ref.split('.')
                if len(ref) != 2:
                    raise ForeignKeyReferenceError(
                        f"Foreign key reference string must be in the format '<table>.<column>', got '{reference[0]}'."
                    )
                
                table, column = tuple(ref)
            else:
                self._info = ref

        elif len(reference) == 2:
            table, column = reference  # type: ignore

            if not isinstance(table, str) and not (isinstance(table, type) and issubclass(table, Table)):
                raise ForeignKeyReferenceError(
                    f"Since the format is ForeignKey(table, column), the foreign key table must be a string or a Table class, got '{type(table).__name__}'."
                )

            if not isinstance(column, str):
                raise ForeignKeyReferenceError(
                    f"Since the format is ForeignKey(table, column), the foreign key column must be a string, got '{type(column).__name__}'."
                )
        else:
            raise ForeignKeyReferenceError(
                f"Invalid foreign key reference format: '{reference}'. "
                "Expected one of: ForeignKey(User.id), ForeignKey(User, 'id'), or ForeignKey('users.id')."
            )

        self._table  = table
        self._column = column

        self.on_delete = on_delete
        self.on_update = on_update

    @cached_property
    def table(self) -> Type['Table']:
        """Get the reference table type."""
        Table = get_table_class('Table')

        if isinstance(self._table, str):
            reference_table = get_table_class(self._table)
        elif isinstance(self._table, type) and issubclass(self._table, Table):
            reference_table = self._table
        elif self._info and self._table is MISSING:
            reference_table = self._info._source_class
        else:
            raise InvalidType(
                f"ForeignKey.table: Expected a Table class or string, got '{type(self._table).__name__}'."
            )

        if reference_table.pgdantic_fields().get(self.column) is None:
            raise ForeignKeyReferenceError(
                f"Referenced column '{self.column}' does not exist on table '{reference_table.__name__}'."
            )

        return reference_table
    
    @cached_property
    def column(self) -> str:
        """Get the reference column name."""
        ColumnInfo = _get_column_class()

        if not self._info and not isinstance(self._column, str):
            raise ForeignKeyReferenceError(
                f"ForeignKey.column: Column must be a string, got '{type(self._column).__name__}'."
            )

        if isinstance(self._info, ColumnInfo):
            self._column = self._info._source_field_name
        
        self._column

        return self._column

    def __str__(self) -> str:
        sql = f"REFERENCES {self.table.Meta.table_name}({self.column})"

        if self.on_delete:
            sql += f" ON DELETE {self.on_delete.value}"

        if self.on_update:
            sql += f" ON UPDATE {self.on_update.value}"

        return sql