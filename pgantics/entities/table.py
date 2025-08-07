from typing import Any, Dict, List, Optional, Tuple, Union

from ..core.enums import PartitionStrategy
from ..core.exceptions import ValidationError
from ..core.registry import (
    _TABLE_REGISTRY,
    get_table_class,
    get_type_class,
    register_table,
)
from ..fields.column import ColumnInfo, CompositeColumnInfo
from ..query.insert import InsertQuery
from ..query.select import SelectQuery
from ..query.update import UpdateQuery
from ..types.complex import Array
from ..types.foreign_key import ForeignKey
from .base import PGAnticsModel
from .check import Check
from .index import Index
from .partitions import Partition, PartitionBound
from .unique import Unique

__all__ = (
    "Table",
    "validate"
)

class Table(PGAnticsModel):
    """A PostgreSQL table, representing a collection of rows with defined columns."""

    __entity_name__ = "Table"
    __supported_field_types__ = (ColumnInfo,)

    class Meta:
        table_name: str
        schema: Optional[str] = None
        comment: Optional[str] = None
        composite_pk: Optional[bool] = False

        # Constraints
        checks: Optional[List[Check]] = None
        exclude_constraints: Optional[str] = None

        # Indexes
        indexes: Optional[List[Index]] = None

        # partitions
        partition_by: Optional[PartitionStrategy] = None
        partition_key: Optional[Union[str, List[str]]] = None
        partitions: Optional[List[Partition]] = None

        partition_of: Optional[Union['Table', str]] = None
        partition_bound: Optional[PartitionBound] = None

    def __init_subclass__(cls, **kwargs: Any):
        register_table(cls)
        return super().__init_subclass__(**kwargs)
    
    @classmethod
    def get_sql_metadata(cls) -> Dict[str, Any]:
        CompositeType = get_type_class('CompositeType')

        metadata = {x: getattr(cls.Meta, x) for x in dir(cls.Meta) if not x.startswith('_') and not callable(getattr(cls.Meta, x))}
        metadata.update({
            'primary_key': [],
            'unique_constraints': {},
            'columns': {},
        })

        if 'indexes' not in metadata:
            metadata['indexes'] = []

        for field_name, field_info in cls.__pgdantic_fields__.items():
            if isinstance(field_info, CompositeColumnInfo):
                raise ValidationError(
                    f"Composite columns are not supported in table '{cls.__name__}'. Use a separate CompositeType for complex structures."
                )
            
            if isinstance(field_info, ColumnInfo):
                meta: Any = field_info.__sql_metadata__
                meta['type'] = field_info.postgres_type

                if issubclass(meta['type'], CompositeType):
                    meta['type'] = meta['type'].get_sql_metadata()

                if field_info.primary_key:
                    metadata['primary_key'].append(field_name)

                if field_info.unique and isinstance(field_info.unique, Unique):
                    metadata['unique_constraints'].setdefault(field_info.unique.composite_group, []).append(field_name)

                if field_info.index:
                    metadata['indexes'].append(field_info.index)

                metadata['columns'][field_name] = meta

        if len(metadata['primary_key']) == 0:
            raise ValidationError(f"Table '{cls.__name__}' must have at least one primary key defined.")
        elif not metadata.get('composite_pk', False) and len(metadata['primary_key']) > 1:
            raise ValidationError(f"Table '{cls.__name__}' has multiple primary keys defined but 'composite_pk' is not set to True in the Meta class.")

        return metadata

    @classmethod
    def full_table_name(cls) -> str:
        """Get the full table name including schema if specified."""

        if hasattr(cls.Meta, 'schema') and cls.Meta.schema:
            return f"{cls.Meta.schema}.{cls.Meta.table_name}"
        return cls.Meta.table_name
    
    @classmethod
    def primary_key(cls) -> Tuple[str, ...]:
        """Return the primary key column if it exists."""

        metadata = cls.get_sql_metadata()
        return tuple(metadata['primary_key'])
        
    @classmethod
    def get_indexes(cls) -> List[Index]:
        """Return the indexes defined for this table."""

        metadata = cls.get_sql_metadata()
        return list(metadata['indexes'].values())
    
    @classmethod
    def get_foreign_keys(cls) -> Dict[str, ColumnInfo]:
        """Retrieve foreign keys defined in the model."""

        return {
            name: field for name, field in cls.__pgdantic_fields__.items()
            if isinstance(field, ColumnInfo) and isinstance(field.postgres_type, ForeignKey)
        }
    
    @classmethod
    def select(cls, *fields: Union[str, Any]) -> 'SelectQuery':
        """Start a SELECT query"""
        query = SelectQuery(cls)
        
        for field in fields:
            if isinstance(field, ColumnInfo):
                table = field._source_class
                field = field._source_field_name
            elif isinstance(field, str):
                table = cls
            else:
                raise ValidationError(f"Invalid field type: {type(field)}. Must be a string or ColumnInfo.")

            if field not in table.__pgdantic_fields__:
                raise ValidationError(f"Field '{field}' does not exist in table '{table.__name__}'.")
            
            query.add_select_field(field)

        return query
    
    def insert(self, *columns: Union[str, Any]) -> 'InsertQuery':
        """Start an INSERT query"""
        return InsertQuery(self).values(*columns)

    def update(self) -> 'UpdateQuery':
        """Start an UPDATE query"""
        return UpdateQuery(self)

    #@classmethod
    #def delete(cls) -> 'DeleteQuery':
    #    """Start a DELETE query"""
    #    return DeleteQuery(cls)

register_table(Table)

def validate() -> None:
    """Validate all registered tables, composite types, foreign keys, etc. This should be called after all models are defined."""

    Table = get_table_class('Table')

    for table in _TABLE_REGISTRY.values():
        if table is Table:
            continue

        for field_name, field_info in table.__pgdantic_fields__.items():
            if isinstance(field_info, ColumnInfo):
                try:
                    postgres_type = field_info.postgres_type # calls the cached_property in order to validate the postgres type
                except Exception as e:
                    raise ValidationError(
                        f"Error validating field '{field_name}' in table '{table.__name__}': {str(e)}"
                    ) from e
                
                if isinstance(postgres_type, ForeignKey):
                    field_info._check_foreign_key()
                elif isinstance(postgres_type, Array):
                    postgres_type.element_type  # calls the cached_property in order to validate the element type

                field_info._set_index_name() # set index name if not already set