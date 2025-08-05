from typing import Any, Dict, Iterable, List, Optional, TypedDict, Union, Unpack

from ..core.enums import IndexMethod, NullsOrder, Order
from ..core.registry import register_index
from ..core.utils import MISSING

__all__ = (
    "Index",
    "IndexColumn",
    "GistIndex",
    "SpGistIndex",
    "GinIndex",
    "BrinIndex",
    "BTreeIndex",
    "HashIndex",
)

class IndexColumn:
    """Represents a column in an index."""
    
    def __init__(self, 
        name: str, 
        /, *, 
        order: Optional[Order] = None,
        nulls: Optional[NullsOrder] = None,
        collation: Optional[str] = None,
        opclass: Optional[str] = None,
        expression: Optional[str] = None
    ):
        """
        Create an index column.
        
        Args:
            name: Name of the column
            order: Optional order for the column (ASC/DESC)
            nulls: Optional nulls order (FIRST/LAST)
            collation: Optional collation for the column
            opclass: Optional operator class for the column
            expression: Optional expression for the column
        """

        self.name = name
        self.order = order
        self.nulls = nulls
        self.collation = collation
        self.opclass = opclass
        self.expression = expression

    def __str__(self) -> str:
        parts = []
        
        # Use expression if provided, otherwise column name
        base = self.expression if self.expression else self.name
        parts.append(base)
        
        if self.collation:
            parts.append(f"COLLATE {self.collation}")
        
        if self.opclass:
            parts.append(self.opclass)
        
        if self.order:
            parts.append(self.order.value.upper())
        
        if self.nulls:
            parts.append(f"NULLS {self.nulls.upper()}")
        
        return " ".join(parts)
    
    def __repr__(self) -> str:
        if self.expression:
            return f"IndexColumn(expression={self.expression!r})"
        return f"IndexColumn(column={self.name!r})"
    
class _IndexOptions(TypedDict, total=False):
    """TypedDict for index options."""
    
    unique: bool
    where: Optional[str]
    include: Optional[List[str]]
    tablespace: Optional[str]
    with_options: Optional[Dict[str, Any]]
    concurrently: bool

class Index:
    """PostgreSQL index type."""

    def __init__(self,
        columns: Optional[Iterable[Union[str, IndexColumn]]] = None,
        /,
        name: str = MISSING,
        method: IndexMethod = IndexMethod.BTREE,
        **kwargs: Unpack[_IndexOptions]
    ):
        """
        Create a PostgreSQL index.
        
        Args:
            columns: List of columns to index
            name: Optional index name
            unique: Whether the index is unique
            method: Index method (default: BTREE)
            where: Optional partial index condition
            include: Optional list of included columns
            tablespace: Optional tablespace for the index
            with_options: Additional options for the index
            concurrently: Whether to create the index concurrently
        """

        self.method = method

        self.unique = kwargs.get("unique", False)
        self.where = kwargs.get("where")
        self.include = kwargs.get("include")
        self.tablespace = kwargs.get("tablespace")
        self.with_options = kwargs.get("with_options")
        self.concurrently = kwargs.get("concurrently", False)

        self.columns: List[IndexColumn] = []

        if columns:
            for column in columns:
                if isinstance(column, IndexColumn):
                    self.columns.append(column)
                elif isinstance(column, str):
                    self.columns.append(IndexColumn(column))
                else:
                    raise TypeError(f"Invalid column type: {type(column)}. Must be str or IndexColumn.")
                
        if name:
            self.name = name
        elif self.columns:
            self.name = f"idx_{'_'.join(c.name for c in self.columns)}"
        else:
            self.name = MISSING
            
        register_index(self)

    def __str__(self) -> str:
        return self.name
    
    def __repr__(self) -> str:
        return f"Index(name={self.name!r}, columns={[c.name for c in self.columns]!r})"
    
def BTreeIndex(
    columns: Optional[Iterable[Union[str, IndexColumn]]] = None,
    /,
    name: str = MISSING,
    **kwargs: Unpack[_IndexOptions]
) -> Index:
    """
    Create a BTree index.
    
    Args:
        columns: List of columns to index
        name: Optional index name
        unique: Whether the index is unique
        where: Optional partial index condition
        include: Optional list of included columns
        tablespace: Optional tablespace for the index
        with_options: Additional options for the index
        concurrently: Whether to create the index concurrently
    """
    kwargs.pop("method", None)  # Ensure method is not set for BTreeIndex
    
    return Index(
        columns,
        name=name,
        method=IndexMethod.BTREE,
        **kwargs
    )

def HashIndex(
    columns: Optional[Iterable[Union[str, IndexColumn]]] = None,
    /,
    name: str = MISSING,
    **kwargs: Unpack[_IndexOptions]
) -> Index:
    """
    Create a Hash index.
    
    Args:
        columns: List of columns to index
        name: Optional index name
        unique: Whether the index is unique
        where: Optional partial index condition
        include: Optional list of included columns
        tablespace: Optional tablespace for the index
        with_options: Additional options for the index
        concurrently: Whether to create the index concurrently
    """
    kwargs.pop("method", None)  # Ensure method is not set for HashIndex
    
    return Index(
        columns,
        name=name,
        method=IndexMethod.HASH,
        **kwargs
    )

def GistIndex(
    columns: Optional[Iterable[Union[str, IndexColumn]]] = None,
    /,
    name: str = MISSING,
    **kwargs: Unpack[_IndexOptions]
) -> Index:
    """
    Create a GIST index.
    
    Args:
        columns: List of columns to index
        name: Optional index name
        unique: Whether the index is unique
        where: Optional partial index condition
        include: Optional list of included columns
        tablespace: Optional tablespace for the index
        with_options: Additional options for the index
        concurrently: Whether to create the index concurrently
    """
    kwargs.pop("method", None)  # Ensure method is not set for GistIndex
    
    return Index(
        columns,
        name=name,
        method=IndexMethod.GIST,
        **kwargs
    )

def SpGistIndex(
    columns: Optional[Iterable[Union[str, IndexColumn]]] = None,
    /,
    name: str = MISSING,
    **kwargs: Unpack[_IndexOptions]
) -> Index:
    """
    Create a SPGIST index.
    
    Args:
        columns: List of columns to index
        name: Optional index name
        unique: Whether the index is unique
        where: Optional partial index condition
        include: Optional list of included columns
        tablespace: Optional tablespace for the index
        with_options: Additional options for the index
        concurrently: Whether to create the index concurrently
    """
    kwargs.pop("method", None)  # Ensure method is not set for SpGistIndex
    
    return Index(
        columns,
        name=name,
        method=IndexMethod.SPGIST,
        **kwargs
    )

def GinIndex(
    columns: Optional[Iterable[Union[str, IndexColumn]]] = None,
    /,
    name: str = MISSING,
    **kwargs: Unpack[_IndexOptions]
) -> Index:
    """
    Create a GIN index.
    
    Args:
        columns: List of columns to index
        name: Optional index name
        unique: Whether the index is unique
        where: Optional partial index condition
        include: Optional list of included columns
        tablespace: Optional tablespace for the index
        with_options: Additional options for the index
        concurrently: Whether to create the index concurrently
    """
    kwargs.pop("method", None)  # Ensure method is not set for GinIndex
    
    return Index(
        columns,
        name=name,
        method=IndexMethod.GIN,
        **kwargs
    )

def BrinIndex(
    columns: Optional[Iterable[Union[str, IndexColumn]]] = None,
    /,
    name: str = MISSING,
    **kwargs: Unpack[_IndexOptions]
) -> Index:
    """
    Create a BRIN index.
    
    Args:
        columns: List of columns to index
        name: Optional index name
        unique: Whether the index is unique
        where: Optional partial index condition
        include: Optional list of included columns
        tablespace: Optional tablespace for the index
        with_options: Additional options for the index
        concurrently: Whether to create the index concurrently
    """
    kwargs.pop("method", None)  # Ensure method is not set for BrinIndex
    
    return Index(
        columns,
        name=name,
        method=IndexMethod.BRIN,
        **kwargs
    )