from functools import cached_property
from typing import Any, Iterable, List, Optional, Type, Union

from ..core.registry import get_type_class, register_domain
from ..entities.check import (
    Check,
    check_email_format,
    check_non_negative,
    check_not_empty,
    check_positive,
    check_range,
)
from ..entities.default import _DefaultValue
from .base import PostgresType, to_postgres_type
from .primitives import Decimal, Integer, Text


class Domain(PostgresType):
    """PostgreSQL domain type."""

    def __init__(self,
        base: Union[Type[PostgresType], PostgresType, Any, str],
        /,
        name: str,
        *,
        nullable: bool = True,
        default: Optional[Union[str, _DefaultValue]] = None,
        checks: Optional[Iterable[Check]] = None,
        collation: Optional[str] = None,
    ):
        """
        Create a domain type.
        
        Args:
            base: The underlying PostgreSQL type
            name: The domain name
            nullable: Whether the domain can be NULL
            default: Default value for the domain
            checks: List of CHECK constraints
            collation: Collation for the domain
        """

        if isinstance(default, str):
            default = _DefaultValue(default, is_expression=True)

        self._base = base
        self.name = name
        self.nullable = nullable
        self.default = default
        self.checks: List[Check] = list(checks) if checks else []
        self.collation = collation

    @cached_property
    def base(self) -> Union[Type[PostgresType], PostgresType]:
        """Get the base type of the domain."""
        CompositeType = get_type_class('CompositeType')

        if isinstance(self._base, str):
            base = get_type_class(self._base)
        elif isinstance(self._base, type) and issubclass(self._base, PostgresType):
            if issubclass(self._base, CompositeType):
                base = self._base
            else:
                base = self._base()
        elif isinstance(self._base, PostgresType):
            base = self._base
        else:
            base = to_postgres_type(self._base)

        return base
    
    def __str__(self) -> str:
        return self.name
    
    def __repr__(self) -> str:
        return f"Domain(name={self.name!r}, base={self.base!r})"

    def add_check(self, check: Check) -> None:
        """Add a CHECK constraint to the domain."""
        self.checks.append(check)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        register_domain(cls)
        return super().__init_subclass__(**kwargs)

# Common domain types
def email_domain(name: str = "email", collation: Optional[str] = None) -> Domain:
    """Create an email domain type."""
    
    return Domain(
        Text(),
        name=name,
        nullable=False,
        checks=[
            check_not_empty("VALUE"),
            check_email_format("VALUE", name=f"{name}_format_check")
        ],
        collation=collation
    )

def positive_integer_domain(name: str = "positive_int", collation: Optional[str] = None) -> Domain:
    """Create a positive integer domain type."""
    
    return Domain(
        Integer(),
        name=name,
        checks=[check_positive("VALUE")],
        collation=collation
    )

def currency_domain(name: str = "currency", precision: int = 10, scale: int = 2, collation: Optional[str] = None) -> Domain:
    """Create a currency domain type."""
    
    return Domain(
        Decimal(precision=precision, scale=scale),
        name=name,
        checks=[check_non_negative("VALUE")],
        collation=collation
    )

def percentage_domain(name: str = "percentage", collation: Optional[str] = None) -> Domain:
    """Create a percentage domain type (0-100)."""

    return Domain(
        Decimal(precision=5, scale=2),
        name=name,
        checks=[check_range("VALUE", 0, 100)],
        collation=collation
    )