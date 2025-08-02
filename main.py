# This file is just meant for testing

import json
from typing import List

from pgantics import (
    Action,
    Array,
    BigInt,
    Column,
    CompositeColumn,
    CompositeType,
    EnumType,
    ForeignKey,
    Table,
    validate,
)


class User(Table):
    class Meta:
        table_name = "users"

    id: int = Column(primary_key=True)
    username: str = Column(unique=True, nullable=False)
    email: str = Column(unique=True, nullable=False)
    created_at: str = Column(nullable=False)

    roles: str = Column('Roles')


class Furniture(CompositeType):
    class Meta:
        type_name = "furniture"

    name: str = CompositeColumn()
    material: str = CompositeColumn()
    dimensions: str = CompositeColumn()

class Address(Table):
    class Meta:
        table_name = "addresses"

    id: int = Column(BigInt, primary_key=True)
    user_id: int = Column(ForeignKey(User, "id", on_delete=Action.CASCADE), nullable=False)
    street: str = Column(nullable=False)
    city: str = Column(nullable=False)
    state: str = Column(nullable=False)
    zip_code: str = Column(nullable=False)
    furniture: List[Furniture] = Column(Array(Furniture), nullable=True)

class Roles(EnumType):
    class Meta:
        type_name = "user_roles"

    ADMIN = "admin"
    USER  = "user"
    GUEST = "guest"

validate()

print(json.dumps(Address.get_sql_metadata(), indent=2, default=str))