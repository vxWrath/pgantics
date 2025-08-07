# This file is just meant for testing

import datetime
import json  # type: ignore
from typing import Any, Dict

from pgantics import (
    JSONB,
    BigSerial,
    BTreeIndex,
    Column,
    Table,
    TimestampTZ,
    default,
    validate,
)
from pgantics.entities.unique import Unique
from pgantics.types.foreign_key import ForeignKey


class User(Table):
    class Meta:
        table_name = "users"
        composite_pk = True

    id: int = Column(BigSerial, primary_key=True)
    address_id: int = Column(BigSerial, primary_key=True)
    username: str = Column(unique=Unique("name"), nullable=False)
    email: str = Column(unique=Unique("name"), nullable=False)
    status: str = Column(nullable=False, default=default.String("active"))

    settings: Dict[str, Any] = Column(JSONB)
    created_at: datetime.datetime = Column(TimestampTZ(), default=default.CurrentTimestamp("UTC"), nullable=False, index=BTreeIndex())

class Address(Table):
    class Meta:
        table_name = "addresses"

    id: int = Column(BigSerial, primary_key=True)
    street: str = Column(nullable=False)
    city: str = Column(nullable=False)
    state: str = Column(nullable=False)
    zip_code: str = Column(nullable=False)
    user_id: int = Column(ForeignKey("users", "id"), nullable=False, index=BTreeIndex())

validate()

print(User.select().where(User.username == "john").build())
print(User.select().where(
    (User.created_at > datetime.datetime(2023, 1, 1)) & 
    (User.status.in_(["active", "premium"]))
).order_by("created_at DESC").limit(10).build())

print(User.select(User.username, Address.street)
    .join(Address)
    .on(User.id == Address.user_id)
    .where(User.username.like("john%"))
    .build()
)

user = User(id=1, username="john", email="john@example.com", status="active", settings={"theme": "dark"}, address_id=1, created_at=datetime.datetime.now())

print(user
    .insert(User.username, "settings")
    .returning(User.id)
    .build()
)