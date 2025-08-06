# This file is just meant for testing

import datetime
import json
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


class Address(Table):
    class Meta:
        table_name = "addresses"

    id: int = Column(BigSerial, primary_key=True)
    street: str = Column(nullable=False)
    city: str = Column(nullable=False)
    state: str = Column(nullable=False)
    zip_code: str = Column(nullable=False)
    user_id: int = Column(ForeignKey("users.id"), nullable=False, index=BTreeIndex())

class User(Table):
    class Meta:
        table_name = "users"
        composite_pk = True

    id: int = Column(BigSerial, primary_key=True)
    address_id: int = Column(BigSerial, primary_key=True)
    username: str = Column(unique=Unique("name"), nullable=False)
    email: str = Column(unique=Unique("name"), nullable=False)

    settings: Dict[str, Any] = Column(JSONB)
    created_at: datetime.datetime = Column(TimestampTZ(), default=default.CurrentTimestamp("UTC"), nullable=False, index=BTreeIndex())

validate()

print(json.dumps(User.get_sql_metadata(), indent=4, default=str))
print()
print(json.dumps(Address.get_sql_metadata(), indent=4, default=str))