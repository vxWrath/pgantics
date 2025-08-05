# This file is just meant for testing

import datetime
import json
from typing import Any, Dict

from pgantics import (
    JSONB,
    BigSerial,
    Column,
    Index,
    Table,
    TimestampTZ,
    default,
    validate,
)


class User(Table):
    class Meta:
        table_name = "users"

    id: int = Column(BigSerial, primary_key=True)
    username: str = Column(unique=True, nullable=False)
    email: str = Column(unique=True, nullable=False)

    settings: Dict[str, Any] = Column(JSONB(), default=default.Cast({"theme": "light", "notifications": True}, postgres_type=JSONB))
    created_at: datetime.datetime = Column(TimestampTZ(), default=default.CurrentTimestamp("UTC"), nullable=False, index=Index())

validate()

print(json.dumps(User.get_sql_metadata(), indent=2, default=str))
print(User.full_table_name())
print(User.primary_key())
print(User.get_indexes())