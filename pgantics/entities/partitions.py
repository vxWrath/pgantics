

from typing import Any, Optional, Sequence

__all__ = (
    "PartitionBound",
    "RangePartitionBound",
    "ListPartitionBound",
    "HashPartitionBound",
    "Partition",
)

class PartitionBound:
    """Base class for partition bounds."""
    pass

class RangePartitionBound(PartitionBound):
    """Represents a range partition bound."""

    def __init__(self, from_value: Any, to_value: Any):
        self.from_value = from_value
        self.to_value = to_value
    
    def __str__(self) -> str:
        return f"FROM ({self.from_value}) TO ({self.to_value})"
    
class ListPartitionBound(PartitionBound):
    def __init__(self, values: Sequence[Any]):
        self.values = values
    
    def __str__(self) -> str:
        formatted_values = [f"'{v}'" if isinstance(v, str) else str(v) for v in self.values]
        return f"IN ({', '.join(formatted_values)})"
    
class HashPartitionBound(PartitionBound):
    def __init__(self, modulus: int, remainder: int):
        self.modulus = modulus
        self.remainder = remainder
    
    def __str__(self) -> str:
        return f"WITH (MODULUS {self.modulus}, REMAINDER {self.remainder})"
    
class Partition:
    """Base class for partitions."""
    
    def __init__(self, name: str, bound: PartitionBound, tablespace: Optional[str] = None):
        self.name = name
        self.bound = bound
        self.tablespace = tablespace
    
    def __str__(self) -> str:
        if self.tablespace:
            return f"PARTITION {self.name} FOR VALUES {self.bound} IN TABLESPACE {self.tablespace}"
        return f"PARTITION {self.name} FOR VALUES {self.bound}"