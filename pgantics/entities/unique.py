__all__ = (
    'Unique',
)

class Unique:
    def __init__(self, composite_group: str, /):
        """
        Represents a postgres UNIQUE constraint.

        Args:
            composite_group (str): The name of the composite group for the unique constraint.
                This is used to group multiple unique constraints together in a composite key.
        """
        self.composite_group = composite_group

    def __str__(self):
        return self.composite_group

    def __repr__(self):
        return f"Unique(composite_group={self.composite_group!r})"