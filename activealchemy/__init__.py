"""
ActiveAlchemy: SQLAlchemy extension with ActiveRecord-like functionality

This package provides both synchronous and asynchronous APIs for database operations.
For backward compatibility, the root imports expose the synchronous API.
"""

from activealchemy.sync import (
    ActiveEngine,
    ActiveRecord,
    Base,
    PKMixin,
    Schema,
    Select,
    UpdateMixin,
)

__all__ = [
    "ActiveEngine",
    "ActiveRecord",
    "Base",
    "PKMixin",
    "Schema",
    "Select",
    "UpdateMixin",
]
