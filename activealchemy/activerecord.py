"""
Backward compatibility module that re-exports from sync/activerecord.py
"""

from activealchemy.sync.activerecord import (
    ActiveRecord,
    Base,
    PKMixin,
    Schema,
    Select,
    UpdateMixin,
)

__all__ = [
    "ActiveRecord",
    "Base",
    "PKMixin",
    "Schema",
    "Select",
    "UpdateMixin",
]
