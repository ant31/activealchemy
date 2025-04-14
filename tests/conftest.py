import uuid

import pytest


from typing import Optional
from activealchemy import PostgreSQLConfigSchema, Base, Schema, ActiveRecord
from sqlalchemy.orm import Mapped, mapped_column


# --- Mock Model and Schema for test_schema.py ---

class MockModel(Base):
    """A simple mock ActiveRecord model for testing schemas."""
    __tablename__ = "mock_models" # Required by Base/ActiveRecord

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    value: Mapped[Optional[int]] = mapped_column(default=None)

    # Add __init__ if not using MappedAsDataclass or similar helpers
    # Base doesn't inherit MappedAsDataclass by default
    def __init__(self, name: str, value: Optional[int] = None, **kw):
        super().__init__(**kw) # Pass extra kwargs to SQLAlchemy internals if needed
        self.name = name
        self.value = value


class MockSchema(Schema[MockModel]):
    """Schema corresponding to MockModel."""
    name: str
    value: Optional[int] = None
    # We don't include 'id' here typically, as it's often DB-generated


@pytest.fixture(scope="session") # Use session scope as the class definition doesn't change
def mock_model_class():
    """Provides the MockModel class."""
    return MockModel

@pytest.fixture(scope="session") # Use session scope as the base class definition doesn't change
def mock_schema_class():
    """Provides the MockSchema class."""
    # Return a fresh copy or the class itself.
    # Be mindful if tests modify the class directly.
    return MockSchema


# --- Other Utility fixtures ---
@pytest.fixture
def unique_id():
    """Generate a unique ID for test data"""
    return str(uuid.uuid4())


@pytest.fixture
def db_config():
    """Provide database configuration for tests."""
    # Example: Load from environment variables or a test config file
    # For simplicity, using defaults here. Replace with your actual config loading.
    return PostgreSQLConfigSchema()

