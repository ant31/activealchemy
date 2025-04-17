"""
Tests for activealchemy/activerecord.py
"""
import pytest
from sqlalchemy.orm import Mapped, mapped_column # Import Mapped and mapped_column

from activealchemy import ActiveEngine, ActiveRecord, Base, PKMixin, PostgreSQLConfigSchema

# --- Fixtures ---

# Use the async_engine fixture from tests/async/conftest.py
# It sets the engine globally via ActiveRecord.set_engine

# Define a simple model for testing ActiveRecord methods directly
# Inherit PKMixin to get an 'id' primary key
class SimpleModel(Base, PKMixin):
    __tablename__ = "simple_models_activerecord"
    # Add a field required by MappedAsDataclass (inherited via PKMixin)
    name: Mapped[str] = mapped_column(init=True, default=None)
    # Inherits engine and session factory from Base/ActiveRecord


# --- Test Cases ---

@pytest.mark.asyncio
async def test_engine_management(async_engine, db_config):
    """Test set_engine and engine methods."""
    # 1. Test that engine is set correctly by the fixture
    assert ActiveRecord.engine() is async_engine
    assert SimpleModel.engine() is async_engine

    # 2. Test setting a different engine
    # Create a dummy config and engine
    dummy_config = PostgreSQLConfigSchema(db="dummy", user="dummy", password="dummy", host="dummy")
    dummy_engine = ActiveEngine(dummy_config)

    # Set the dummy engine on the specific class
    SimpleModel.set_engine(dummy_engine)
    assert SimpleModel.engine() is dummy_engine
    # Verify ActiveRecord base still has the original engine
    assert ActiveRecord.engine() is async_engine

    # 3. Test error when engine is not set
    class UnconfiguredModel(ActiveRecord): # Don't inherit from Base which might have engine set
        __tablename__ = "unconfigured_activerecord"
        # No set_engine called

    with pytest.raises(ValueError, match="No active engine configured"):
        UnconfiguredModel.engine()

    # 4. Test set_engine with invalid type
    with pytest.raises(TypeError, match="Engine must be an instance of ActiveEngine"):
        SimpleModel.set_engine("not an engine") # type: ignore

    # Reset engine for SimpleModel to avoid affecting other tests
    SimpleModel.set_engine(async_engine)
    assert SimpleModel.engine() is async_engine
