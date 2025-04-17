"""
Tests for activealchemy/activerecord.py
"""
import pytest
from sqlalchemy.orm import Mapped, mapped_column  # Import Mapped and mapped_column

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

    # Explicitly remove the inherited engine for this test case
    UnconfiguredModel.__active_engine__ = None

    with pytest.raises(ValueError, match="No active engine configured"):
        UnconfiguredModel.engine()

    # 4. Test set_engine with invalid type
    with pytest.raises(TypeError, match="Engine must be an instance of ActiveEngine"):
        SimpleModel.set_engine("not an engine") # type: ignore

    # Reset engine for SimpleModel to avoid affecting other tests
    SimpleModel.set_engine(async_engine)
    assert SimpleModel.engine() is async_engine


@pytest.mark.asyncio
async def test_session_management(async_engine, unique_id):
    """Test session_factory, get_session, and obj_session methods."""
    # 1. Test session_factory returns the correct factory
    factory = SimpleModel.session_factory()
    assert factory is not None
    # Check if it's the factory associated with the default engine config
    _, expected_factory = async_engine.session() # Get the default factory
    assert factory is expected_factory

    # 2. Test get_session creates a new session
    async with await SimpleModel.get_session() as session1:
        assert session1 is not None
        assert session1.is_active

    # 3. Test get_session returns provided session
    async with await SimpleModel.get_session() as provided_session:
        returned_session = await SimpleModel.get_session(session=provided_session)
        assert returned_session is provided_session

    # 4. Test session_factory error when not configured
    class UnconfiguredModel(ActiveRecord):
        __tablename__ = "unconfigured_session_test"
        # No engine set

    UnconfiguredModel.__active_engine__ = None # Ensure no inherited engine
    UnconfiguredModel._session_factory = None # Ensure no inherited factory

    with pytest.raises(ValueError, match="Session factory not configured"):
        UnconfiguredModel.session_factory()

    # 5. Test obj_session
    instance = SimpleModel(name=f"session_test_{unique_id}")
    assert instance.obj_session() is None # Transient object has no session

    async with await SimpleModel.get_session() as s:
        # Add instance to session
        s.add(instance)
        await s.flush([instance]) # Flush to make it persistent in this session
        # Now the object should be associated with the session
        assert instance.obj_session() is s

        # Test obj_session after commit and refresh
        await s.commit()
        await s.refresh(instance)
        assert instance.obj_session() is s

    # Test obj_session after session is closed (should be None or detached state)
    # Accessing obj_session after close might be undefined or raise, depending on impl.
    # Let's check it's not the closed session.
    assert instance.obj_session() is not s # Session 's' is closed now
    # It might be None or associated with a new session if accessed later.
    # For now, just ensure it's not the closed one.

    # Reset engine for SimpleModel (done in test_engine_management teardown implicitly if needed)
    # SimpleModel.set_engine(async_engine)
