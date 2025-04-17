"""
Tests for activealchemy/activerecord.py
"""
import uuid  # Import uuid for tests

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


@pytest.mark.asyncio
async def test_instance_representation_and_data(unique_id):
    """Test instance representation (__str__, __repr__) and data methods (to_dict, dump_model, load, etc.)."""
    instance_name = f"repr_test_{unique_id}"
    instance = SimpleModel(name=instance_name)
    instance_id = instance.id # Get the generated UUID

    # 1. Test __str__ and __repr__
    expected_str = f"SimpleModel({instance_id})"
    # Dataclass repr includes init fields
    expected_repr = f"SimpleModel(id=UUID('{instance_id}'), name='{instance_name}')"
    assert str(instance) == expected_str
    # Use the specific dataclass repr format for the assertion
    assert repr(instance) == expected_repr

    # 2. Test id_key
    # Transient object id_key might vary, let's test after save
    # assert instance.id_key() == f"SimpleModel:transient_{id(instance)}" # Less reliable
    await instance.save(commit=True)
    assert instance.id_key() == f"SimpleModel:{instance_id}"

    # 3. Test __columns__fields__
    fields = SimpleModel.__columns__fields__()
    assert "id" in fields
    assert fields["id"][0] is uuid.UUID # Check type
    # Default is complex (function call), check it exists
    assert fields["id"][1] is not None

    assert "name" in fields
    assert fields["name"][0] is str
    assert fields["name"][1] is None # Default is None

    # 4. Test to_dict
    data_dict = instance.to_dict()
    assert data_dict == {"id": instance_id, "name": instance_name}

    data_dict_fields = instance.to_dict(fields={"name"})
    assert data_dict_fields == {"name": instance_name}

    data_dict_meta = instance.to_dict(with_meta=True)
    assert data_dict_meta["id"] == instance_id
    assert data_dict_meta["name"] == instance_name
    assert "__metadata__" in data_dict_meta
    assert data_dict_meta["__metadata__"]["model"] == "tests.async.test_activerecord:SimpleModel"
    assert data_dict_meta["__metadata__"]["table"] == "simple_models_activerecord"

    # 5. Test dump_model (should be JSON serializable)
    dumped_data = instance.dump_model()
    # UUID should be converted to string
    assert dumped_data == {"id": str(instance_id), "name": instance_name}
    # Test if it's actually JSON serializable (basic check)
    import json
    try:
        json.dumps(dumped_data)
    except TypeError:
        pytest.fail("dump_model output was not JSON serializable")

    # 6. Test load
    load_data = {"name": f"loaded_{unique_id}", "id": str(uuid.uuid4())} # Provide string UUID
    loaded_instance = SimpleModel.load(load_data)
    assert isinstance(loaded_instance, SimpleModel)
    assert loaded_instance.name == f"loaded_{unique_id}"
    # ID should be set, but might be string or UUID depending on load logic
    # ActiveRecord.load currently just sets attributes, so it might remain a string.
    # Let's check the type after potential conversion or direct set.
    # If load is expected to handle type conversion, this needs adjustment.
    # Current load just sets attributes, so it will be a string.
    assert loaded_instance.id == load_data["id"] # Check if it matches the input string

    # Test load with extra data (should be ignored)
    load_data_extra = {"name": f"loaded_extra_{unique_id}", "extra": "ignored"}
    loaded_extra = SimpleModel.load(load_data_extra)
    assert loaded_extra.name == f"loaded_extra_{unique_id}"
    assert not hasattr(loaded_extra, "extra")

    # Test load with non-dict
    with pytest.raises(ValueError, match="Input 'data' must be a dictionary"):
        SimpleModel.load("not a dict") # type: ignore
