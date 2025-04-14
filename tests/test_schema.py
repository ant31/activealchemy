import pytest
from pydantic import ValidationError
from typing import Optional

from activealchemy import ActiveRecord, Schema, Base
from sqlalchemy.orm import Mapped, mapped_column


# --- Mock Model and Schema ---

class MockModel(Base):
    """A simple mock ActiveRecord model for testing schemas."""
    __tablename__ = "mock_models" # Required by Base/ActiveRecord

    id: Mapped[int] = mapped_column(primary_key=True) # Example PK, removed init=False
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


# --- Test Cases ---

def test_schema_initialization():
    """Test creating a schema instance."""
    data = {"name": "Test Name", "value": 123}
    schema_instance = MockSchema(**data)
    assert schema_instance.name == "Test Name"
    assert schema_instance.value == 123

def test_schema_validation():
    """Test Pydantic validation within the schema."""
    # Missing required field 'name'
    with pytest.raises(ValidationError):
        MockSchema(value=456)

    # Incorrect type for 'value'
    with pytest.raises(ValidationError):
        MockSchema(name="Test Name", value="not a number")

def test_schema_to_model():
    """Test converting a schema instance to a model instance."""
    schema_instance = MockSchema(name="Schema to Model", value=789)
    model_instance = schema_instance.to_model(MockModel)

    assert isinstance(model_instance, MockModel)
    assert model_instance.name == "Schema to Model"
    assert model_instance.value == 789
    # 'id' will not be set as it's not in the schema and marked init=False

def test_schema_from_model():
    """Test creating a schema instance from a model instance."""
    model_instance = MockModel(name="Model to Schema", value=101)
    # Simulate adding an ID as if it came from DB
    model_instance.id = 1

    schema_instance = MockSchema.model_validate(model_instance)

    assert schema_instance.name == "Model to Schema"
    assert schema_instance.value == 101
    # 'id' should not be included by default unless explicitly added to schema

def test_schema_add_fields_simple():
    """Test dynamically adding fields with default values."""
    # Add fields *before* creating an instance
    MockSchema.add_fields(
        extra_field_str="default_string",
        extra_field_int=99
    )

    # Verify fields exist on the class
    assert "extra_field_str" in MockSchema.model_fields
    assert "extra_field_int" in MockSchema.model_fields

    # Create instance and check defaults
    schema_instance = MockSchema(name="Test Add Fields")
    assert schema_instance.extra_field_str == "default_string"
    assert schema_instance.extra_field_int == 99

    # Create instance overriding defaults
    schema_instance_override = MockSchema(
        name="Test Add Fields Override",
        extra_field_str="overridden",
        extra_field_int=100
    )
    assert schema_instance_override.extra_field_str == "overridden"
    assert schema_instance_override.extra_field_int == 100

    # Clean up added fields for other tests (Pydantic models are modified globally)
    # This is tricky. A better approach might be to define a new schema per test.
    # For now, let's try removing them (may not be officially supported)
    if "extra_field_str" in MockSchema.model_fields:
        del MockSchema.model_fields["extra_field_str"]
        if "extra_field_str" in MockSchema.__annotations__:
             del MockSchema.__annotations__["extra_field_str"]
    if "extra_field_int" in MockSchema.model_fields:
        del MockSchema.model_fields["extra_field_int"]
        if "extra_field_int" in MockSchema.__annotations__:
             del MockSchema.__annotations__["extra_field_int"]
    MockSchema.model_rebuild(force=True)


def test_schema_add_fields_typed():
    """Test dynamically adding fields with type and default."""

    # Define a temporary schema to avoid polluting MockSchema globally
    class TempSchema(Schema[MockModel]):
        name: str
        value: Optional[int] = None

    TempSchema.add_fields(
        typed_field=(str, "typed_default"),
        optional_typed_field=(Optional[bool], None)
    )

    assert "typed_field" in TempSchema.model_fields
    assert TempSchema.model_fields["typed_field"].annotation is str
    assert "optional_typed_field" in TempSchema.model_fields
    assert TempSchema.model_fields["optional_typed_field"].annotation is Optional[bool]


    # Test instantiation and validation
    instance1 = TempSchema(name="Typed Fields")
    assert instance1.typed_field == "typed_default"
    assert instance1.optional_typed_field is None

    instance2 = TempSchema(
        name="Typed Fields Override",
        typed_field="override",
        optional_typed_field=True
    )
    assert instance2.typed_field == "override"
    assert instance2.optional_typed_field is True

    # Test type validation
    with pytest.raises(ValidationError):
        TempSchema(name="Bad Type", typed_field=123) # Expecting str

    with pytest.raises(ValidationError):
        TempSchema(name="Bad Optional Type", optional_typed_field="not a bool")


def test_schema_add_fields_invalid_definition():
    """Test error handling for invalid field definitions in add_fields."""
    class InvalidDefSchema(Schema[MockModel]):
        name: str

    with pytest.raises(ValueError, match="Field definitions should either be a tuple"):
        # Providing a tuple as the default value directly is ambiguous
        InvalidDefSchema.add_fields(bad_field=("tuple", "as", "default"))

    # Check tuple definition length
    with pytest.raises(ValueError, match="Field definitions should either be a tuple"):
        InvalidDefSchema.add_fields(bad_tuple_def=("just_type",)) # Too short

    with pytest.raises(ValueError, match="Field definitions should either be a tuple"):
        InvalidDefSchema.add_fields(bad_tuple_def=(str, "default", "extra")) # Too long


def test_schema_extra_fields_allowed():
    """Test that extra fields are allowed by default config."""
    data = {"name": "Test Name", "value": 123, "unexpected_field": "some_value"}
    # Should not raise validation error if extra='allow'
    try:
        schema_instance = MockSchema(**data)
        assert schema_instance.name == "Test Name"
        assert schema_instance.value == 123
        # Check if the extra field is accessible (depends on Pydantic version/config)
        # In Pydantic v2 with extra='allow', it's often stored in __pydantic_extra__
        assert getattr(schema_instance, 'unexpected_field', None) == "some_value" or \
               schema_instance.__pydantic_extra__.get('unexpected_field') == "some_value"

    except ValidationError as e:
        pytest.fail(f"Schema validation failed unexpectedly with extra='allow': {e}")
