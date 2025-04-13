# -*- coding: utf-8 -*-
"""
Core base classes and mixins for the ActiveRecord pattern.

This module provides generic base classes intended to be shared between
synchronous and asynchronous implementations of an ActiveRecord pattern
using SQLAlchemy and Pydantic.

Key Components:
- BaseSelect: A placeholder for common Select logic (currently minimal).
- BaseActiveRecord: The core mixin providing shared ORM instance methods
  like serialization (`to_dict`, `dump_model`), loading (`load`),
  representation (`__str__`, `__repr__`), and engine management placeholders.
- BasePKMixin: Provides a standard UUID primary key column (`id`).
- BaseUpdateMixin: Provides standard timestamp tracking columns (`created_at`, `updated_at`).
- BaseSchema: A Pydantic BaseModel subclass for data validation and serialization,
  including helpers for converting to/from ORM models and dynamic field addition.

Dependencies:
- SQLAlchemy (>= 1.4, ideally 2.0+)
- Pydantic (>= 2.0)
- pydantic-core
"""

import logging
import uuid
from datetime import datetime
from typing import Any, ClassVar, Generic, Literal, Self, TypeVar, cast

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from pydantic.fields import FieldInfo
from pydantic_core import to_jsonable_python
from sqlalchemy import FromClause, func, inspect as sa_inspect
from sqlalchemy.orm import (
    ColumnProperty,
    DeclarativeBase,  # Import for type hint context if needed
    Mapped,
    MappedAsDataclass,
    Mapper,
    mapped_column,
    attributes,
)

logger = logging.getLogger(__name__)

# --- Generic Type Variables ---
# Used in BaseActiveRecord and BaseSelect to allow subclasses (sync/async)
# to specify their concrete Engine, Session, Query, and Result types.
EngineType = TypeVar("EngineType")
SessionType = TypeVar("SessionType")
QueryType = TypeVar("QueryType", bound="BaseSelect")  # Query type should inherit from BaseSelect
ResultType = TypeVar("ResultType")
TBaseModel = TypeVar("TBaseModel", bound="BaseActiveRecord")  # TypeVar for the model itself

# --- Base Select ---


class BaseSelect(Generic[SessionType, TBaseModel]):
    """
    Base class for Select functionality.

    Intended to be subclassed by concrete sync/async Select implementations.
    Provides common configuration and placeholders.
    """

    # SQLAlchemy Select caching behavior
    inherit_cache: ClassVar[bool] = True

    # Placeholders for attributes expected in subclasses
    # The session used for executing the query (if pre-associated)
    _session: SessionType | None = None
    # The ORM model class being queried
    _model_cls: type[TBaseModel]

    def __init__(self, entity: type[TBaseModel], session: SessionType | None = None) -> None:
        """
        Initialize the Select statement.

        Args:
            entity: The ORM model class (subclass of BaseActiveRecord) to query.
            session: An optional session to associate with this Select object.
        """
        if not issubclass(entity, BaseActiveRecord):
            # Ensure the entity is compatible
            raise TypeError(f"Entity must be a subclass of BaseActiveRecord, got {entity}")
        self._model_cls = entity
        self._session = session
        # Note: Actual Select statement construction (e.g., super().__init__(entity))
        # happens in the concrete subclasses (like the async Select previously shown).


# --- Base ActiveRecord ---


class BaseActiveRecord(Generic[EngineType, SessionType, QueryType, ResultType]):
    """
    Core base mixin for ActiveRecord models.

    Provides shared functionality for both synchronous and asynchronous
    ActiveRecord implementations, including serialization, loading,
    representation, and basic engine management hooks.

    Subclasses (concrete sync/async implementations) must define:
    - `__active_engine__`: The database engine instance.
    - `__session__`: A session instance or factory.
    - Methods for database interaction (add, save, delete, query execution, etc.).
    """

    # --- Class Configuration ---
    # To be defined by the final ORM model class
    __tablename__: ClassVar[str]
    __schema__: ClassVar[str | None] = None  # Optional schema

    # Populated by SQLAlchemy's declarative system
    __table__: ClassVar[FromClause]
    __mapper__: ClassVar[Mapper[Any]]

    # --- Engine/Session Management (Placeholders) ---
    # Concrete engine/session instances or factories must be provided by subclasses
    # or configured globally. Using class variables requires careful management,
    # especially for sessions in concurrent environments.
    __active_engine__: ClassVar[EngineType | None] = None
    # Consider if __session__ should be a factory or managed differently in subclasses
    __session__: ClassVar[SessionType | None] = None

    @classmethod
    def engine(cls) -> EngineType:
        """
        Return the configured active engine instance.

        Raises:
            ValueError: If no active engine has been set via `set_engine`.
        """
        if cls.__active_engine__ is None:
            raise ValueError(f"No active engine configured for {cls.__name__}. Call set_engine() first.")
        return cls.__active_engine__

    @classmethod
    def set_engine(cls, engine: EngineType) -> None:
        """
        Set the active engine instance for this class and its subclasses.
        Note: This sets the engine globally for all subclasses inheriting this specific base.
        Consider instance-based or scoped engine management for more complex apps.

        Args:
            engine: The engine instance (sync or async based on `EngineType`).
        """
        cls.__active_engine__ = engine

    # --- Instance Representation & Utilities ---

    def __str__(self) -> str:
        """Return a simple string representation, including primary key if available."""
        # Try common primary key names
        pk_val = getattr(self, "id", None)  # Check 'id' first
        if pk_val is None and hasattr(self, "pk_uuid"):
            pk_val = getattr(self, "pk_uuid", None)  # Check 'pk_uuid'

        pk_repr = f"({pk_val})" if pk_val is not None else "(unsaved)"
        # Use class name for better identification
        return f"<{self.__class__.__name__}{pk_repr}>"

    def __repr__(self) -> str:
        """Provide a developer-friendly representation showing attribute values."""
        insp = sa_inspect(self)
        if not insp:
            # Fallback if inspection fails (e.g., not fully initialized)
            return f"<{self.__class__.__name__}(inspection failed)>"

        attrs = []
        # Use instance_state for potentially more accurate representation
        state = attributes.instance_state(self)
        committed_state = state.committed_state

        for col in insp.mapper.column_attrs:
            key = col.key
            # Show loaded attributes from committed state if possible
            if key in committed_state:
                value = committed_state[key]
                attrs.append(f"{key}={value!r}")
            # Show pending changes if attribute is loaded
            elif key in state.dict:
                value = state.dict[key]
                attrs.append(f"{key}={value!r} (pending)")
            # Indicate deferred attributes without loading them
            elif key in insp.unloaded:
                attrs.append(f"{key}=<deferred>")
            # Fallback for other cases
            else:
                try:
                    value = getattr(self, key)
                    attrs.append(f"{key}={value!r}")
                except Exception:
                    attrs.append(f"{key}=<error loading>")

        return f"<{self.__class__.__name__}({', '.join(attrs)})>"

    def printn(self) -> None:
        """Print the mapped attributes and their current state (for debugging)."""
        print(f"--- Attributes for {self} ---")
        insp = sa_inspect(self)
        if not insp:
            print("Instance not inspectable (maybe transient or detached?).")
            # Avoid raw __dict__ as it includes internal state
            # print("Raw __dict__:", self.__dict__)
            return

        for attr in insp.attrs:
            key = attr.key
            try:
                value = attr.value
                state_info = attr.history.sum  # Provides info like added/deleted/unchanged
                print(f"{key}: {value!r} (State: {state_info})")
            except Exception as e:
                print(f"{key}: <Error loading: {e}>")
        print("-------------------------")

    def id_key(self) -> str:
        """
        Return a unique string key for this instance based on class name and primary key.

        Raises:
            AttributeError: If the instance lacks a common primary key attribute ('id' or 'pk_uuid').
            ValueError: If the primary key value is None (instance is transient).
        """
        pk_val = getattr(self, "id", None)
        if pk_val is None and hasattr(self, "pk_uuid"):
            pk_val = getattr(self, "pk_uuid", None)

        if pk_val is None:
            # Check if it's just transient or actually missing the attribute
            insp = sa_inspect(self)
            if insp and insp.transient:
                raise ValueError("Cannot generate id_key for a transient (unsaved) instance.")
            # If not transient and still no PK, then the attribute is likely missing
            raise AttributeError(f"{self.__class__.__name__} instance lacks a primary key value ('id' or 'pk_uuid').")

        return f"{self.__class__.__name__}:{pk_val}"

    @classmethod
    def __columns__fields__(cls) -> dict[str, tuple[type, Any]]:
        """
        Get a dictionary of mapped column names to their Python type and default value.

        Note: Requires the mapper to be configured (class instrumented by SQLAlchemy).

        Returns:
            A dictionary mapping column names to (python_type, default_value).

        Raises:
            ValueError: If the class is not a mapped class.
        """
        try:
            insp = sa_inspect(cls)
            if not insp or not hasattr(insp, "mapper"):
                raise ValueError(f"Class {cls.__name__} is not a mapped class.")

            dd = {}
            for col in insp.mapper.columns:
                # Ensure col is a Column object
                if not isinstance(col, sa.Column):
                    continue

                py_type = Any  # Default type
                if hasattr(col.type, "python_type"):
                    py_type = col.type.python_type

                default_val = None
                if col.default:
                    default_val = col.default.arg
                elif col.server_default:
                    # Represent server defaults symbolically
                    default_val = f"<Server Default: {col.server_default.arg}>"

                dd[col.name] = (py_type, default_val)
            return dd
        except Exception as e:
            logger.error(f"Error inspecting columns for {cls.__name__}: {e}", exc_info=True)
            # Raise a more specific error if inspection failed
            raise ValueError(f"Could not inspect columns for {cls.__name__}") from e

    # --- Serialization / Deserialization ---

    def to_dict(self, with_meta: bool = False, fields: set[str] | None = None) -> dict[str, Any]:
        """
        Generate a dictionary representation of the mapped columns of the instance.

        Args:
            with_meta: If True, include metadata about the model, table, and schema.
            fields: An optional set of field names to include. If None, all mapped columns are included.

        Returns:
            A dictionary containing the instance's data.

        Raises:
            ValueError: If the instance is not a mapped instance.
        """
        insp = sa_inspect(self)
        if not insp or not insp.mapper:
            # Check if it's a detached instance that was previously mapped
            if insp and insp.detached:
                logger.warning(f"Instance {self} is detached. to_dict may be incomplete.")
                # Proceed cautiously, might only have PKs
            else:
                raise ValueError(f"Instance {self} is not a mapped instance.")

        # Get column-based properties from the mapper
        col_prop_keys = {p.key for p in insp.mapper.iterate_properties if isinstance(p, ColumnProperty)}

        # Determine which keys to include
        keys_to_include = col_prop_keys
        if fields is not None:
            keys_to_include = col_prop_keys.intersection(fields)
            invalid_fields = fields - col_prop_keys
            if invalid_fields:
                logger.warning(
                    f"Requested fields not found or not columns in {self.__class__.__name__}: {invalid_fields}"
                )

        # Extract data using inspection state if possible to avoid loading deferred fields
        data = {}
        instance_state = attributes.instance_state(self)
        committed_state = instance_state.committed_state

        for key in keys_to_include:
            if key in committed_state:
                data[key] = committed_state[key]
            elif key in instance_state.dict:  # Check pending state
                data[key] = instance_state.dict[key]
            elif key not in insp.unloaded:  # If not unloaded, try getattr (might load)
                try:
                    data[key] = getattr(self, key)
                except Exception as e:
                    logger.warning(f"Could not retrieve attribute '{key}' for to_dict: {e}")
                    data[key] = None  # Placeholder on error
            else:
                # Attribute is deferred and not loaded
                data[key] = None  # Or another placeholder like "<deferred>"

        # Add metadata if requested
        if with_meta:
            classname = f"{self.__class__.__module__}.{self.__class__.__name__}"
            data["__metadata__"] = {
                "model": classname,
                "table": getattr(self, "__tablename__", "unknown"),
                "schema": getattr(self, "__schema__", None),
            }
        return data

    def dump_model(self, with_meta: bool = False, fields: set[str] | None = None) -> dict[str, Any]:
        """
        Return a JSON-serializable dict representation using pydantic-core.

        Converts types like UUID, datetime to JSON-compatible formats.

        Args:
            with_meta: If True, include metadata (passed to `to_dict`).
            fields: Optional set of field names to include (passed to `to_dict`).

        Returns:
            A dictionary suitable for JSON serialization.
        """
        raw_dict = self.to_dict(with_meta=with_meta, fields=fields)
        try:
            # Convert types (like UUID, datetime) to JSON-compatible formats
            return to_jsonable_python(raw_dict)
        except Exception as e:
            logger.error(f"Error making dictionary JSON serializable for {self}: {e}", exc_info=True)
            # Fallback or re-raise depending on desired behavior
            raise TypeError(f"Failed to serialize model {self} to JSON-compatible dict") from e

    @classmethod
    def load(cls: type[Self], data: dict[str, Any]) -> Self:
        """
        Create a new, transient instance from a dictionary of attributes.

        Note:
            - This creates a *transient* instance (not associated with a session).
              Use `add()` or `save()` in concrete subclasses to persist it.
            - It attempts to initialize the class using `__init__` with the provided
              data, which is generally safer than direct `__dict__` manipulation.
            - Assumes the class `__init__` can handle the provided keys or uses
              `MappedAsDataclass` which generates a suitable `__init__`.

        Args:
            data: A dictionary where keys are attribute names corresponding to
                  the model's `__init__` parameters or mapped attributes.

        Returns:
            A new, transient instance of the class.

        Raises:
            ValueError: If the class is not mapped or inspection fails.
            TypeError: If `data` contains keys not accepted by `__init__` or if
                       initialization fails.
            ValidationError: If using Pydantic models and validation fails.
        """
        insp = sa_inspect(cls)
        if not insp or not hasattr(insp, "mapper"):
            raise ValueError(f"Class {cls.__name__} is not a mapped class.")

        # Filter data to include only known column properties? Or rely on __init__?
        # Relying on __init__ is more flexible but requires a well-defined __init__.
        # MappedAsDataclass provides this.
        col_prop_keys = {p.key for p in insp.mapper.iterate_properties if isinstance(p, ColumnProperty)}
        filtered_data = {k: v for k, v in data.items() if k in col_prop_keys}
        unknown_keys = data.keys() - col_prop_keys
        if unknown_keys:
            logger.warning(f"Ignoring unknown keys during load of {cls.__name__}: {unknown_keys}")

        try:
            # Use filtered data to initialize the object
            # This assumes __init__ accepts these keys (true for MappedAsDataclass)
            obj = cls(**filtered_data)
            return obj
        # Catch potential errors during initialization (e.g., type errors)
        except (TypeError, ValidationError) as e:
            logger.error(f"Error initializing {cls.__name__} from data: {e}", exc_info=True)
            raise TypeError(f"Failed to load {cls.__name__} from dict. Data: {data}") from e
        except Exception as e:  # Catch other unexpected errors
            logger.error(f"Unexpected error loading {cls.__name__}: {e}", exc_info=True)
            raise e

    # --- Database Statement Builders (Example) ---

    @classmethod
    def get_insert(
        cls: type[TBaseModel],
        on_conflict: Literal["update", "nothing"] | None = None,
        index_elements: list[str] | None = None,
        set_: dict[str, Any] | None = None,
    ) -> sa_pg.Insert:
        """
        Create a PostgreSQL-specific INSERT statement with optional ON CONFLICT handling.

        Note: This is PostgreSQL specific due to `sqlalchemy.dialects.postgresql`.

        Args:
            on_conflict: Specify 'update' for ON CONFLICT DO UPDATE or
                         'nothing' for ON CONFLICT DO NOTHING.
            index_elements: List of column names for the conflict target.
                            Required for 'update', recommended for 'nothing'.
            set_: Dictionary of columns to update for ON CONFLICT DO UPDATE.
                  Keys are column names, values can be expressions (e.g., using
                  `insert_statement.excluded`). Required for 'update'.

        Returns:
            A `sqlalchemy.dialects.postgresql.Insert` object.

        Raises:
            ValueError: If required arguments for the chosen `on_conflict` strategy are missing.
            TypeError: If `on_conflict` is 'update' or 'nothing' but dialect is not PostgreSQL
                       (though this method inherently uses pg.insert).
        """
        # Base insert statement for the class
        ins = sa_pg.insert(cls)  # Explicitly uses PostgreSQL insert

        if on_conflict == "update":
            if not index_elements:
                raise ValueError("`index_elements` are required for ON CONFLICT DO UPDATE.")
            if set_ is None:  # Use empty dict if not provided? Or require? Require for clarity.
                raise ValueError("`set_` dictionary is required for ON CONFLICT DO UPDATE.")
            # Pass arguments explicitly to on_conflict_do_update
            ins = ins.on_conflict_do_update(index_elements=index_elements, set_=set_)
        elif on_conflict == "nothing":
            # Pass arguments explicitly to on_conflict_do_nothing
            # index_elements are optional but recommended for clarity/correctness
            ins = ins.on_conflict_do_nothing(index_elements=index_elements)

        return ins


# --- Base Mixins for Common Columns ---


class BasePKMixin(MappedAsDataclass):
    """
    Base mixin providing a UUID primary key column named 'id'.

    Inherits from `MappedAsDataclass` to ensure compatibility with dataclass-style models.
    Models using this mixin should typically inherit from `DeclarativeBase` and this mixin.
    """

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True,
        server_default=func.gen_random_uuid(),  # DB generates UUID if not provided
        default_factory=uuid.uuid4,  # Python generates UUID if not provided
        init=True,  # Allow providing in __init__
        kw_only=True,  # Must be provided as keyword argument if specified in __init__
    )


class BaseUpdateMixin(MappedAsDataclass):
    """
    Base mixin providing timestamp tracking columns `created_at` and `updated_at`.

    Inherits from `MappedAsDataclass`. Timestamps are typically managed by the database.
    Models using this mixin should typically inherit from `DeclarativeBase` and this mixin.
    """

    # `init=False` means these are not included in the generated __init__
    # `server_default` tells the DB to set the value on INSERT
    created_at: Mapped[datetime] = mapped_column(
        server_default=func.now(),
        init=False,
        repr=False,  # Often excluded from repr for brevity
    )
    # `onupdate` tells the DB to update the value on UPDATE
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now(), init=False, repr=False)


# --- Base Schema (Pydantic) ---

# Generic type variable for the ORM model class linked to the schema
OrmModelType = TypeVar("OrmModelType", bound=DeclarativeBase)  # Assume models inherit from DeclarativeBase


class BaseSchema(BaseModel, Generic[OrmModelType]):
    """
    Base Pydantic schema class for serialization, deserialization, and validation
    related to SQLAlchemy ORM models.

    Provides:
    - Configuration for creating schemas from ORM model attributes.
    - A helper method to convert a schema instance back to a (transient) ORM model.
    - A class method to dynamically add fields to the schema definition.
    """

    # Pydantic model configuration
    model_config: ClassVar[ConfigDict] = ConfigDict(
        from_attributes=True,  # Allow creating schema from ORM model instance attributes
        extra="ignore",  # Ignore extra fields in input data (safer than 'allow')
        # populate_by_name=True, # Useful if using aliases
    )

    def to_model(self, model_cls: type[OrmModelType]) -> OrmModelType:
        """
        Convert this Pydantic schema instance into a new, transient ORM model instance.

        Uses the `model_cls.load()` classmethod if available and compatible,
        otherwise falls back to initializing the model class directly with the
        schema's data (`model_cls(**self.model_dump())`).

        Args:
            model_cls: The ORM model class (e.g., MyModel inheriting BaseActiveRecord).

        Returns:
            A new, transient instance of `model_cls`.

        Raises:
            TypeError: If direct initialization fails (e.g., `__init__` mismatch).
            ValueError: If `model_cls.load()` exists but fails.
        """
        schema_data = self.model_dump(exclude_unset=True)  # Get data from schema

        # Prefer using the model's load method if it exists (potentially safer)
        if hasattr(model_cls, "load") and callable(getattr(model_cls, "load")):
            try:
                # Assuming model_cls.load accepts a dictionary
                logger.debug(f"Converting schema to {model_cls.__name__} using .load()")
                # Cast needed because 'load' isn't part of the generic OrmModelType definition
                load_method = cast(Any, getattr(model_cls, "load"))
                return load_method(schema_data)
            except Exception as e:
                logger.warning(
                    f"Failed to use {model_cls.__name__}.load() for schema conversion, falling back. Error: {e}"
                )
                # Fall through to direct initialization if load fails

        # Fallback: Direct initialization (requires __init__ to accept the data)
        try:
            logger.debug(f"Converting schema to {model_cls.__name__} using direct initialization")
            # This works well if model_cls uses MappedAsDataclass or has a compatible __init__
            return model_cls(**schema_data)
        except (TypeError, ValidationError) as e:
            logger.error(f"Failed to initialize {model_cls.__name__} from schema data: {e}", exc_info=True)
            raise TypeError(
                f"Could not convert schema to {model_cls.__name__}. "
                f"Ensure model's __init__ or load() method matches schema fields. Data: {schema_data}"
            ) from e

    # Source: Adapted from https://github.com/pydantic/pydantic/issues/1937#issuecomment-695313040
    # Note: Dynamically modifying models like this can have side effects and impact type checking. Use cautiously.
    @classmethod
    def add_fields(cls, **field_definitions: Any) -> type[Self]:
        """
        Dynamically add fields to this Pydantic schema class.

        This method modifies the class directly. Use with caution.

        Args:
            **field_definitions: Keyword arguments where the key is the new field
              name and the value is either:
                - A tuple of (type_annotation, default_value)
                - Just a default_value (type annotation will be inferred or Any)

        Returns:
            The modified class itself.

        Raises:
            ValueError: If a field definition tuple doesn't have exactly two elements.

        Example:
            `MySchema.add_fields(new_field=(str, 'default'), another_field=123)`
        """
        new_fields: dict[str, FieldInfo] = {}
        new_annotations: dict[str, Any] = {}

        for f_name, f_def in field_definitions.items():
            f_annotation: Any = Any  # Default annotation
            f_value: Any = ...  # Pydantic's marker for required field if no default

            if isinstance(f_def, tuple):
                # Case: (type_annotation, default_value)
                if len(f_def) != 2:
                    raise ValueError(
                        f"Field definition tuple must be (<type>, <default>). Got {f_def} for field '{f_name}'."
                    )
                f_annotation, f_value = f_def
            else:
                # Case: Just a default_value
                f_value = f_def
                # Keep f_annotation as Any, or try to infer? Pydantic infers if possible.

            # Store annotation and create FieldInfo
            new_annotations[f_name] = f_annotation
            # Use Field() to allow setting default_factory, constraints etc. if needed
            # For simple defaults, FieldInfo directly is okay too.
            new_fields[f_name] = Field(default=f_value)  # Let Pydantic handle default vs default_factory

        # Update the class's annotations and fields
        cls.__annotations__.update(new_annotations)
        # Use model_fields.update for Pydantic v2
        cls.model_fields.update(new_fields)

        # Rebuild the model to incorporate the new fields
        # Pass _defer_rebuild=False and _types_namespace=globals() ? Check Pydantic v2 docs.
        # force=True is generally needed for dynamic changes.
        cls.model_rebuild(force=True)
        logger.info(f"Dynamically added fields {list(field_definitions.keys())} to schema {cls.__name__}")
        return cls
