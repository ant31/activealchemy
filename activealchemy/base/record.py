import logging
import uuid
from datetime import datetime
from typing import Any, ClassVar, Generic, Literal, Self, TypeVar

import sqlalchemy.dialects.postgresql as sa_pg
from pydantic import BaseModel, ConfigDict
from pydantic.fields import FieldInfo
from pydantic_core import to_jsonable_python
from sqlalchemy import FromClause, func
from sqlalchemy.orm import (
    ColumnProperty,
    Mapped,
    MappedAsDataclass,
    Mapper,
    mapped_column,
)

logger = logging.getLogger(__name__)

E = TypeVar("E")  # Engine type
S = TypeVar("S")  # Session type
Q = TypeVar("Q")  # Query/Select type
R = TypeVar("R")  # Result type


class BaseSelect(Generic[S, R]):
    """Base class for Select functionality"""

    inherit_cache: ClassVar[bool] = True
    session: S | None
    cls: Any = None


class BaseActiveRecord(Generic[E, S, Q, R]):
    """Base models mixin class with shared functionality between sync and async versions"""

    __tablename__ = "orm_mixin"
    __schema__ = "public"
    __table__: ClassVar[FromClause]
    __mapper__: ClassVar[Mapper[Any]]

    # These need to be implemented by subclasses
    __active_engine__: ClassVar[E] = None
    __session__: ClassVar[S] = None

    @classmethod
    def engine(cls) -> E:
        """Return the active engine."""
        if cls.__active_engine__ is None:
            raise ValueError("No active engine set")
        return cls.__active_engine__

    @classmethod
    def set_engine(cls, engine: E):
        cls.__active_engine__ = engine

    def __str__(self):
        if hasattr(self, "id"):
            return f"{self.__tablename__}({self.id})"
        return f"{self.__tablename__}(id?)"

    def __repr__(self) -> str:
        return str(self)

    def printn(self):
        """Print the attributes of the class."""
        for k, v in self.__dict__.items():
            print(f"{k}: {v}")

    def id_key(self):
        """Return a unique key for this instance."""
        if hasattr(self, "id"):
            return f"{self.__class__.__name__}:{self.id}"
        raise AttributeError("Class has no 'id' attribute")

    @classmethod
    def __columns__fields__(cls) -> Any:
        dd = {}
        if cls.__table__ is None:
            raise ValueError("No table associated to this class")
        for col in list(cls.__table__.columns):
            dd[col.name] = (col.type.python_type, col.default.arg if col.default is not None else None)
        return dd

    def dump_model(self, with_meta: bool = True, fields: set[str] | None = None) -> dict[str, Any]:
        """Return a dict representation of the instance."""
        return to_jsonable_python(self.to_dict(with_meta, fields))

    def to_dict(self, with_meta: bool = True, fields: set[str] | None = None) -> dict[str, Any]:
        """Generate a JSON-style nested dict structure from an object."""
        if hasattr(self, "__mapper__"):
            col_prop_names = [p.key for p in self.__mapper__.iterate_properties if isinstance(p, ColumnProperty)]
            data = dict((name, getattr(self, name)) for name in col_prop_names)
        else:
            data = self.__dict__.copy()
        if with_meta:
            classname = ":".join([self.__class__.__module__, self.__class__.__name__])
            data.update({"__metadata__": {"model": classname, "table": self.__tablename__, "schema": self.__schema__}})
        if fields is not None:
            data = {k: v for k, v in data.items() if k in fields}
        return data

    @classmethod
    def load(cls, *args, **kwargs) -> Self:
        """Load an instance from a dict."""
        obj = cls()
        if hasattr(cls, "__mapper__"):
            col_prop_names = [p.key for p in cls.__mapper__.iterate_properties if isinstance(p, ColumnProperty)]
            data = dict((name, getattr(cls, name)) for name in col_prop_names)
        else:
            raise ValueError("Cannot load a model without a mapper")
        if len(args) > 0 and isinstance(args[0], dict):
            kwargs.update(args[0])
        for key, value in kwargs.items():
            if key in data:
                setattr(obj, key, value)
        return obj

    @classmethod
    def get_insert(cls, *args, on_conflict: Literal["update", "nothing"] | None = None, **kwargs) -> sa_pg.Insert:
        """Create an insert statement"""
        ins = sa_pg.insert(cls)
        if on_conflict == "update":
            ins = ins.on_conflict_do_update(*args, **kwargs)
        elif on_conflict == "nothing":
            ins = ins.on_conflict_do_nothing(*args, **kwargs)
        return ins


class BasePKMixin(MappedAsDataclass):
    """Base primary key mixin"""

    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=func.gen_random_uuid(), default_factory=uuid.uuid4, kw_only=True
    )


class BaseUpdateMixin(MappedAsDataclass):
    """Base update tracking mixin"""

    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now(), init=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), init=False)


class BaseSchema[T](BaseModel):
    """Base schema class for serialization/deserialization"""

    model_config: ClassVar[ConfigDict] = ConfigDict(from_attributes=True, extra="allow")

    def to_model(self, modelcls: type[T]) -> T:
        inst = modelcls()
        inst.__dict__.update(self.model_dump())
        return inst

    # Source: https://github.com/pydantic/pydantic/issues/1937#issuecomment-695313040
    @classmethod
    def add_fields(cls, **field_definitions: Any):
        new_fields: dict[str, FieldInfo] = {}

        for f_name, f_def in field_definitions.items():
            if isinstance(f_def, tuple):
                try:
                    f_annotation, f_value = f_def
                except ValueError as e:
                    raise ValueError(
                        "field definitions should either be a tuple of (<type>, <default>) or just a "
                        "default value, unfortunately this means tuples as "
                        "default values are not allowed"
                    ) from e
            else:
                f_annotation, f_value = None, f_def

            new_fields[f_name] = FieldInfo(annotation=f_annotation | None, default=f_value)

        cls.model_fields.update(new_fields)
        cls.model_rebuild(force=True)
