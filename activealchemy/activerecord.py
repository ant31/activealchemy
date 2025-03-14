import logging
import uuid
from collections.abc import Sequence
from datetime import datetime
from typing import Any, ClassVar, Literal, Self, TypeVar

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg
from pydantic import BaseModel, ConfigDict
from pydantic.fields import FieldInfo
from pydantic_core import to_jsonable_python
from sqlalchemy import FromClause, ScalarResult, func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import (
    ColumnProperty,
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    Mapper,
    mapped_column,
    object_session,
    sessionmaker,
)

from activealchemy.engine import ActiveEngine

logger = logging.getLogger(__name__)


# pylint: disable=too-many-ancestors
# pylint: disable=too-many-public-methods
# Base imported from orm-classes
class Select[T: "ActiveRecord"](sa.Select):
    inherit_cache: bool = True

    def __init__(self, cls: T, session=None, *args, **kwargs):
        self.session = session
        super().__init__(cls, *args, **kwargs)
        self.cls = cls

    def scalars(self, session: sa.orm.Session | None = None) -> ScalarResult[T]:
        try:
            if not session:
                session = self.session
            session = self.cls.new_session(session)
            return session.execute(self).scalars()
        except SQLAlchemyError as e:
            raise e


# pylint: disable=no-member
class ActiveRecord:
    """Models mixin class"""

    __tablename__ = "orm_mixin"
    __schema__ = "public"
    __active_engine__: ActiveEngine | None = None
    __session__: sessionmaker | None = None
    __table__: ClassVar[FromClause]
    __mapper__: ClassVar[Mapper[Any]]

    @classmethod
    def engine(cls) -> ActiveEngine:
        """Return the active engine."""
        if cls.__active_engine__ is None:
            raise ValueError("No active engine set")
        return cls.__active_engine__

    @classmethod
    def set_engine(cls, engine: ActiveEngine):
        cls.__active_engine__ = engine

    @classmethod
    def dispose_engines(cls):
        """Dispose of all engines, they will be recreated when needed."""
        if cls.__active_engine__ is not None:
            cls.engine().dispose_engines()

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
        return self.__class__.__name__ + ":" + str(self.id)

    @classmethod
    def __columns__fields__(cls) -> Any:
        dd = {}
        if cls.__table__ is None:
            raise ValueError("No table associated to this class")
        for col in list(cls.__table__.columns):
            dd[col.name] = (col.type.python_type, col.default.arg if col.default is not None else None)
        return dd

    @classmethod
    def session_factory(cls) -> sessionmaker:
        """Return the session associated to this class.

        This session is shared by all class using the same schema.
        """
        _, session = cls.engine().session()
        return session

    @classmethod
    def new_session(cls, session: sa.orm.Session | None = None) -> sa.orm.Session:
        """Create a new session associated to this class."""
        if session is not None:
            return session
        cls.__session__ = cls.session_factory()(expire_on_commit=True)
        return cls.__session__

    @classmethod
    def select(cls, session: sa.orm.Session | None = None,  *args, **kwargs) -> Select[Self]:
        """Return the SQLAlchemy query object associated to this class.

        It is equivalent to call session.query(MyClass).
        """
        return Select[Self](cls, session=session, *args, **kwargs)

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
    def flush(cls, objs: list[Self], session) -> None:
        """Flush all changes to the database."""
        session.flush(objs)

    def flush_me(self) -> None:
        """Flush all changes to this object to the database."""
        self.obj_session().flush([self])

    @classmethod
    def delete(cls, obj: Self, session: sa.orm.Session | None = None) -> None:
        """Delete the instance from the database.

        This is equivalent to:
        session.delete(myObj)
        """
        s = cls.new_session(session)
        s.delete(obj)

    def delete_me(self) -> None:
        """Mark the instance as deleted.

        The database delete operation will occurs upon next flush().
        This is equivalent to:
        session.delete(myObj)
        """
        self.delete(self)

    @classmethod
    def expire(cls, obj: Self) -> None:
        """Expire the instance, forcing a refresh when it is next accessed."""
        obj.obj_session().expire(obj)

    def expire_me(self) -> None:
        """Expire the instance, forcing a refresh when it is next accessed."""
        self.expire(self)

    @classmethod
    def refresh(cls, obj: Self) -> None:
        """Query the database to refresh the obj's attributes."""
        obj.obj_session().refresh(obj)

    def refresh_me(self) -> None:
        """Query the database to refresh this instance's attributes."""
        self.refresh(self)

    def obj_session(self, session: sa.orm.Session | None = None) -> sa.orm.Session:
        session = object_session(self)
        if session is None:
            session = self.new_session(session)
            session.merge(self)
            session.refresh(self)
        return session

    @classmethod
    def expunge(cls, obj: Self) -> None:
        """Remove this instance fromn its session."""
        session = obj.obj_session()
        session.expunge(obj)

    def expunge_me(self):
        """Remove this instance from its session."""
        self.expunge(self)

    @classmethod
    def commit(cls, obj: Self | None = None, session: sa.orm.Session | None = None) -> None:
        """Commit the session associated to this class."""
        if session:
            session.commit()
            return

        if obj is not None:
            session = obj.obj_session()
        if session is None:
            raise ValueError("No session associated to this object")
        session.commit()

    def commit_me(self) -> None:
        """Commit the session associated to this object."""
        self.commit(self)

    @classmethod
    def rollback(cls, obj: Self | None = None, session: sa.orm.Session | None = None) -> None:
        """Rollback the session associated to this class."""
        if session:
            session.rollback()
            return
        if obj is not None:
            session = obj.obj_session()
        if session is None:
            raise ValueError("No session associated to this object")
        session.rollback()

    def rollback_me(self) -> None:
        """Rollback the session associated to this object"""
        self.rollback(self)

    def is_modified(self) -> bool:
        return self.obj_session().is_modified([self])

    @classmethod
    def add(cls, obj: Self, commit=False, session: sa.orm.Session | None = None) -> Self:
        s = cls.new_session(session)
        try:
            s.add(obj)
            if commit:
                s.commit()
                s.refresh(obj)
        except SQLAlchemyError as e:
            s.rollback()
            raise e
        return obj

    @classmethod
    def get_insert(cls, *args, on_conflict: Literal["update", "nothing"] | None = None, **kwargs) -> sa_pg.Insert:
        ins = sa_pg.insert(cls)
        if on_conflict == "update":
            ins = ins.on_conflict_do_update(*args, **kwargs)
        elif on_conflict == "nothing":
            ins = ins.on_conflict_do_nothing(*args, **kwargs)
        return ins

    @classmethod
    def add_all(
            cls, objs: list[Self], commit=False, skip_duplicate=True, fields: set[str] | None = None, session: sa.orm.Session | None = None
    ) -> Sequence[Self]:
        conflict = "nothing" if skip_duplicate else None
        values = [o.dump_model(with_meta=False, fields=fields) for o in objs]
        insert = cls.get_insert(on_conflict=conflict).values(values).returning(cls)
        s = cls.new_session(session)
        res = s.scalars(insert, execution_options={"populate_existing": True}).all()
        if commit:
            s.commit()
        return res

    def save(self, commit=False, session: sa.orm.Session | None = None) -> Self:
        """Add this instance to the database."""
        return self.add(self, commit, session)

    def add_me(self, commit=False, session: sa.orm.Session | None = None) -> Self:
        """Add this instance to the database."""
        return self.add(self, commit, session)

    # query methods
    @classmethod
    def find_by(cls, *args, session: sa.orm.Session | None = None, **kwargs) -> Self | None:
        """Returns an instance matching the criteria.

        This is equivalent to:
        session.query(MyClass).filter_by(...).first()
        """
        session = self.new_session(session)
        return session.execute(cls.select(session=session).where(*args, **kwargs).limit(1)).scalars().first()

    @classmethod
    def get(cls, *args, session: sa.orm.Session | None = None, **kwargs) -> Self | None:
        """Returns an instance of this class.

        Returns the instance of this class based on the given identifier,
        or None if not found. This is equivalent to:
        session.query(MyClass).get(...)
        """
        session = cls.new_session(session)
        return session.get(cls, *args, **kwargs)

    @classmethod
    def first(cls, col: Mapped | None = None, session: sa.orm.Session | None = None) -> Self | None:
        """Returns the first instance of this class."""
        if col is None:
            col = cls.id
        session = cls.new_session(session)
        return session.execute(cls.select(session=session).order_by(col).limit(1)).scalars().first()

    @classmethod
    def last(cls, col: Mapped | None = None, session: sa.orm.Session | None = None) -> Self | None:
        """Returns the last instance from the database."""
        if col is None:
            col = cls.id
        session = cls.new_session(session)
        return session.execute(cls.select().order_by(col.desc()).limit(1)).scalars().first()

    @classmethod
    def all(
        cls, query: Select[Self] | None = None, limit: int | None = None, session: sa.orm.Session | None = None
    ) -> Sequence[Self]:
        """Returns a list of all instances of this class in the database.

        This is the equivalent to:
        session.query(MyClass).all() inSQLAlchemy
        """
        if query is None:
            query = cls.select(session=session)
        if limit is not None:
            query = query.limit(limit)

        session = cls.new_session(session)
        return session.execute(query).scalars().all()

    @classmethod
    def where(cls, *args, session: sa.orm.Session | None = None, **kwargs) -> Select[Self]:
        """Returns a query with the given criteria.

        This is equivalent to:
        session.select(MyClass).where(...)
        """
        return cls.select(session).where(*args, **kwargs)

    @classmethod
    def exec(cls, query: Select[Self], session: sa.orm.Session | None = None) -> ScalarResult[Self]:
        """Execute the given query."""
        session = cls.new_session(session)
        return session.session().execute(query).scalars()

    @classmethod
    def count(cls, q: Select[Self] | None = None, session: sa.orm.Session | None = None) -> int:
        """Return the number of instances in the database."""
        if q is None:
            q = cls.select()
        q = q.offset(0)
        query = q.with_only_columns(func.count()).select_from(cls).order_by(None)  # pylint: disable=not-callable

        s = cls.new_session(session)
        res = s.execute(query).scalars().first()
        if res is None:
            return 0
        return res


class PKMixin(MappedAsDataclass, ActiveRecord):
    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=func.gen_random_uuid(), default_factory=uuid.uuid4, kw_only=True
    )

    @classmethod
    def find(cls, pk_uuid: uuid.UUID, session: sa.orm.Session | None = None) -> Self | None:
        """Return the instance with the given id."""
        return cls.get(pk_uuid, session=session)


# pylint: disable=not-callable
class UpdateMixin(MappedAsDataclass, ActiveRecord):
    updated_at: Mapped[datetime] = mapped_column(server_default=(func.now)(), onupdate=(func.now)(), init=False)
    created_at: Mapped[datetime] = mapped_column(server_default=(func.now)(), init=False)

    @classmethod
    def last_modified(cls, session: sa.orm.Session | None = None) -> Self | None:
        """Returns the last modified instance from the database."""
        session = cls.new_session(session)
        return session.execute(cls.select().order_by(cls.updated_at.desc()).limit(1)).scalars().first()

    @classmethod
    def last_created(cls, session: sa.orm.Session | None = None) -> Self | None:
        """Returns the last modified instance from the database."""
        session = cls.new_session(session)
        return session.execute(cls.select().order_by(cls.created_at.desc()).limit(1)).scalars().first()

    @classmethod
    def first_created(cls, session: sa.orm.Session | None = None) -> Self | None:
        """Returns the last modified instance from the database."""
        session = cls.new_session(session)
        return session.execute(cls.select().order_by(cls.created_at).limit(1)).scalars().first()

    @classmethod
    def get_since(cls, date, query=None, session: sa.orm.Session | None = None) -> Sequence[Self]:
        """Returns a list of all instances modified since `date`."""
        if query is None:
            query = cls.select(session=session)
        if date is None:
            return cls.all(query.order_by(cls.updated_at.desc()), session=session)

        return cls.all(query.where(cls.updated_at > date).order_by(cls.updated_at.desc()), session=session)


class Base(ActiveRecord, DeclarativeBase): ...


T = TypeVar("T", bound=Base)


class BaseSchema[T](BaseModel):
    """document:"""

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
