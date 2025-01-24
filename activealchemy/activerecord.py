import logging
import uuid
from collections.abc import Sequence
from datetime import datetime
from typing import Any, Literal, Self

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg
from pydantic_core import to_jsonable_python
from sqlalchemy import ScalarResult, func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import ColumnProperty, Mapped, MappedAsDataclass, mapped_column, object_session

from activealchemy.engine import ActiveEngine

logger = logging.getLogger(__name__)


# Base imported from orm-classes
class Select[T: "ActiveRecord"](sa.Select):
    inherit_cache: bool = True

    def __init__(self, cls: T, *args, **kwargs):
        super().__init__(cls, *args, **kwargs)
        self.cls = cls

    def scalars(self) -> ScalarResult[T]:
        try:
            return self.cls.session().execute(self).scalars()
        except SQLAlchemyError as e:
            self.cls.session().rollback()
            raise e


class ActiveRecord:
    """Models mixin class"""

    __tablename__ = "orm_mixin"
    __schema__ = "public"
    __active_engine__: ActiveEngine | None = None

    @classmethod
    def engine(cls):
        return cls.__active_engine__

    @classmethod
    def set_engine(cls, engine: ActiveEngine):
        cls.__active_engine__ = engine

    @classmethod
    def dispose_engines(cls):
        """Dispose of all engines, they will be recreated when needed."""
        cls.engine().dispose_engines()

    def __str__(self):
        return f"{self.__tablename__}({self.id})"

    def __repr__(self) -> str:
        return str(self)

    def printn(self):
        """Print the attributes of the class."""
        {print(f"{k}: {v}") for k, v in self.__dict__.items()}

    def id_key(self):
        return self.__class__.__name__ + ":" + str(self.id)

    @classmethod
    def session(cls):
        """Return the session associated to this class.

        This session is shared by all class using the same schema.
        """
        _, session = cls.__active_engine__.session()
        return session

    @classmethod
    def select(cls, *args, **kwargs) -> Select[Self]:
        """Return the SQLAlchemy query object associated to this class.

        It is equivalent to call session.query(MyClass).
        """
        return Select[Self](cls, *args, **kwargs)

    def dump_model(self, with_meta: bool = True) -> dict[str, Any]:
        """Return a dict representation of the instance."""
        return to_jsonable_python(self.to_dict(with_meta))

    def to_dict(self, with_meta: bool = True) -> dict[str, Any]:
        """Generate a JSON-style nested dict structure from an object."""
        if hasattr(self, "__mapper__"):
            col_prop_names = [p.key for p in self.__mapper__.iterate_properties if isinstance(p, ColumnProperty)]
            data = dict((name, getattr(self, name)) for name in col_prop_names)
        else:
            data = self.__dict__.copy()
        if with_meta:
            classname = ":".join([self.__class__.__module__, self.__class__.__name__])
            data.update({"__metadata__": {"model": classname, "table": self.__tablename__, "schema": self.__schema__}})
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
    def flush(cls, objs: list[Self]) -> None:
        """Flush all changes to the database."""
        cls.session().flush(objs)

    def flush_me(self) -> None:
        """Flush all changes to this object to the database."""
        self.obj_session().flush([self])

    @classmethod
    def delete(cls, obj: Self) -> None:
        """Delete the instance from the database.

        This is equivalent to:
        session.delete(myObj)
        """
        cls.session().delete(obj)

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

    def obj_session(self) -> sa.orm.Session:
        session = object_session(self)
        if session is None:
            session = self.session()
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
    def commit(cls, obj: Self | None = None) -> None:
        """Commit the session associated to this class."""
        if obj is not None:
            session = obj.obj_session()
        else:
            session = cls.session()

        session.commit()

    def commit_me(self) -> None:
        """Commit the session associated to this object."""
        self.commit(self)

    @classmethod
    def rollback(cls, obj: Self | None = None) -> None:
        """Rollback the session associated to this class."""
        if obj is not None:
            session = obj.obj_session()
        else:
            session = cls.session()
        session.rollback()

    def rollback_me(self) -> None:
        """Rollback the session associated to this object"""
        self.rollback(self)

    def is_modified(self) -> bool:
        return self.obj_session().is_modified([self])

    @classmethod
    def add(cls, obj: Self, commit=False) -> Self:
        try:
            cls.session().add(obj)
            if commit:
                cls.session().commit()
                cls.session().refresh(obj)
        except SQLAlchemyError as e:
            cls.session().rollback()
            raise e
        return obj

    @classmethod
    def get_insert(cls, on_conflict: Literal["update", "nothing"] | None = None, *args, **kwargs) -> sa_pg.Insert:
        ins = sa_pg.insert(cls)
        if on_conflict == "update":
            ins = ins.on_conflict_do_update(*args, **kwargs)
        elif on_conflict == "nothing":
            ins = ins.on_conflict_do_nothing(*args, **kwargs)
        return ins

    @classmethod
    def add_all(cls, objs: list[Self], commit=False, skip_duplicate=True) -> Sequence[Self]:
        conflict = "nothing" if skip_duplicate else None
        insert = cls.get_insert(on_conflict=conflict)
        values = [o.dump_model(with_meta=False) for o in objs]
        insert.values(values)
        insert.returning(cls)
        try:
            res = cls.session().scalars(insert, execution_options={"populate_existing": True})
        except SQLAlchemyError as e:
            cls.session().rollback()
            raise e
        return res.all()

    def save(self, commit=False):
        """Add this instance to the database."""
        return self.add(self, commit)

    def add_me(self, commit=False):
        """Add this instance to the database."""
        return self.add(self, commit)

    # query methods
    @classmethod
    def find_by(cls, *args, **kwargs) -> Self | None:
        """Returns an instance matching the criteria.

        This is equivalent to:
        session.query(MyClass).filter_by(...).first()
        """
        return cls.session().execute(cls.select().where(*args, **kwargs).limit(1)).scalars().first()

    @classmethod
    def get(cls, *args, **kwargs) -> Self | None:
        """Returns an instance of this class.

        Returns the instance of this class based on the given identifier,
        or None if not found. This is equivalent to:
        session.query(MyClass).get(...)
        """
        return cls.session().get(cls, *args, **kwargs)

    @classmethod
    def first(cls) -> Self | None:
        """Returns the first instance of this class."""
        return cls.session().execute(cls.select().order_by(cls.id).limit(1)).scalars().first()

    @classmethod
    def last(cls) -> Self | None:
        """Returns the last instance from the database."""
        return cls.session().execute(cls.select().order_by(cls.id.desc()).limit(1)).scalars().first()

    @classmethod
    def all(cls, query: Select[Self] | None = None, limit: int | None = None) -> Sequence[Self]:
        """Returns a list of all instances of this class in the database.

        This is the equivalent to:
        session.query(MyClass).all() inSQLAlchemy
        """
        if query is None:
            query = cls.select()
        if limit is not None:
            query = query.limit(limit)
        return cls.session().execute(query).scalars().all()

    @classmethod
    def where(cls, *args, **kwargs) -> Select[Self]:
        """Returns a query with the given criteria.

        This is equivalent to:
        session.select(MyClass).where(...)
        """
        return cls.select().where(*args, **kwargs)

    @classmethod
    def e(cls, query: Select[Self]) -> ScalarResult[Self]:
        """Execute the given query."""
        return cls.session().execute(query).scalars()

    @classmethod
    def count(cls, q: Select[Self] | None = None) -> int:
        """Return the number of instances in the database."""
        if q is None:
            q = cls.select()
        q = q.offset(0)
        query = q.with_only_columns(func.count()).select_from(cls).order_by(None)  # pylint: disable=not-callable
        session = cls.session()
        res = session.execute(query).scalars().first()
        if res is None:
            return 0
        return res


class PKMixin(MappedAsDataclass, ActiveRecord):
    id: Mapped[uuid.UUID] = mapped_column(
        primary_key=True, server_default=func.gen_random_uuid(), default_factory=uuid.uuid4, kw_only=True
    )

    @classmethod
    def find(cls, id: uuid.UUID) -> Self | None:
        """Return the instance with the given id."""
        return cls.get(id)


class UpdateMixin(MappedAsDataclass, ActiveRecord):
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now(), init=False)
    created_at: Mapped[datetime] = mapped_column(server_default=func.now(), init=False)

    @classmethod
    def last_modified(cls) -> Self | None:
        """Returns the last modified instance from the database."""
        return cls.session().execute(cls.select().order_by(cls.updated_at.desc()).limit(1)).scalars().first()

    @classmethod
    def last_created(cls) -> Self | None:
        """Returns the last modified instance from the database."""
        return cls.session().execute(cls.select().order_by(cls.created_at.desc()).limit(1)).scalars().first()

    @classmethod
    def first_created(cls) -> Self | None:
        """Returns the last modified instance from the database."""
        return cls.session().execute(cls.select().order_by(cls.created_at).limit(1)).scalars().first()

    @classmethod
    def get_since(cls, date, query=None) -> Sequence[Self]:
        """Returns a list of all instances modified since `date`."""
        if query is None:
            query = cls.select()
        if date is None:
            return cls.all(query.order_by(cls.updated_at.desc()))
        else:
            return cls.all(query.where(cls.updated_at > date).order_by(cls.updated_at.desc()))
