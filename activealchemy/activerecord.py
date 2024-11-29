import uuid
from datetime import datetime
from typing import Any, Self

import sqlalchemy as sa
from pydantic_core import to_jsonable_python
from sqlalchemy import func
from sqlalchemy.orm import (
    ColumnProperty,
    DeclarativeBase,
    Mapped,
    MappedAsDataclass,
    mapped_column,
    object_session,
)

from activealchemy.engine import ActiveEngine

# Base imported from orm-classes


class Base(DeclarativeBase):
    pass


class ActiveRecordMixin:
    """Models mixin class"""

    __tablename__ = "orm_mixin"
    __schema__ = "public"
    __active_engine__: ActiveEngine

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
        print("\n".join(f"{k}: {v}") for k, v in self.__dict__.items())

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
    def select(cls) -> sa.Select:
        """Return the SQLAlchemy query object associated to this class.

        It is equivalent to call session.query(MyClass).
        """
        return sa.select(cls)

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

    def flush(self) -> None:
        """Flush all changes to this object to the database."""

        self.obj_session().flush([self])

    def delete(self) -> None:
        """Mark the instance as deleted.

        The database delete operation will occurs upon next flush().
        This is equivalent to:
        session.delete(myObj)
        """
        self.session().delete(self)

    def expire(self) -> None:
        """Expire the instance, forcing a refresh when it is next accessed."""
        self.obj_session().expire([self])

    def refresh(self) -> None:
        """Query the database to refresh this instance's attributes."""
        self.obj_session().refresh(self)

    def obj_session(self) -> sa.orm.Session:
        session = object_session(self)
        if session is None:
            session = self.session()
            session.merge(self)
            session.refresh(self)
        return session

    def expunge(self) -> None:
        """Remove this instance fromn its session."""
        self.obj_session().expunge(self)

    def commit(self) -> None:
        """Commit the session associated to this object."""
        session = object_session(self)
        if session is not None:
            session.commit()

    def rollback(self) -> None:
        """Rollback the session associated to this object."""
        self.obj_session().rollback()

    def is_modified(self) -> bool:
        return self.obj_session().is_modified([self])

    def add(self, commit=False):
        """Add this instance to the database."""
        self.session().add(self)
        if commit:
            self.session().commit()
            self.refresh()
        return self

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
    def all(cls, query: sa.Select | None = None, limit: int | None = None) -> list[Self]:
        """Returns a list of all instances of this class in the database.

        This is the equivalent to:
        session.query(MyClass).all() inSQLAlchemy
        """
        if query is None:
            query = cls.select()
        if limit is not None:
            query = query.limit(limit)
        return cls.session().execute(query).scalars().all()


class PKMixin(MappedAsDataclass, ActiveRecordMixin):
    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, server_default=func.gen_random_uuid(), init=False)

    @classmethod
    def find(cls, id: uuid.UUID) -> Self | None:
        """Return the instance with the given id."""
        return cls.get(id)


class UpdateMixin(MappedAsDataclass, ActiveRecordMixin):
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
    def get_since(cls, date, query=None) -> list[Self]:
        """Returns a list of all instances modified since `date`."""
        if query is None:
            query = cls.select()

        if date is None:
            return cls.all(query.order_by(cls.updated_at.desc()))
        else:
            return cls.all(query.where(cls.updated_at > date).order_by(cls.updated_at.desc()))
