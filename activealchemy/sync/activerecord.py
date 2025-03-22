import logging
import uuid
from collections.abc import Sequence
from typing import ClassVar, Self, TypeVar

import sqlalchemy as sa
from sqlalchemy import ScalarResult, func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    object_session,
    sessionmaker,
)

from activealchemy.base.record import (
    BaseActiveRecord,
    BasePKMixin,
    BaseSchema,
    BaseSelect,
    BaseUpdateMixin,
)
from activealchemy.sync.engine import ActiveEngine

logger = logging.getLogger(__name__)


class Select[T: "ActiveRecord"](BaseSelect[Session, ScalarResult[T]], sa.Select):
    """Sync version of Select for queries"""

    inherit_cache: bool = True

    def __init__(self, cls: T, session=None, *args, **kwargs):
        self.session = session
        self.cls = cls
        super().__init__(cls, *args, **kwargs)

    def scalars(self, session: Session | None = None) -> ScalarResult[T]:
        try:
            if not session:
                session = self.session
            session = self.cls.new_session(session)
            return session.execute(self).scalars()
        except SQLAlchemyError as e:
            raise e


class ActiveRecord(BaseActiveRecord[ActiveEngine, Session, Select, ScalarResult]):
    """Sync version of the ActiveRecord mixin"""

    __active_engine__: ClassVar[ActiveEngine] = None
    __session__: ClassVar[Session] = None

    @classmethod
    def session_factory(cls) -> sessionmaker:
        """Return the session associated to this class."""
        _, session = cls.engine().session()
        return session

    @classmethod
    def new_session(cls, session: Session | None = None) -> Session:
        """Create a new session associated to this class."""
        if session is not None:
            return session
        cls.__session__ = cls.session_factory()(expire_on_commit=True)
        return cls.__session__

    @classmethod
    def dispose_engines(cls):
        """Dispose of all engines, they will be recreated when needed."""
        if cls.__active_engine__ is not None:
            cls.engine().dispose_engines()

    @classmethod
    def select(cls, *args, session: Session | None = None, **kwargs) -> Select[Self]:
        """Return the SQLAlchemy query object associated to this class."""
        return Select[Self](cls, *args, session=session, **kwargs)

    @classmethod
    def flush(cls, objs: list[Self], session: Session) -> None:
        """Flush all changes to the database."""
        session.flush(objs)

    def flush_me(self) -> None:
        """Flush all changes to this object to the database."""
        session = self.obj_session()
        session.flush([self])

    @classmethod
    def delete(cls, obj: Self, session: Session | None = None) -> None:
        """Delete the instance from the database."""
        s = cls.new_session(session)
        s.delete(obj)

    def delete_me(self) -> None:
        """Mark the instance as deleted."""
        self.delete(self)

    @classmethod
    def expire(cls, obj: Self) -> None:
        """Expire the instance, forcing a refresh when it is next accessed."""
        session = obj.obj_session()
        session.expire(obj)

    def expire_me(self) -> None:
        """Expire the instance, forcing a refresh when it is next accessed."""
        self.expire(self)

    @classmethod
    def refresh(cls, obj: Self) -> None:
        """Query the database to refresh the obj's attributes."""
        session = obj.obj_session()
        session.refresh(obj)

    def refresh_me(self) -> None:
        """Query the database to refresh this instance's attributes."""
        self.refresh(self)

    def obj_session(self, session: Session | None = None) -> Session:
        """Get the session associated with this object."""
        session = object_session(self)
        if session is None:
            session = self.new_session(session)
            session.add(self)
            session.refresh(self)
        return session

    @classmethod
    def expunge(cls, obj: Self) -> None:
        """Remove this instance from its session."""
        session = obj.obj_session()
        session.expunge(obj)

    def expunge_me(self):
        """Remove this instance from its session."""
        self.expunge(self)

    @classmethod
    def commit(cls, obj: Self | None = None, session: Session | None = None) -> None:
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
    def rollback(cls, obj: Self | None = None, session: Session | None = None) -> None:
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
        """Check if this object has been modified."""
        session = self.obj_session()
        return session.is_modified(self)

    @classmethod
    def add(cls, obj: Self, commit=False, session: Session | None = None) -> Self:
        """Add this instance to the database."""
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
    def add_all(
        cls,
        objs: list[Self],
        commit=False,
        skip_duplicate=True,
        fields: set[str] | None = None,
        session: Session | None = None,
    ) -> Sequence[Self]:
        """Add multiple instances to the database."""
        conflict = "nothing" if skip_duplicate else None
        values = [o.dump_model(with_meta=False, fields=fields) for o in objs]
        insert = cls.get_insert(on_conflict=conflict).values(values).returning(cls)
        s = cls.new_session(session)
        res = s.scalars(insert, execution_options={"populate_existing": True}).all()
        if commit:
            s.commit()
        return res

    def save(self, commit=False, session: Session | None = None) -> Self:
        """Add this instance to the database."""
        return self.add(self, commit, session)

    def add_me(self, commit=False, session: Session | None = None) -> Self:
        """Add this instance to the database."""
        return self.add(self, commit, session)

    # Query methods
    @classmethod
    def find_by(cls, *args, session: Session | None = None, **kwargs) -> Self | None:
        """Returns an instance matching the criteria."""
        session = cls.new_session(session)
        return session.execute(cls.select(session=session).where(*args, **kwargs).limit(1)).scalars().first()

    @classmethod
    def get(cls, *args, session: Session | None = None, **kwargs) -> Self | None:
        """Returns an instance of this class by primary key."""
        session = cls.new_session(session)
        return session.get(cls, *args, **kwargs)

    @classmethod
    def first(cls, col: Mapped | None = None, session: Session | None = None) -> Self | None:
        """Returns the first instance of this class."""
        if col is None:
            if cls.__table__ is not None:
                pk = cls.__table__.primary_key.columns.values()[0]
                col = getattr(cls, pk.name)
            else:
                raise ValueError("No table associated to this class")
        session = cls.new_session(session)
        return session.execute(cls.select(session=session).order_by(col).limit(1)).scalars().first()

    @classmethod
    def last(cls, col: Mapped | None = None, session: Session | None = None) -> Self | None:
        """Returns the last instance from the database."""
        if col is None:
            if cls.__table__ is not None:
                pk = cls.__table__.primary_key.columns.values()[0]
                col = getattr(cls, pk.name)
            else:
                raise ValueError("No table associated to this class")
        session = cls.new_session(session)
        return session.execute(cls.select().order_by(col.desc()).limit(1)).scalars().first()

    @classmethod
    def all(
        cls, query: Select[Self] | None = None, limit: int | None = None, session: Session | None = None
    ) -> Sequence[Self]:
        """Returns a list of all instances of this class in the database."""
        if query is None:
            query = cls.select(session=session)
        if limit is not None:
            query = query.limit(limit)

        session = cls.new_session(session)
        return session.execute(query).scalars().all()

    @classmethod
    def where(cls, *args, session: Session | None = None, **kwargs) -> Select[Self]:
        """Returns a query with the given criteria."""
        return cls.select(session=session).where(*args, **kwargs)

    @classmethod
    def exec(cls, query: Select[Self], session: Session | None = None) -> ScalarResult[Self]:
        """Execute the given query."""
        session = cls.new_session(session)
        return session.execute(query).scalars()

    @classmethod
    def count(cls, q: Select[Self] | None = None, session: Session | None = None) -> int:
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


class PKMixin(BasePKMixin, ActiveRecord):
    """Primary key mixin for sync records."""

    @classmethod
    def find(cls, pk_uuid: uuid.UUID, session: Session | None = None) -> Self | None:
        """Return the instance with the given id."""
        return cls.get(pk_uuid, session=session)


class UpdateMixin(BaseUpdateMixin, ActiveRecord):
    """Update tracking mixin for sync records."""

    @classmethod
    def last_modified(cls, session: Session | None = None) -> Self | None:
        """Returns the last modified instance from the database."""
        session = cls.new_session(session)
        return session.execute(cls.select().order_by(cls.updated_at.desc()).limit(1)).scalars().first()

    @classmethod
    def last_created(cls, session: Session | None = None) -> Self | None:
        """Returns the last created instance from the database."""
        session = cls.new_session(session)
        return session.execute(cls.select().order_by(cls.created_at.desc()).limit(1)).scalars().first()

    @classmethod
    def first_created(cls, session: Session | None = None) -> Self | None:
        """Returns the first created instance from the database."""
        session = cls.new_session(session)
        return session.execute(cls.select().order_by(cls.created_at).limit(1)).scalars().first()

    @classmethod
    def get_since(cls, date, query=None, session: Session | None = None) -> Sequence[Self]:
        """Returns a list of all instances modified since `date`."""
        if query is None:
            query = cls.select(session=session)
        if date is None:
            return cls.all(query.order_by(cls.updated_at.desc()), session=session)

        return cls.all(query.where(cls.updated_at > date).order_by(cls.updated_at.desc()), session=session)


class Base(ActiveRecord, DeclarativeBase): ...


T = TypeVar("T", bound=Base)


# Reuse the base schema class but with our specific Base class
class Schema[T](BaseSchema[T]): ...
