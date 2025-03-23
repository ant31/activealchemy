import logging
import uuid
from collections.abc import Sequence
from typing import Any, ClassVar, Literal, Self, TypeVar

import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sa_pg
from pydantic_core import to_jsonable_python
from sqlalchemy import FromClause, ScalarResult, func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncAttrs, AsyncSession, async_object_session, async_sessionmaker
from sqlalchemy.orm import (
    ColumnProperty,
    DeclarativeBase,
    Mapped,
    Mapper,
)

from activealchemy.aio.engine import ActiveEngine
from activealchemy.base.record import (
    BaseActiveRecord,
    BasePKMixin,
    BaseSchema,
    BaseSelect,
    BaseUpdateMixin,
)

logger = logging.getLogger(__name__)


class Select[T: "ActiveRecord"](BaseSelect[AsyncSession, T], sa.Select):
    """Async version of Select for queries"""

    inherit_cache: ClassVar[bool] = True

    def __init__(self, cls: T, session: AsyncSession | None = None, *args, **kwargs) -> None:
        self.session = session
        self.cls = cls
        super().__init__(cls, *args, **kwargs)

    async def scalars(self, session: AsyncSession | None = None) -> ScalarResult[T]:
        try:
            if not session:
                session = self.session
            session = await self.cls.new_session(session)
            result = await session.execute(self)
            return result.scalars()
        except SQLAlchemyError as e:
            raise e


class ActiveRecord(AsyncAttrs, BaseActiveRecord[ActiveEngine, AsyncSession, Select, ScalarResult]):
    """Async version of the ActiveRecord mixin"""

    __tablename__ = "orm_mixin"
    __schema__ = "public"
    __active_engine__: ClassVar[ActiveEngine]
    __session__: ClassVar[AsyncSession]
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
    async def dispose_engines(cls):
        """Dispose of all engines, they will be recreated when needed."""
        if cls.__active_engine__ is not None:
            await cls.engine().dispose_engines()

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
        if hasattr(self, "id"):
            return f"{self.__class__.__name__}:{self.id}"
        return f"{self.__class__.__name__}:unknown"

    @classmethod
    def __columns__fields__(cls) -> Any:
        dd = {}
        if cls.__table__ is None:
            raise ValueError("No table associated to this class")
        for col in list(cls.__table__.columns):
            dd[col.name] = (col.type.python_type, col.default.arg if col.default is not None else None)
        return dd

    @classmethod
    def session_factory(cls) -> async_sessionmaker:
        """Return the session associated to this class."""
        _, session = cls.engine().session()
        return session

    @classmethod
    async def new_session(cls, session: AsyncSession | None = None) -> AsyncSession:
        """Create a new session associated to this class."""
        if session is not None:
            return session
        _, session_factory = cls.engine().session()
        cls.__session__ = session_factory()
        return cls.__session__

    @classmethod
    def select(cls, *args, session: AsyncSession | None = None, **kwargs) -> Select[Self]:
        """Return the SQLAlchemy query object associated to this class."""
        return Select[Self](cls, *args, session=session, **kwargs)

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
    async def flush(cls, objs: list[Self], session) -> None:
        """Flush all changes to the database."""
        await session.flush(objs)

    @classmethod
    async def new_obj_session(cls, obj: Self, session: AsyncSession | None = None) -> tuple[AsyncSession, Self]:
        """Create a new session associated to this object."""
        if not session:
            session = obj.obj_session()
        session = await cls.new_session(session)
        if obj not in session:
            obj = await session.merge(obj)
        return session, obj

    @classmethod
    async def delete(cls, obj: Self, session: AsyncSession | None = None) -> None:
        """Delete the instance from the database."""
        session, obj = await cls.new_obj_session(obj, session)
        await session.delete(obj)

    @classmethod
    async def expire(cls, obj: Self, session: AsyncSession | None = None) -> Self:
        """Expire the instance, forcing a refresh when it is next accessed."""
        session, obj = await cls.new_obj_session(obj, session)
        session.expire(obj)
        return obj

    @classmethod
    async def refresh(cls, obj: Self, session: AsyncSession | None = None) -> Self:
        """Query the database to refresh the obj's attributes."""
        session, obj = await cls.new_obj_session(obj, session)
        await session.refresh(obj)
        return obj

    def obj_session(self) -> AsyncSession | None:
        """Get the session associated with this object."""
        return async_object_session(self)

    @classmethod
    async def expunge(cls, obj: Self, session: AsyncSession | None = None) -> Self:
        """Remove this instance from its session."""
        session, obj = await cls.new_obj_session(obj, session)
        session.expunge(obj)
        return obj

    @classmethod
    async def commit(cls, obj: Self | None = None, session: AsyncSession | None = None) -> None:
        """Commit the session associated to this class."""
        if session:
            await session.commit()
            return

        if obj is not None:
            session = obj.obj_session()
        if session is None:
            raise ValueError("No session associated to this object")
        await session.commit()

    @classmethod
    async def rollback(cls, obj: Self | None = None, session: AsyncSession | None = None) -> None:
        """Rollback the session associated to this class."""
        if session:
            await session.rollback()
            return
        if obj is not None:
            session = obj.obj_session()
        if session is None:
            raise ValueError("No session associated to this object")
        await session.rollback()

    async def is_modified(self, session: AsyncSession | None = None) -> bool:
        """Check if this object has been modified."""
        if session is None:
            session = self.obj_session()
        if session is None:
            raise ValueError("No session associated to this object")
        return session.is_modified(self)

    @classmethod
    async def add(cls, obj: Self, commit=False, session: AsyncSession | None = None) -> Self:
        """Add this instance to the database."""
        s = await cls.new_session(session)
        try:
            s.add(obj)
            if commit:
                await s.commit()
                await s.refresh(obj)
        except SQLAlchemyError as e:
            await s.rollback()
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
    async def add_all(
        cls,
        objs: list[Self],
        commit=False,
        skip_duplicate=True,
        fields: set[str] | None = None,
        session: AsyncSession | None = None,
    ) -> Sequence[Self]:
        """Add multiple instances to the database."""
        conflict = "nothing" if skip_duplicate else None
        values = [o.dump_model(with_meta=False, fields=fields) for o in objs]
        insert = cls.get_insert(on_conflict=conflict).values(values).returning(cls)
        s = await cls.new_session(session)
        result = await s.execute(insert)
        res = result.scalars().all()
        if commit:
            await s.commit()
        return res

    async def save(self, commit=False, session: AsyncSession | None = None) -> Self:
        """Add this instance to the database."""
        return await self.add(self, commit, session)

    # Query methods
    @classmethod
    async def find_by(cls, *args, session: AsyncSession | None = None, **kwargs) -> Self | None:
        """Returns an instance matching the criteria."""
        session = await cls.new_session(session)
        result = await session.execute(cls.select(session=session).where(*args, **kwargs).limit(1))
        return result.scalars().first()

    @classmethod
    async def get(cls, *args, session: AsyncSession | None = None, **kwargs) -> Self | None:
        """Returns an instance of this class by primary key."""
        session = await cls.new_session(session)
        return await session.get(cls, *args, **kwargs)

    @classmethod
    async def first(cls, col: Mapped | None = None, session: AsyncSession | None = None) -> Self | None:
        """Returns the first instance of this class."""
        if col is None:
            if cls.__table__ is not None:
                pk = cls.__table__.primary_key.columns.values()[0]
                col = getattr(cls, pk.name)
            else:
                raise ValueError("No table associated to this class")
        session = await cls.new_session(session)
        result = await session.execute(cls.select(session=session).order_by(col).limit(1))
        return result.scalars().first()

    @classmethod
    async def last(cls, col: Mapped | None = None, session: AsyncSession | None = None) -> Self | None:
        """Returns the last instance from the database."""
        if col is None:
            if cls.__table__ is not None:
                pk = cls.__table__.primary_key.columns.values()[0]
                col = getattr(cls, pk.name)
            else:
                raise ValueError("No table associated to this class")
        session = await cls.new_session(session)
        result = await session.execute(cls.select().order_by(col.desc()).limit(1))
        return result.scalars().first()

    @classmethod
    async def all(
        cls, query: Select[Self] | None = None, limit: int | None = None, session: AsyncSession | None = None
    ) -> Sequence[Self]:
        """Returns a list of all instances of this class in the database."""
        if query is None:
            query = cls.select(session=session)
        if limit is not None:
            query = query.limit(limit)

        session = await cls.new_session(session)
        result = await session.execute(query)
        return result.scalars().all()

    @classmethod
    def where(cls, *args, session: AsyncSession | None = None, **kwargs) -> Select[Self]:
        """Returns a query with the given criteria."""
        return cls.select(session=session).where(*args, **kwargs)

    @classmethod
    async def exec(cls, query: Select[Self], session: AsyncSession | None = None) -> ScalarResult[Self]:
        """Execute the given query."""
        session = await cls.new_session(session)
        result = await session.execute(query)
        return result.scalars()

    @classmethod
    async def count(cls, q: Select[Self] | None = None, session: AsyncSession | None = None) -> int:
        """Return the number of instances in the database."""
        if q is None:
            q = cls.select()
        q = q.offset(0)
        query = q.with_only_columns(func.count()).select_from(cls).order_by(None)  # pylint: disable=not-callable  # pylint: disable=not-callable

        s = await cls.new_session(session)
        result = await s.execute(query)
        res = result.scalars().first()
        if res is None:
            return 0
        return res


class PKMixin(BasePKMixin, ActiveRecord):
    """Primary key mixin for async records."""

    @classmethod
    async def find(cls, pk_uuid: uuid.UUID, session: AsyncSession | None = None) -> Self | None:
        """Return the instance with the given id."""
        return await cls.get(pk_uuid, session=session)


class UpdateMixin(BaseUpdateMixin, ActiveRecord):
    """Update tracking mixin for async records."""

    @classmethod
    async def last_modified(cls, session: AsyncSession | None = None) -> Self | None:
        """Returns the last modified instance from the database."""
        session = await cls.new_session(session)
        result = await session.execute(cls.select().order_by(cls.updated_at.desc()).limit(1))
        return result.scalars().first()

    @classmethod
    async def last_created(cls, session: AsyncSession | None = None) -> Self | None:
        """Returns the last created instance from the database."""
        session = await cls.new_session(session)
        result = await session.execute(cls.select().order_by(cls.created_at.desc()).limit(1))
        return result.scalars().first()

    @classmethod
    async def first_created(cls, session: AsyncSession | None = None) -> Self | None:
        """Returns the first created instance from the database."""
        session = await cls.new_session(session)
        result = await session.execute(cls.select().order_by(cls.created_at).limit(1))
        return result.scalars().first()

    @classmethod
    async def get_since(cls, date, query=None, session: AsyncSession | None = None) -> Sequence[Self]:
        """Returns a list of all instances modified since `date`."""
        if query is None:
            query = cls.select(session=session)
        if date is None:
            return await cls.all(query.order_by(cls.updated_at.desc()), session=session)

        return await cls.all(query.where(cls.updated_at > date).order_by(cls.updated_at.desc()), session=session)


class Base(ActiveRecord, DeclarativeBase): ...


T = TypeVar("T", bound=Base)


# Reuse the base schema class but with our specific Base class
class Schema[T](BaseSchema[T]): ...
