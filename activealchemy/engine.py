from collections.abc import Callable
from multiprocessing.util import register_after_fork
from typing import Any

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool

from activealchemy.config import PostgreSQLConfigSchema


class ForkEngines:
    registered = False

    def __init__(self, callback: Callable[[], None]):
        self.callback = callback

    def __call__(self, callback):
        self.registered = False  # child must reregister
        self.callback()


class ActiveEngine:
    def __init__(self, config: PostgreSQLConfigSchema, **kwargs: Any):
        self.config = config
        self.engine_kwargs = self._prep_engine_arguments(kwargs)
        self.sessions: dict[str, dict[str, scoped_session]] = {}

        self.engines: dict[str, dict[str, Engine]] = {}
        self.after_fork = ForkEngines(self.dispose_engines)

    def __del__(self):
        self.dispose_engines()

    def dispose_engines(self):
        """Dispose of engines for all schemas."""
        for _, v in self.engines.items():
            for engine in v.values():
                engine.dispose()
        self.engines.clear()
        self.sessions.clear()

    def _prep_engine_arguments(self, kwargs) -> dict[str, Any]:
        """Prepare the arguments for the engine."""
        if self.config.use_internal_pool is False and "poolclass" not in kwargs:
            kwargs["poolclass"] = NullPool
        if "connect_args" not in kwargs:
            kwargs["connect_args"] = {"connect_timeout": self.config.connect_timeout}
        if "pool_pre_ping" not in kwargs:
            kwargs["pool_pre_ping"] = True
        if "echo" not in kwargs:
            kwargs["echo"] = self.config.debug

        return kwargs

    def engine(self, schema: str | None = None) -> Engine:
        """Get the engine for the given schema, recreating one if needed."""
        name = self.config.db
        if schema is None:
            schema = self.config.default_schema
        try:
            return self.engines[name][schema]
        except KeyError:
            if name not in self.engines:
                self.engines[name] = {}
            engine = create_engine(self.config.uri(), **self.engine_kwargs)
            self.engines[name][self.config.default_schema] = engine
            self.after_fork.registered = True
            register_after_fork(self.after_fork, self.after_fork)
            return engine

    def session(self, schema: str | None = None) -> tuple[Engine, scoped_session]:
        """Creates/returns a session connected to the schema"""
        if schema is None:
            schema = self.config.default_schema
        engine = self.engine(schema)
        if self.config.db not in self.sessions:
            self.sessions[self.config.db] = {}
        if schema not in self.sessions[self.config.db]:
            self.sessions[self.config.db][schema] = scoped_session(sessionmaker(bind=engine))
        return engine, self.sessions[self.config.db][schema]
