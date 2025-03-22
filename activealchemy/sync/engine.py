from multiprocessing.util import register_after_fork

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import sessionmaker

from activealchemy.base.engine import BaseActiveEngine


class ActiveEngine(BaseActiveEngine[Engine, sessionmaker]):
    """Synchronous version of the ActiveEngine"""

    def _dispose_engines_callback(self):
        """Called by ForkEngines to dispose engines"""
        self.dispose_engines()

    def dispose_engines(self):
        """Dispose of engines for all schemas."""
        for _, v in self.engines.items():
            for engine in v.values():
                engine.dispose()
        self.engines.clear()
        self.sessions.clear()

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

    def session(self, schema: str | None = None) -> tuple[Engine, sessionmaker]:
        """Creates/returns a session connected to the schema"""
        if schema is None:
            schema = self.config.default_schema
        engine = self.engine(schema)
        if self.config.db not in self.sessions:
            self.sessions[self.config.db] = {}
        if schema not in self.sessions[self.config.db]:
            self.sessions[self.config.db][schema] = sessionmaker(bind=engine)
        return engine, self.sessions[self.config.db][schema]
