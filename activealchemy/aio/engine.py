from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from activealchemy.base.engine import BaseActiveEngine


class ActiveEngine(BaseActiveEngine[AsyncEngine, async_sessionmaker]):
    """Async version of the ActiveEngine"""

    async def dispose_engines(self):
        """Async method to dispose of engines for all schemas."""
        for _, v in self.engines.items():
            for engine in v.values():
                await engine.dispose()
        self.engines.clear()
        self.sessions.clear()

    def __del__(self): ...

    def engine(self, schema: str | None = None) -> AsyncEngine:
        """Get the engine for the given schema, recreating one if needed."""
        name = self.config.db
        if schema is None:
            schema = self.config.default_schema
        try:
            return self.engines[name][schema]
        except KeyError:
            if name not in self.engines:
                self.engines[name] = {}
            # Convert the URI to async format
            async_uri = self.config.async_uri()
            print(async_uri, self.engine_kwargs)
            engine = create_async_engine(async_uri, **self.engine_kwargs)
            self.engines[name][self.config.default_schema] = engine
            return engine

    def session(self, schema: str | None = None) -> tuple[AsyncEngine, async_sessionmaker]:
        """Creates/returns an async session connected to the schema"""
        if schema is None:
            schema = self.config.default_schema
        engine = self.engine(schema)
        if self.config.db not in self.sessions:
            self.sessions[self.config.db] = {}
        if schema not in self.sessions[self.config.db]:
            self.sessions[self.config.db][schema] = async_sessionmaker(bind=engine, expire_on_commit=False)
        return engine, self.sessions[self.config.db][schema]
