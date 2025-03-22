from collections.abc import Callable
from typing import Any, Generic, TypeVar

from sqlalchemy.pool import NullPool

from activealchemy.config import PostgreSQLConfigSchema

E = TypeVar("E")  # Engine type
S = TypeVar("S")  # Session type


class ForkEngines:
    registered = False

    def __init__(self, callback: Callable[[], None]):
        self.callback = callback

    def __call__(self, callback):
        self.registered = False  # child must reregister
        self.callback()


class BaseActiveEngine(Generic[E, S]):
    """Base engine functionality shared between sync and async versions"""

    def __init__(self, config: PostgreSQLConfigSchema, **kwargs: Any):
        self.config = config
        self.engine_kwargs = self._prep_engine_arguments(kwargs)
        self.sessions: dict[str, dict[str, S]] = {}
        self.engines: dict[str, dict[str, E]] = {}
        self.after_fork = ForkEngines(self._dispose_engines_callback)

    def __del__(self):
        self._dispose_engines_callback()

    def _dispose_engines_callback(self):
        """Called by ForkEngines, should be implemented by subclasses"""
        raise NotImplementedError

    def _prep_engine_arguments(self, kwargs) -> dict[str, Any]:
        """Prepare the arguments for the engine."""
        if (self.config.use_internal_pool is False and "poolclass" not in kwargs) or self.config.mode == "async":
            kwargs["poolclass"] = NullPool

        if "connect_args" not in kwargs:
            kwargs["connect_args"] = {"connect_timeout": self.config.connect_timeout}
        if "pool_pre_ping" not in kwargs and self.config.mode == "sync":
            kwargs["pool_pre_ping"] = True
        if "echo" not in kwargs:
            kwargs["echo"] = self.config.debug
        if self.config.mode == "async" and self.config.async_driver == "asyncpg":
            if "sslmode" in self.config.params:
                self.config.params["ssl"] = self.config.params.pop("sslmode")
            if "connect_args" in kwargs and "connect_timeout" in kwargs["connect_args"]:
                kwargs["connect_args"]["timeout"] = kwargs["connect_args"].pop("connect_timeout")
        if self.config.kwargs:
            kwargs.update(self.config.kwargs)
        return kwargs
