"""
Provides base components for managing SQLAlchemy engines, including fork handling.

This module contains:
- ForkEngines: A helper class to manage engine disposal after process forks.
- BaseActiveEngine: A generic base class for synchronous and asynchronous
  SQLAlchemy engine managers, handling configuration and argument preparation.
"""

import logging
import os
from collections.abc import Callable
from typing import Any, Generic, TypeVar

from sqlalchemy.pool import NullPool

from activealchemy.config import PostgreSQLConfigSchema

logger = logging.getLogger(__name__)


# --- Generic Type Variables ---
EngineType = TypeVar("EngineType")  # Represents sync or async Engine
SessionMakerType = TypeVar("SessionMakerType")  # Represents sync or async sessionmaker


class ForkEngines:
    """
    Helper to manage engine disposal after process forks.

    When a process forks (e.g., using `os.fork`), database connections managed
    by SQLAlchemy engines in the parent process should typically be discarded
    in the child process to avoid issues with shared state or stale connections.

    This class provides a callable instance that can be registered using
    `os.register_at_fork(after_in_child=...)`. When the child process starts
    after a fork, the registered callable (`__call__` method of this class)
    will execute the provided `dispose_callback` to clean up the engines
    inherited from the parent.

    Attributes:
        dispose_callback: The function to call in the child process after a fork
                          to dispose of engines (e.g., `engine_manager.dispose_engines`).
        is_registered: A flag indicating whether this instance has been successfully
                       registered with `os.register_at_fork`. This helps prevent
                       multiple registrations.
    """
    is_registered: bool = False

    def __init__(self, dispose_callback: Callable[[], Any]):
        """
        Initialize the fork handler.

        Args:
            dispose_callback: The function to execute in the child process
                              after a fork to dispose of inherited engines.
                              This function should handle both sync and async disposal
                              appropriately depending on the engine type.
        """
        if not callable(dispose_callback):
            raise TypeError("dispose_callback must be a callable function")
        self.dispose_callback = dispose_callback
        logger.debug("ForkEngines helper initialized.")

    def register(self) -> None:
        """
        Register the disposal callback to run in the child process after a fork.

        Uses `os.register_at_fork` if available. Logs a warning on platforms
        where fork is not supported or available. Skips registration if already
        registered.
        """
        if self.is_registered:
            logger.debug("Fork handler already registered.")
            return

        if hasattr(os, 'register_at_fork'):
            try:
                # Register self.__call__ to be executed in the child process
                # after the fork occurs.
                os.register_at_fork(after_in_child=self)
                self.is_registered = True
                logger.info("Registered engine disposal handler for post-fork cleanup in child process.")
            except Exception as e:
                # Catch potential errors during registration
                logger.error(f"Failed to register fork handler: {e}", exc_info=True)
        else:
            logger.warning("os.register_at_fork not available on this platform. Post-fork engine disposal disabled.")

    def __call__(self) -> None:
        """
        The method executed in the child process after a fork.

        Calls the `dispose_callback` provided during initialization to clean up
        engines inherited from the parent process. Resets the `is_registered`
        flag for the child process instance, as the child needs its own
        registration if it intends to fork further.
        """
        logger.info("Fork detected in child process. Executing engine disposal callback...")
        try:
            # Reset registration status for the *child's* instance of ForkEngines
            self.is_registered = False
            # Execute the actual engine disposal logic
            self.dispose_callback()
            logger.info("Engine disposal callback executed successfully in child process.")
        except Exception as e:
            logger.error(f"Error during post-fork engine disposal callback: {e}", exc_info=True)
            # Depending on severity, might want to raise or exit


class BaseActiveEngine(Generic[EngineType, SessionMakerType]):
    """
    Base class for managing SQLAlchemy engines (sync or async).
    Handles configuration processing and fork handling setup.
    """
    config: PostgreSQLConfigSchema
    engine_kwargs: dict[str, Any]
    sessions: dict[str, dict[str, SessionMakerType]]
    engines: dict[str, dict[str,  EngineType]]
    after_fork: ForkEngines

    def __init__(self, config: PostgreSQLConfigSchema, **kwargs: Any):
        if not isinstance(config, PostgreSQLConfigSchema):
            raise TypeError("config must be an instance of PostgreSQLConfigSchema")
        self.config = config
        logger.debug(f"Initializing BaseActiveEngine with config: {config}")
        self.engine_kwargs = self._prep_engine_arguments(kwargs)
        self.sessions = {}
        self.engines = {}
        self.after_fork = ForkEngines(self._dispose_engines_callback)

    def _dispose_engines_callback(self):
        """Called by ForkEngines, should be implemented by subclasses"""
        raise NotImplementedError

    def _prep_engine_arguments(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """
        Prepare the keyword arguments for SQLAlchemy engine creation.

        Merges default arguments derived from the `self.config` object with
        any explicitly provided `incoming_kwargs`. Handles specific adjustments
        for async mode and the asyncpg driver.

        Args:
            incoming_kwargs: Keyword arguments passed during engine initialization.

        Returns:
            A dictionary of processed arguments ready for `create_engine` or
            `create_async_engine`.
        """
        # Work on a copy to avoid modifying the original dictionary
        new_kwargs = kwargs.copy()

        # --- Merge Additional Config Kwargs ---
        if self.config.kwargs:
            logger.debug(f"Merging additional new_kwargs from config: {self.config.kwargs}")
            new_kwargs.update(self.config.kwargs)


        logger.debug(f"Preparing engine arguments from config and initial kwargs: {new_kwargs}")
        # No internal pool for Async drivers
        new_kwargs["poolclass"] = NullPool


        if "connect_args" not in new_kwargs:
            new_kwargs["connect_args"] = {"connect_timeout": self.config.connect_timeout}

        # --- Connection Arguments ---
        if "connect_args" not in new_kwargs:
            # Set default connect_timeout if not provided
            kwargs["connect_args"] = {"connect_timeout": self.config.connect_timeout}
            logger.debug(f"Setting default connect_args: {new_kwargs['connect_args']}")
        elif isinstance(new_kwargs["connect_args"], dict) and "connect_timeout" not in new_kwargs["connect_args"]:
            # Add connect_timeout if connect_args exists but lacks it
             new_kwargs["connect_args"]["connect_timeout"] = self.config.connect_timeout
             logger.debug(f"Adding default connect_timeout to existing connect_args: {new_kwargs['connect_args']}")

        # --- Echo SQL ---
        if "echo" not in kwargs:
            kwargs["echo"] = self.config.debug
            logger.debug(f"Setting echo={kwargs['echo']} based on config.debug")

        # --- Asyncpg Specific Adjustments ---
        if self.config.driver == "asyncpg":
            logger.debug("Applying asyncpg-specific argument adjustments.")
            # Handle sslmode -> ssl parameter for asyncpg connection string args
            # This assumes params are handled separately when building the DSN
            if "sslmode" in self.config.params and self.config.driver == "asyncpg":
                logger.debug("Adjusting 'sslmode' to 'ssl' in config params for asyncpg.")
                self.config.params["ssl"] = self.config.params.pop("sslmode")


            # Adjust connect_timeout -> timeout within connect_args for asyncpg driver
            if ("connect_args" in new_kwargs and isinstance(new_kwargs["connect_args"], dict)
                and "connect_timeout" in new_kwargs["connect_args"] and "timeout" not in new_kwargs["connect_args"]):
                    timeout = new_kwargs["connect_args"].pop("connect_timeout")
                    new_kwargs["connect_args"]["timeout"] = timeout
                    logger.debug("Adjusted 'connect_timeout' to 'timeout' in connect_args for asyncpg: %s",
                                 new_kwargs['connect_args'])


        logger.debug(f"Final prepared engine arguments: {kwargs}")
        return kwargs
