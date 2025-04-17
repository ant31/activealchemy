# pylint: disable=no-self-argument
# pylint: disable=no-self-argument, E0213 # E0213 complains about abstract methods not being @classmethod
import logging
from abc import ABC, abstractmethod
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_core import PydanticCustomError

logger: logging.Logger = logging.getLogger("activealchemy")


# --- Base Configuration ---

class BaseDBConfig(BaseModel, ABC):
    """
    Abstract base class for database configurations.

    Defines common fields and requires subclasses to implement
    dialect-specific methods like `uri`.
    """
    model_config = ConfigDict(extra="allow")

    # Common fields applicable to most databases
    db: str = Field(description="Database name or identifier (e.g., database name for PG, file path for SQLite)")
    driver: str = Field(description="SQLAlchemy async driver name (e.g., 'asyncpg', 'aiosqlite')")
    debug: bool = Field(default=False, description="Enable debug logging (e.g., SQL echo)")
    default_schema: str | None = Field(default=None, description="Default schema to use (if applicable)")
    create_engine_kwargs: dict[str, Any] = Field(
        default_factory=dict, description="Extra kwargs for create_async_engine"
    )
    # Removed confusing 'kwargs' alias field and validator

    @abstractmethod
    def uri(self) -> str:
        """Build the SQLAlchemy DSN/URI for the specific dialect."""
        raise NotImplementedError

    @property
    def dialect_driver(self) -> str:
        """Return the dialect+driver string (e.g., 'postgresql+asyncpg')."""
        # Basic implementation, subclasses might need to override if dialect != driver prefix
        dialect = self.__class__.__name__.lower().replace("configschema", "")
        return f"{dialect}+{self.driver}"


# --- PostgreSQL Configuration ---

class PostgreSQLConfigSchema(BaseDBConfig):
    """
    Placeholder for configuration schema.

    Expected Attributes:
        db (str): Database name identifier.
        default_schema (str): Default PostgreSQL schema.
        use_internal_pool (bool): Whether to use SQLAlchemy's pool.
        mode (Literal["sync", "async"]): Operation mode.
        connect_timeout (int): Connection timeout in seconds.
        debug (bool): Enable debug logging (e.g., SQL echo).
        async_driver (str): Async driver (e.g., 'asyncpg').
        params (dict): Dictionary of extra connection parameters.
        kwargs (dict): Extra kwargs for engine creation.

    Expected Methods:
        uri() -> str: Returns the asynchronous DSN.
    """
    # --- Required Fields ---
    db: str = Field(default="activealchemy-dev", description="PostgreSQL database name")
    user: str = Field(default="activealchemy", description="Database user")
    password: str = Field(default="activealchemy", description="Database password")
    host: str = Field(default="localhost", description="Database host")
    port: int = Field(default=5432, description="Database port")
    driver: str = Field(default="asyncpg", description="Async driver for PostgreSQL (e.g., 'asyncpg')")

    # --- Optional Fields with Defaults ---
    params: dict[str, str] = Field(
        default_factory=lambda: {"sslmode": "disable"}, description="Additional connection parameters (e.g., sslmode)"
    )
    connect_timeout: int = Field(default=10, description="Connection timeout in seconds (used in connect_args)")
    default_schema: str = Field(default="public", description="Default PostgreSQL schema") # Overrides BaseDBConfig

    @field_validator('driver')
    @classmethod
    def check_driver(cls, v: str):
        """Validate supported PostgreSQL drivers."""
        supported = ["asyncpg"] # Add others like psycopg if needed later
        if v not in supported:
            raise PydanticCustomError(
                "value_error",
                f"Unsupported PostgreSQL driver '{v}'. Supported drivers: {supported}",
                {"driver": v, "supported": supported},
            )
        return v

    def uri(self) -> str:
        """Build the PostgreSQL DSN."""
        # Handle asyncpg's ssl parameter difference if needed (often handled by connect_args now)
        # params = self.params.copy()
        # if "sslmode" in params and self.driver == "asyncpg":
        #     logger.debug("Adjusting 'sslmode' to 'ssl' in config params for asyncpg.")
        #     params["ssl"] = params.pop("sslmode")

        dsn = f"postgresql+{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        if self.params:
            query_params = "&".join([f"{k}={v}" for k, v in self.params.items()])
            dsn = f"{dsn}?{query_params}"
        logger.debug(f"Built PostgreSQL DSN: {dsn}")
        return dsn

    @property
    def dialect_driver(self) -> str:
        """Return the dialect+driver string."""
        return f"postgresql+{self.driver}"


# --- SQLite Configuration ---

class SQLiteConfigSchema(BaseDBConfig):
    """
    Configuration schema for SQLite databases.
    """
    # --- Required Fields ---
    # For SQLite, 'db' field from BaseDBConfig represents the file path.
    db: str = Field(description="Path to the SQLite database file (e.g., './my_app.db', ':memory:')")
    driver: str = Field(default="aiosqlite", description="Async driver for SQLite (usually 'aiosqlite')")

    # --- Optional Fields with Defaults ---
    # SQLite doesn't use user/password/host/port in the same way
    # default_schema is not applicable to SQLite in the standard sense
    default_schema: str | None = Field(default=None, description="Schema (not typically used in SQLite)")
    connect_timeout: int = Field(default=5, description="Connection timeout (specific to driver behavior)")

    @field_validator('driver')
    @classmethod
    def check_driver(cls, v: str):
        """Validate supported SQLite drivers."""
        supported = ["aiosqlite"]
        if v not in supported:
             raise PydanticCustomError(
                "value_error",
                f"Unsupported SQLite driver '{v}'. Supported drivers: {supported}",
                {"driver": v, "supported": supported},
            )
        return v

    def uri(self) -> str:
        """Build the SQLite DSN."""
        # Example: sqlite+aiosqlite:///path/to/database.db
        # For in-memory: sqlite+aiosqlite:///:memory:
        if self.db == ":memory:":
            dsn = f"sqlite+{self.driver}:///{self.db}"
        else:
            # Assume db is a file path
            dsn = f"sqlite+{self.driver}:///{self.db}" # Note the three slashes for absolute/relative paths

        logger.debug(f"Built SQLite DSN: {dsn}")
        return dsn

    @property
    def dialect_driver(self) -> str:
        """Return the dialect+driver string."""
        return f"sqlite+{self.driver}"
