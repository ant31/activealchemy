# pylint: disable=no-self-argument
import logging
import logging.config

from typing import Literal

from pydantic import Field, BaseModel, ConfigDict


logger: logging.Logger = logging.getLogger("activealchemy")


class BaseConfig(BaseModel):
    model_config = ConfigDict(extra="allow")


class PostgreSQLConfigSchema(BaseConfig):
    db: str = Field(default="engine")
    user: str = Field(default="al")
    port: int = Field(default=5432)
    password: str = Field(default="oi")
    host: str = Field(default="localhost")
    params: dict[str, str] = Field(default_factory=dict)
    driver: str = Field(default="psycopg2")
    async_driver: str = Field(default="asyncpg")
    use_internal_pool: bool = Field(default=True)
    connect_timeout: int = Field(default=10)
    create_engine_kwargs: dict[str, Any] = Field(default_factory=dict)
    debug: bool = Field(default=False)
    default_schema: str = Field(default="public")
    mode: Literal["sync", "async"] = Field(default="sync")

    def uri(self) -> str:
        host = f"postgresql+{self.driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        params = "&".join([f"{k}={v}" for k, v in self.params.items()])
        if params:
            host = f"{host}?{params}"
        return host

    def async_uri(self) -> str:
        host = f"postgresql+{self.async_driver}://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}"
        params = "&".join([f"{k}={v}" for k, v in self.params.items()])
        if params:
            host = f"{host}?{params}"
        return host
