import activealchemy
from activealchemy.config import PostgreSQLConfigSchema

def test_config():
    PostgresConfig = PostgreSQLConfigSchema(db="activealchemy-test", port=5434)
    assert PostgresConfig.db == "activealchemy-test"
    assert PostgresConfig.user == "activealchemy"
    assert PostgresConfig.port == 5434
    assert PostgresConfig.password == "activealchemy"
    assert PostgresConfig.host == "localhost"
    assert PostgresConfig.params == {"sslmode": "disable"}
    assert PostgresConfig.driver == "psycopg2"
    assert PostgresConfig.async_driver == "asyncpg"
    assert PostgresConfig.use_internal_pool is True
    assert PostgresConfig.connect_timeout == 10
    assert PostgresConfig.create_engine_kwargs == {}
    assert PostgresConfig.debug is False
    assert PostgresConfig.default_schema == "public"
    assert PostgresConfig.mode == "sync"

    assert PostgresConfig.uri() == "postgresql+psycopg2://activealchemy:activealchemy@localhost:5434/activealchemy-test?sslmode=disable"
    assert PostgresConfig.async_uri() == "postgresql+asyncpg://activealchemy:activealchemy@localhost:5434/activealchemy-test?sslmode=disable"
    PostgresConfig.port = 5435
    assert PostgresConfig.uri() == "postgresql+psycopg2://activealchemy:activealchemy@localhost:5435/activealchemy-test?sslmode=disable"
    assert PostgresConfig.async_uri() == "postgresql+asyncpg://activealchemy:activealchemy@localhost:5435/activealchemy-test?sslmode=disable"
    PostgresConfig. params = {"sslmode": "require"}
    assert PostgresConfig.uri() == "postgresql+psycopg2://activealchemy:activealchemy@localhost:5435/activealchemy-test?sslmode=require"
