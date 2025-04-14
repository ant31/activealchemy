from activealchemy.config import PostgreSQLConfigSchema


def test_config():
    PostgresConfig = PostgreSQLConfigSchema(db="activealchemy-test", port=5434)
    assert PostgresConfig.db == "activealchemy-test"
    assert PostgresConfig.user == "activealchemy"
    assert PostgresConfig.port == 5434
    assert PostgresConfig.password == "activealchemy"
    assert PostgresConfig.host == "localhost"
    assert PostgresConfig.params == {"sslmode": "disable"}
    assert PostgresConfig.driver == "asyncpg"
    #``assert PostgresConfig.async_driver == "asyncpg"
    assert PostgresConfig.connect_timeout == 10
    assert PostgresConfig.create_engine_kwargs == {}
    assert PostgresConfig.debug is False
    assert PostgresConfig.default_schema == "public"


    assert PostgresConfig.uri() == "postgresql+asyncpg://activealchemy:activealchemy@localhost:5434/activealchemy-test?ssl=disable"

    PostgresConfig.port = 5435
    assert PostgresConfig.uri() == "postgresql+asyncpg://activealchemy:activealchemy@localhost:5435/activealchemy-test?ssl=disable"
    PostgresConfig. params = {"sslmode": "require"}
    assert PostgresConfig.uri() == "postgresql+asyncpg://activealchemy:activealchemy@localhost:5435/activealchemy-test?ssl=require"
    PostgresConfig.driver = "asyncpg-other"
    assert PostgresConfig.uri() == "postgresql+asyncpg-other://activealchemy:activealchemy@localhost:5435/activealchemy-test?sslmode=require"
