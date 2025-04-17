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
    # assert PostgresConfig.kwargs == {} # Removed assertion for removed field


    expected_uri_5434 = "postgresql+asyncpg://activealchemy:activealchemy@localhost:5434/activealchemy-test?sslmode=disable"
    assert PostgresConfig.uri() == expected_uri_5434

    PostgresConfig.port = 5435
    expected_uri_5435_disable = "postgresql+asyncpg://activealchemy:activealchemy@localhost:5435/activealchemy-test?sslmode=disable"
    assert PostgresConfig.uri() == expected_uri_5435_disable

    PostgresConfig. params = {"sslmode": "require"}
    expected_uri_5435_require = "postgresql+asyncpg://activealchemy:activealchemy@localhost:5435/activealchemy-test?sslmode=require"
    assert PostgresConfig.uri() == expected_uri_5435_require

    # Test with a different (hypothetical) driver to ensure params aren't altered
    PostgresConfig.driver = "psycopg-imaginary"
    assert PostgresConfig.uri() == "postgresql+psycopg-imaginary://activealchemy:activealchemy@localhost:5435/activealchemy-test?sslmode=require"
