import os

import pytest
import pytest_asyncio
import sqlalchemy
from sqlalchemy import Column, Integer, String, text

from activealchemy import ActiveEngine, ActiveRecord, Base, PKMixin, PostgreSQLConfigSchema, SQLiteConfigSchema


# Database configuration for tests
@pytest.fixture(scope="session")
def db_config():
    """Get database configuration from environment variables or use defaults"""
    return PostgreSQLConfigSchema(
        user=os.environ.get("TEST_DB_USER", "activealchemy"),
        password=os.environ.get("TEST_DB_PASSWORD", "activealchemy"),
        host=os.environ.get("TEST_DB_HOST", "localhost"),
        port=int(os.environ.get("TEST_DB_PORT", "5434")),
        db=os.environ.get("TEST_DB", "pythonapp-test"),
        debug=os.environ.get("TEST_DB_DEBUG", "false").lower() == "true",
        driver="asyncpg"
    )


# --- SQLite Fixtures ---

@pytest.fixture(scope="session")
def sqlite_config():
    """Provides an in-memory SQLite configuration."""
    return SQLiteConfigSchema(
        db=":memory:",
        driver="aiosqlite",
        debug=os.environ.get("TEST_DB_DEBUG", "false").lower() == "true",
        # connect_timeout is handled by engine prep based on config
    )

@pytest_asyncio.fixture(scope="session")
async def async_sqlite_engine(sqlite_config):
    """Create an async SQLite engine once per test session."""
    print("Creating async SQLite engine (:memory:)...")
    # No need to set params like for PG here, connect_args handled by ActiveEngine
    engine = ActiveEngine(sqlite_config)
    # Note: We don't set ActiveRecord.set_engine globally here,
    # tests using SQLite will need to set it explicitly or use the engine directly.
    yield engine

    print("\nDisposing async SQLite engines...")
    await engine.dispose_engines()
    print("Async SQLite engines disposed.")


from sqlalchemy.orm import Mapped, mapped_column  # Import necessary types


class SQLiteTestModel(Base, PKMixin):
    """Simple model specifically for SQLite tests."""
    __tablename__ = "aa_sqlite_test_models" # Renamed to avoid SQLite prefix conflict
    # id is inherited from PKMixin
    # Define 'value' using Mapped/mapped_column and set init=True
    value: Mapped[int] = mapped_column(Integer, nullable=False, init=True)


@pytest.fixture(scope="session") # Session scope as the class definition doesn't change
def sqlite_test_model_class():
    """Provides the SQLiteTestModel class for tests."""
    return SQLiteTestModel


@pytest_asyncio.fixture(scope="function")
async def sqlite_session_with_tables(async_sqlite_engine, sqlite_test_model_class):
    """
    Provides an AsyncSession with the SQLite test table created.
    Ensures cleanup after the test.
    """
    # Use the session factory associated with the engine/model
    # This ensures consistency if the factory has specific settings
    session_factory = async_sqlite_engine.session()[1] # Get sessionmaker
    model = sqlite_test_model_class
    engine = async_sqlite_engine.engine() # Get the engine

    # Create tables using a connection from the engine first
    async with engine.begin() as conn:
        print(f"\n[SQLite Setup] Creating table {model.__tablename__} using connection {conn}...")
        await conn.run_sync(model.metadata.create_all)
        print(f"[SQLite Setup] Table {model.__tablename__} created.")

    # Now create and yield the session for the test
    async with session_factory() as session:
        yield session

        # Teardown: Clean the table using the same session
        print(f"\n[SQLite Teardown] Cleaning table {model.__tablename__} in session {session}...")
        try:
            # Use DELETE within the same session context
            async with session.begin(): # Use transaction for cleanup
                await session.execute(text(f'DELETE FROM "{model.__tablename__}"'))
                print(f"[SQLite Teardown] Cleaned table {model.__tablename__}.")
        except sqlalchemy.exc.SQLAlchemyError as e:
            print(f"[SQLite Teardown] Error cleaning table {model.__tablename__}: {e}")
            await session.rollback() # Ensure rollback on error during cleanup
        # Session is closed automatically by the outer async with session_factory()


# --- PostgreSQL Fixtures (Existing) ---

@pytest_asyncio.fixture(scope="session")
async def async_engine(db_config):
    """Create an async engine once per test session""" # Updated docstring
    db_config.driver = "asyncpg"
    db_config.params = {"ssl": "disable", "timeout": 5}
    print("Creating async engine...")
    engine = ActiveEngine(db_config)
    ActiveRecord.set_engine(engine)
    print("Set Engine")
    yield engine

    print("\nDisposing async engines...") # Add print for debugging test runs
    await engine.dispose_engines()
    print("Async engines disposed.")


class TestModel(Base):
    """Test model for select tests"""
    # Note: Removed aclean_tables fixture. Tests now rely on unique_id
    # for isolation. Ensure all test data creation and queries use unique_id.
    __tablename__ = "test_select_models"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)

@pytest.fixture(scope="session")
def test_model():
    """Provide the TestModel class for tests"""
    return TestModel

@pytest_asyncio.fixture
async def setup_select(async_engine, test_model): # Removed aclean_tables
    """Set up select tests"""
    print("setup_select")
    TestModel.set_engine(async_engine)

    # Clean up
    async with async_engine.engine().begin() as conn:
        # Pass the specific model's metadata
        # Ensure table exists (create_all is idempotent)
        await conn.run_sync(test_model.metadata.create_all)

    # Data creation moved to individual tests
    print("setup_select: Table ensured.")

@pytest_asyncio.fixture
async def setup_mixin_tests(async_engine, # Removed aclean_tables
                            mock_pk_model_class, mock_update_model_class, mock_combined_model_class):
    """Set up engine and tables for mixin tests."""
    models = [mock_pk_model_class, mock_update_model_class, mock_combined_model_class]
    # Create tables
    print("setup_mixin_tests")
    async with async_engine.engine().begin() as conn:
        print("Creating tables...")
        for model in models:
            print(f"Creating table: {model.__tablename__}")
            await conn.run_sync(model.metadata.create_all)


@pytest_asyncio.fixture(scope="session",autouse=True)
async def aclean_tables(async_engine):
    """Clean all tables before and after tests"""
    tables = ["test_select_models", "mock_pk_models", "mock_update_models",
              "mock_combined_models", "resident_city", "resident", "city","country", ]
    print("aclean")
    # Use the engine manager provided by the fixture
    # Get a session using the globally set engine manager
    async with await ActiveRecord.get_session() as session:
        print("Cleaning tables...")
        async with session.begin(): # Use a transaction for cleanup
            print("Cleaning tables in transaction...")
            for table in tables: # Truncate in reverse dependency order
                print(f"Truncating table: {table}") # Debug print
                # Suppress errors if table doesn't exist
                try:
                    await session.execute(text(f'DELETE FROM "{table}"'))
                    print(f"Truncated table: {table}")
                except sqlalchemy.exc.SQLAlchemyError as e:
                    # This might happen if the table doesn't exist yet on the first run
                    print(f"Error deleting from table {table}: {e}")
        # No explicit commit needed with session.begin()

    yield # Let the test run

    # Add cleanup *after* the test as well to ensure clean state
    print("aclean (post-yield)")
    async with await ActiveRecord.get_session() as session:
        print("Cleaning tables post-yield...")
        async with session.begin():
            print("Cleaning tables post-yield in transaction...")
            # Iterate in reverse to handle potential foreign key dependencies if any exist
            for table in reversed(tables):
                print(f"Deleting from table post-yield: {table}")
                try:
                    await session.execute(text(f'DELETE FROM "{table}"'))
                    print(f"Deleted from table post-yield: {table}")
                except sqlalchemy.exc.SQLAlchemyError as e:
                    print(f"Error deleting post-yield from table {table}: {e}")
