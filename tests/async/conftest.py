import os

import pytest
import pytest_asyncio
import sqlalchemy.exc
from sqlalchemy import Column, String, text

from activealchemy import ActiveEngine, ActiveRecord, Base, PostgreSQLConfigSchema


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


# Async fixtures
@pytest_asyncio.fixture(scope="session")
async def async_engine(db_config):
    """Create an async engine for tests"""
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


    # engine.dispose_engines()
    # We need to handle the dispose_engines call differently for async
    # This will use the sync_dispose method since __del__ can't await
    #
@pytest_asyncio.fixture
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

class TestModel(Base):
    """Test model for select tests"""
    __tablename__ = "test_select_models"

    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)

@pytest.fixture(scope="session")
def test_model():
    """Provide the TestModel class for tests"""
    return TestModel

@pytest_asyncio.fixture
async def setup_select(async_engine, aclean_tables, test_model):
    """Set up select tests"""
    print("setup_select")
    TestModel.set_engine(async_engine)

    # Clean up
    async with async_engine.engine().begin() as conn:
        # Pass the specific model's metadata
        await conn.run_sync(test_model.metadata.create_all)

    # Add test data
    model1 = TestModel(id="1", name="Test 1")
    model2 = TestModel(id="2", name="Test 2")
    model3 = TestModel(id="3", name="Test 3")
    print("Adding test data...")
    async with await TestModel.get_session() as session:
        await TestModel.add_all([model1, model2, model3], commit=True, session=session)

        print("Adding test data... exit")

@pytest_asyncio.fixture
async def setup_mixin_tests(async_engine, aclean_tables,
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
