import os

import pytest
import pytest_asyncio
from sqlalchemy import Column, String

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
