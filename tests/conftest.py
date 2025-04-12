import uuid

import pytest
from activealchemy import ActiveEngine
from activealchemy.demo.models import DemoBase # Import the Base model


from activealchemy.config import PostgreSQLConfigSchema
import pytest_asyncio

# Utility fixtures
@pytest.fixture
def unique_id():
    """Generate a unique ID for test data"""
    return str(uuid.uuid4())


@pytest.fixture
def db_config():
    """Provide database configuration for tests."""
    # Example: Load from environment variables or a test config file
    # For simplicity, using defaults here. Replace with your actual config loading.
    return PostgreSQLConfigSchema()


@pytest.fixture
def async_engine(db_config: PostgreSQLConfigSchema) -> ActiveEngine:
    """Create an async engine instance for testing."""
    # Ensure ActiveEngine is initialized correctly
    engine = ActiveEngine(config=db_config, echo=db_config.debug) # Pass echo explicitly if needed
    # The Base model needs the engine set
    DemoBase.set_engine(engine) # Set the engine on the Base
    return engine


@pytest_asyncio.fixture
async def aclean_tables(async_engine: ActiveEngine):
    """Fixture to clean tables before and after async tests."""
    async with async_engine.engine().begin() as conn:
        await conn.run_sync(DemoBase.metadata.drop_all)
        await conn.run_sync(DemoBase.metadata.create_all)
    yield
    async with async_engine.engine().begin() as conn:
        await conn.run_sync(DemoBase.metadata.drop_all)
