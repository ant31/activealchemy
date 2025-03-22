import os
from contextlib import suppress

import pytest
import pytest_asyncio
from sqlalchemy import text

from activealchemy.aio import ActiveEngine as AsyncActiveEngine
from activealchemy.aio import ActiveRecord as AsyncActiveRecord
from activealchemy.config import PostgreSQLConfigSchema


# Database configuration for tests
@pytest.fixture
def db_config():
    """Get database configuration from environment variables or use defaults"""
    return PostgreSQLConfigSchema(
        user=os.environ.get("TEST_DB_USER", "activealchemy"),
        password=os.environ.get("TEST_DB_PASSWORD", "activealchemy"),
        host=os.environ.get("TEST_DB_HOST", "localhost"),
        port=int(os.environ.get("TEST_DB_PORT", "5434")),
        db=os.environ.get("TEST_DB", "pythonapp-test"),
        debug=os.environ.get("TEST_DB_DEBUG", "false").lower() == "true",
        use_internal_pool=False
    )





# Async fixtures
@pytest.fixture
def async_engine(db_config):
    """Create an async engine for tests"""
    db_config.async_driver = "asyncpg"
    db_config.mode = "async"
    db_config.use_internal_pool = False
    db_config.params = {"ssl": "disable"}
    engine = AsyncActiveEngine(db_config)
    AsyncActiveRecord.set_engine(engine)
    yield engine
    # engine.dispose_engines()
    # We need to handle the dispose_engines call differently for async
    # This will use the sync_dispose method since __del__ can't await


@pytest.fixture
def sync_session(sync_engine):
    """Create a sync session for tests"""
    _, session_factory = sync_engine.session()
    session = session_factory()
    try:
        yield session
    finally:
        session.rollback()
        session.close()

@pytest_asyncio.fixture
async def aclean_tables(async_engine):
    """Clean all tables before and after tests"""
    # List of tables to clean, ordered by dependency
    tables = ["resident", "city", "country"]
    # Resident.delete_all(commit=True)
    # City.delete_all(commit=True)
    # Country.delete_all(commit=True)
    # # Create a transaction to clean tables
    async with await AsyncActiveRecord.new_session() as session:
        for table in tables:
            with suppress(Exception):
                await session.execute(text(f"TRUNCATE TABLE {table} CASCADE"))

            # Table might not exist yet
        await session.commit()
