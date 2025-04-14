import os
from contextlib import suppress

from activealchemy import Base, Select, ActiveEngine, ActiveRecord, PostgreSQLConfigSchema


import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy import Column, String



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
        driver="asyncpg"
    )





# Async fixtures
@pytest.fixture
def async_engine(db_config):
    """Create an async engine for tests"""
    db_config.driver = "asyncpg"
    db_config.params = {"ssl": "disable"}
    engine = ActiveEngine(db_config)
    ActiveRecord.set_engine(engine)
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
    async with await ActiveRecord.get_session() as session:
        for table in tables:
            with suppress(Exception):
                await session.execute(text(f"TRUNCATE TABLE {table} CASCADE"))

            # Table might not exist yet
        await session.commit()


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
async def setup_select(async_engine):
    """Set up select tests"""
    TestModel.set_engine(async_engine)

    # Clean up

    with suppress(Exception):
        async with await TestModel.get_session() as session:
            conn = await session.connection()
            await conn.run_sync(TestModel.metadata.drop_all)

    # Create the table
    async with await TestModel.get_session() as session:
        conn = await session.connection()
        await conn.run_sync(TestModel.metadata.create_all)
        await session.commit()

    # Add test data
    model1 = TestModel(id="1", name="Test 1")
    model2 = TestModel(id="2", name="Test 2")
    model3 = TestModel(id="3", name="Test 3")

    async with await TestModel.get_session() as session:
        session.add_all([model1, model2, model3])


    yield async_engine

    # Clean up
    async with await TestModel.get_session() as session:
        conn = await session.connection()
        await conn.run_sync(TestModel.metadata.drop_all)

    TestModel.__active_engine__ = None


@pytest_asyncio.fixture
async def setup_mixin_tests(async_engine, mock_pk_model_class, mock_update_model_class, mock_combined_model_class):
    """Set up engine and tables for mixin tests."""
    models = [mock_pk_model_class, mock_update_model_class, mock_combined_model_class]
    for model in models:
        model.set_engine(async_engine)

    # Drop tables first (suppress errors if they don't exist)
    async with await models[0].get_session() as session:
        conn = await session.connection()
        for model in reversed(models): # Drop in reverse order of potential dependencies
            with suppress(Exception):
                await conn.run_sync(model.metadata.drop_all)
        await session.commit()


    # Create tables
    async with await models[0].get_session() as session:
        conn = await session.connection()
        for model in models:
            await conn.run_sync(model.metadata.create_all)
        await session.commit()

    yield async_engine # Provide engine to tests if needed

    # Clean up tables after tests
    async with await models[0].get_session() as session:
        conn = await session.connection()
        for model in reversed(models):
             with suppress(Exception):
                 await conn.run_sync(model.metadata.drop_all)
        await session.commit()

    # Clear engine association
    for model in models:
        model.__active_engine__ = None
