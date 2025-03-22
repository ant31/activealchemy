import os
from contextlib import suppress

import pytest
from sqlalchemy import text

from activealchemy.config import PostgreSQLConfigSchema
from activealchemy.sync import ActiveEngine, ActiveRecord


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


# Sync fixtures
@pytest.fixture
def sync_engine(db_config):
    """Create a sync engine for tests"""
    engine = ActiveEngine(db_config)
    ActiveRecord.set_engine(engine)
    yield engine


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


@pytest.fixture
def clean_tables(sync_engine):
    """Clean all tables before and after tests"""
    # List of tables to clean, ordered by dependency
    tables = ["resident", "city", "country"]
    # Resident.delete_all(commit=True)
    # City.delete_all(commit=True)
    # Country.delete_all(commit=True)

    # # Create a transaction to clean tables
    with ActiveRecord.new_session() as session:
        for table in tables:
            with suppress(Exception):
                session.execute(text(f"TRUNCATE TABLE {table} CASCADE"))
            session.commit()
