import uuid

import pytest


from activealchemy.config import PostgreSQLConfigSchema


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

