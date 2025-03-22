import uuid

import pytest


# Utility fixtures
@pytest.fixture
def unique_id():
    """Generate a unique ID for test data"""
    return str(uuid.uuid4())
