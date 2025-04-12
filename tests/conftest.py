import uuid

import pytest
from activealchemy import ActiveEngine
from activealchemy.demo.models import DemoBase # Import the Base model


# Utility fixtures
@pytest.fixture
def unique_id():
    """Generate a unique ID for test data"""
    return str(uuid.uuid4())
