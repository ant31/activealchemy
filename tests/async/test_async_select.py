"""
Unit tests for the async Select class
"""
from contextlib import suppress

import pytest
import pytest_asyncio
from sqlalchemy import Column, String

from activealchemy.aio import Base
from activealchemy.aio.activerecord import Select


class TestModel(Base):
    """Test model for select tests"""
    __tablename__ = "test_select_models"
    
    id = Column(String, primary_key=True)
    name = Column(String, nullable=False)


@pytest_asyncio.fixture
async def setup_select(async_engine):
    """Set up select tests"""
    TestModel.set_engine(async_engine)

    # Clean up

    with suppress(Exception):
        async with await TestModel.new_session() as session:
            conn = await session.connection()
            await conn.run_sync(TestModel.metadata.drop_all)

    # Create the table
    async with await TestModel.new_session() as session:
        conn = await session.connection()
        await conn.run_sync(TestModel.metadata.create_all)
        await session.commit()
    
    # Add test data
    model1 = TestModel(id="1", name="Test 1")
    model2 = TestModel(id="2", name="Test 2")
    model3 = TestModel(id="3", name="Test 3")
    
    async with await TestModel.new_session() as session:
        session.add_all([model1, model2, model3])
        # await session.commit()
    
    yield async_engine
    
    # Clean up
    async with await TestModel.new_session() as session:
        conn = await session.connection()
        await conn.run_sync(TestModel.metadata.drop_all)

    TestModel.__active_engine__ = None


@pytest.mark.asyncio
async def test_select_init(setup_select):
    """Test Select class initialization"""
    # Test initialization with session
    async with await TestModel.new_session() as session:
        select = TestModel.select(session=session)
        assert select.session == session
        assert select.cls == TestModel
    
    # Test initialization without session
    select = Select[TestModel](TestModel)
    assert select.session is None
    assert select.cls == TestModel


@pytest.mark.asyncio
async def test_select_scalars(setup_select):
    """Test Select.scalars method"""
    # With provided session
    async with await TestModel.new_session() as session:
        select = TestModel.select(session=session)
        result = await select.scalars()
        items = list(result)
        assert len(items) == 3
        assert all(isinstance(item, TestModel) for item in items)
    
    # Without provided session

        result = await TestModel.all(session=session)
        items = list(result)
        assert len(items) == 3


@pytest.mark.asyncio
async def test_select_where(setup_select):
    """Test Select with where clause"""
    async with await TestModel.new_session() as session:
        select = TestModel.select(session=session).where(TestModel.name == "Test 2")
        result = await select.scalars()
        items = list(result)
        assert len(items) == 1
        assert items[0].name == "Test 2"


@pytest.mark.asyncio
async def test_select_order_by(setup_select):
    """Test Select with order_by clause"""
    async with await TestModel.new_session() as session:
        select = TestModel.select(session=session).order_by(TestModel.name.desc())
        result = await select.scalars()
        items = list(result)
        assert len(items) == 3
        assert items[0].name == "Test 3"
        assert items[1].name == "Test 2"
        assert items[2].name == "Test 1"


@pytest.mark.asyncio
async def test_select_limit(setup_select):
    """Test Select with limit clause"""
    async with await TestModel.new_session() as session:
        select = TestModel.select(session=session).limit(2)
        result = await select.scalars()
        items = list(result)
        assert len(items) == 2

