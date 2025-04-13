"""
Unit tests for the async Select class
"""

import pytest



from activealchemy.aio import Base
from activealchemy.aio.activerecord import Select




@pytest.mark.asyncio
async def test_select_init(setup_select, test_model):
    """Test Select class initialization"""
    # Test initialization with session
    TestModel = test_model
    async with await TestModel.new_session() as session:
        select = TestModel.select(session=session)
        assert select.session == session
        assert select.cls == TestModel
    
    # Test initialization without session
    select = Select[TestModel](TestModel)
    assert select.session is None
    assert select.cls == TestModel


@pytest.mark.asyncio
async def test_select_scalars(async_engine, setup_select, test_model):
    """Test Select.scalars method"""
    # With provided session
    TestModel = test_model
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
async def test_select_where(setup_select, test_model):
    """Test Select with where clause"""
    TestModel = test_model
    async with await TestModel.new_session() as session:
        select = TestModel.select(session=session).where(TestModel.name == "Test 2")
        result = await select.scalars()
        items = list(result)
        assert len(items) == 1
        assert items[0].name == "Test 2"


@pytest.mark.asyncio
async def test_select_order_by(setup_select, test_model):
    """Test Select with order_by clause"""
    TestModel = test_model
    async with await TestModel.new_session() as session:
        select = TestModel.select(session=session).order_by(TestModel.name.desc())
        result = await select.scalars()
        items = list(result)
        assert len(items) == 3
        assert items[0].name == "Test 3"
        assert items[1].name == "Test 2"
        assert items[2].name == "Test 1"


@pytest.mark.asyncio
async def test_select_limit(setup_select, test_model):
    """Test Select with limit clause"""
    TestModel = test_model
    async with await TestModel.new_session() as session:
        select = TestModel.select(session=session).limit(2)
        result = await select.scalars()
        items = list(result)
        assert len(items) == 2

