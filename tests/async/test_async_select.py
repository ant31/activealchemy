"""
Unit tests for the async Select class
"""
from unittest.mock import AsyncMock, patch

import pytest
from sqlalchemy.exc import SQLAlchemyError


@pytest.mark.asyncio
async def test_select_init(setup_select, test_model):
    """Test Select class initialization"""
    # Test initialization (session is no longer stored in Select)

    TestModel = test_model
    select = TestModel.select() # No session passed here
    # assert select._session is None # _session attribute removed
    assert select._orm_cls == TestModel

    # Test initialization without session (same as above now)
    # select = Select[TestModel](TestModel) # Select() takes entities directly
    # select.set_context(cls=TestModel) # No session in set_context
    # assert select._session is None
    # assert select._orm_cls == TestModel


@pytest.mark.asyncio
async def test_select_scalars(async_engine, setup_select, test_model):
    """Test Select.scalars method"""
    TestModel = test_model
    # Session must be provided to scalars()
    async with await TestModel.get_session() as session:
        select = TestModel.select() # Create select statement
        result = await select.scalars(session=session) # Pass session here
        items = list(result)
        assert len(items) == 3
        assert all(isinstance(item, TestModel) for item in items)

    # Test calling via ActiveRecord.all (which handles session internally)
    result_all = await TestModel.all() # No session passed, all() manages it
    items_all = list(result_all)
    assert len(items_all) == 3


@pytest.mark.asyncio
async def test_select_where(setup_select, test_model):
    """Test Select with where clause"""
    TestModel = test_model
    async with await TestModel.get_session() as session:
        select = TestModel.select().where(TestModel.name == "Test 2") # No session in select()
        result = await select.scalars(session=session) # Pass session here
        items = list(result)
        assert len(items) == 1
        assert items[0].name == "Test 2"


@pytest.mark.asyncio
async def test_select_order_by(setup_select, test_model):
    """Test Select with order_by clause"""
    TestModel = test_model
    async with await TestModel.get_session() as session:
        select = TestModel.select().order_by(TestModel.name.desc()) # No session in select()
        result = await select.scalars(session=session) # Pass session here
        items = list(result)
        assert len(items) == 3
        assert items[0].name == "Test 3"
        assert items[1].name == "Test 2"
        assert items[2].name == "Test 1"


@pytest.mark.asyncio
async def test_select_limit(setup_select, test_model):
    """Test Select with limit clause"""
    TestModel = test_model
    async with await TestModel.get_session() as session:
        select = TestModel.select().limit(2) # No session in select()
        result = await select.scalars(session=session) # Pass session here
        items = list(result)
        assert len(items) == 2


@pytest.mark.asyncio
async def test_select_scalars_no_session(setup_select, test_model):
    """Test Select.scalars raises ValueError if session is None."""
    TestModel = test_model
    select = TestModel.select()
    with pytest.raises(ValueError, match="Session is required"):
        # Explicitly pass None, although type hints should prevent this in real code
        await select.scalars(session=None) # type: ignore


@pytest.mark.asyncio
async def test_select_scalars_db_error(setup_select, test_model):
    """Test Select.scalars raises SQLAlchemyError on execution failure."""
    TestModel = test_model
    select = TestModel.select()

    # Mock the session's execute method to raise an error
    mock_session = AsyncMock()
    mock_session.execute.side_effect = SQLAlchemyError("Database connection failed")

    with pytest.raises(SQLAlchemyError, match="Database connection failed"):
        await select.scalars(session=mock_session)

    mock_session.execute.assert_awaited_once()

