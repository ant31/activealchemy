"""
Integration tests specifically using the SQLite backend.
"""

# import pytest

# Fixtures used:
# - async_sqlite_engine: Provides the ActiveEngine instance configured for SQLite.
# - SQLiteTestModel: The ORM model class for these tests.
# - aclean_sqlite_tables: Ensures the table is created and cleaned for each test.
# - unique_id: Generates unique identifiers for test data isolation.


# @pytest.mark.asyncio
# async def test_sqlite_create_retrieve(async_sqlite_engine, sqlite_test_model_class, sqlite_session_with_tables, unique_id):
#     """Test basic create and retrieve operations with SQLite."""
#     # Use the fixture that provides the class
#     Model = sqlite_test_model_class
#     # Use the session provided by the fixture
#     session = sqlite_session_with_tables
#     # Set the engine (still potentially useful for class methods not taking session)
#     Model.set_engine(async_sqlite_engine)

#     test_value = int(unique_id[:8], 16) # Create a unique integer value

#     # Create - Pass the session explicitly
#     new_item = Model(value=test_value)
#     # save() implicitly calls add(), pass session there
#     await new_item.save(commit=True, session=session)

#     assert new_item.id is not None
#     assert new_item.value == test_value

#     # Retrieve using get() - Pass the session
#     retrieved_item = await Model.get(new_item.id, session=session)
#     assert retrieved_item is not None
#     assert retrieved_item.id == new_item.id
#     assert retrieved_item.value == test_value

#     # Retrieve using find_by() - Pass the session
#     found_item = await Model.find_by(value=test_value, session=session)
#     assert found_item is not None
#     assert found_item.id == new_item.id
#     assert found_item.value == test_value

#     # Retrieve using first() with where() - Pass the session
#     # Note: where() itself doesn't need the session, but first() does
#     first_item = await Model.where(Model.value == test_value).first(session=session)
#     assert first_item is not None
#     assert first_item.id == new_item.id

#     # Cleaned up by sqlite_session_with_tables fixture


# @pytest.mark.asyncio
# async def test_sqlite_add_all(async_sqlite_engine, sqlite_test_model_class, sqlite_session_with_tables, unique_id):
#     """Test adding multiple items using add_all with SQLite."""
#     Model = sqlite_test_model_class
#     session = sqlite_session_with_tables
#     Model.set_engine(async_sqlite_engine)

#     # Remove hyphens from unique_id before slicing
#     uid_hex = unique_id.replace("-", "")
#     val1 = int(uid_hex[0:8], 16)
#     val2 = int(uid_hex[8:16], 16)
#     val3 = int(uid_hex[16:24], 16)


#     items_to_add = [
#         Model(value=val1),
#         Model(value=val2),
#         Model(value=val3),
#     ]

#     # Add all - Pass the session
#     added_items = await Model.add_all(items_to_add, commit=True, session=session)

#     assert len(added_items) == 3
#     for item in added_items:
#         assert item.id is not None # Ensure IDs were generated/assigned

#     # Verify count - Pass the session
#     count = await Model.count(session=session)
#     assert count == 3

#     # Verify retrieval - Pass the session
#     all_items = await Model.all(session=session)
#     assert len(all_items) == 3
#     retrieved_values = {item.value for item in all_items}
#     assert retrieved_values == {val1, val2, val3}

#     # Cleaned up by sqlite_session_with_tables fixture


# @pytest.mark.asyncio
# async def test_sqlite_delete(async_sqlite_engine, sqlite_test_model_class, sqlite_session_with_tables, unique_id):
#     """Test deleting an item with SQLite."""
#     Model = sqlite_test_model_class
#     session = sqlite_session_with_tables
#     Model.set_engine(async_sqlite_engine)

#     test_value = int(unique_id[:8], 16)
#     item = Model(value=test_value)
#     # Save using the specific session
#     await item.save(commit=True, session=session)

#     # Verify creation - Pass the session
#     assert await Model.count(session=session) == 1

#     # Delete - Pass the session
#     await Model.delete(item, commit=True, session=session)

#     # Verify deletion - Pass the session
#     assert await Model.count(session=session) == 0
#     assert await Model.get(item.id, session=session) is None

#     # Cleaned up by sqlite_session_with_tables fixture
