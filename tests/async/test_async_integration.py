"""
Integration tests for asynchronous ActiveAlchemy
"""

import asyncio
import uuid

import pytest

from activealchemy.demo.amodels import ACity, ACountry, AResident


@pytest.mark.asyncio
async def test_integration_create_retrieve_update_delete(async_engine, aclean_tables, unique_id):
    """Test the full CRUD cycle in an integration test"""
#    _ = await aclean_tables
    async with await ACountry.new_session() as session:

        user = AResident(
            name=f"intuser_{unique_id}",
            email=f"int_{unique_id}@example.com"
        )
        await user.save(commit=True, session=session)
        user_id = user.id

        # Retrieve the user
        retrieved_user = await AResident.get(user_id, session=session)
        assert retrieved_user is not None
        assert retrieved_user.name == f"intuser_{unique_id}"
        assert retrieved_user.email == f"int_{unique_id}@example.com"

        c1 = await ACountry(name="country1", code="c1").save(commit=True, session=session)
        # await c1.refresh_me()
        await session.refresh(c1)
        assert c1.id != uuid.UUID("00000000-0000-0000-0000-000000000000")

        # Create items for the user
        queries = []
        for i in range(3):
            queries.append(ACity(name=f"city{i}", country_id=c1.id).save(session=session, commit=False))
        cities = await asyncio.gather(*queries)
        await session.commit()
        assert len(cities) == 3
        c1_new = await ACountry.get(c1.id, session=session)
        assert c1_new is not None
        assert len(await ACity.all()) == 3
        assert len(await c1_new.awaitable_attrs.cities) == 3
        assert all(city.country_id == c1.id for city in cities)

        # Update the user
        c1_new.code = "c2"
        c1_new = await c1_new.save(commit=True, session=session)

#     # verify update
        c1_new = await ACountry.find(c1.id, session=session)
        assert c1_new is not None
        assert c1_new.code == "c2"

#         new_cities = (await ACity.where(ACity.country_id == c1.id, session=session).scalars()).all()
#         assert len(new_cities) == 3
#         await ACountry.delete(c1_new, session=session)
#         await session.commit()
#         c1_new = await ACountry.find(c1.id, session=session)
#         assert c1_new is None

#         new_cities = (await ACity.where(ACity.country_id == c1.id, session=session).scalars()).all()
#         assert len(new_cities) == 0


# @pytest.mark.asyncio
# async def test_integration_querying(engine_and_models, unique_id):
#     """Test complex querying functionality"""
#     # Create multiple users
#     users = []
#     for i in range(5):
#         user = AsyncTestUser(
#             username=f"quser_{i}_{unique_id}",
#             email=f"q_{i}_{unique_id}@example.com",
#             is_active=(i % 2 == 0)  # Some active, some inactive
#         )
#         await user.save(commit=True)
#         users.append(user)

#         # Create items for each user
#         for j in range(i + 1):  # Each user has a different number of items
#             item = AsyncTestItem(
#                 name=f"Item {j} for User {i}_{unique_id}",
#                 description=f"Description {j}",
#                 user_id=user.id
#             )
#             await item.save(commit=True)

#     # Query active users
#     active_users = await AsyncTestUser.all(AsyncTestUser.where(
#         AsyncTestUser.is_active == True,
#         AsyncTestUser.username.like(f"quser_%_{unique_id}")
#     ))
#     assert len(active_users) == 3  # Users 0, 2, 4 are active

#     # Query users with at least 3 items
#     users_with_many_items = []
#     all_query_users = await AsyncTestUser.all(AsyncTestUser.where(
#         AsyncTestUser.username.like(f"quser_%_{unique_id}")
#     ))
#     for user in all_query_users:
#         await AsyncTestUser.refresh(user)
#         if len(user.items) >= 3:
#             users_with_many_items.append(user)

#     assert len(users_with_many_items) == 3  # Users 2, 3, 4 have 3+ items

#     # Clean up
#     for user in users:
#         await user.delete_me()
#         await user.commit_me()


# @pytest.mark.asyncio
# async def test_integration_transactions(engine_and_models, unique_id):
#     """Test transaction handling"""
#     # Create a session
#     async with engine_and_models.session_factory() as session:
#         # Start a transaction
#         async with session.begin():
#             # Create a user within the transaction
#             user = AsyncTestUser(
#                 username=f"txuser_{unique_id}",
#                 email=f"tx_{unique_id}@example.com"
#             )
#             session.add(user)
#             await session.flush()  # Flush but don't commit

#             user_id = user.id

#             # Create an item
#             item = AsyncTestItem(
#                 name=f"Transaction item for {unique_id}",
#                 description="This item is in a transaction",
#                 user_id=user_id
#             )
#             session.add(item)

#             # Rollback manually (simulating an error)
#             # We use a nested session to rollback within our test
#             await session.rollback()

#     # After rollback, user shouldn't exist
#     found_user = await AsyncTestUser.get(user_id)
#     assert found_user is None

#     # Create a successful transaction
#     user = AsyncTestUser(
#         username=f"txuser2_{unique_id}",
#         email=f"tx2_{unique_id}@example.com"
#     )
#     await user.save(commit=False)  # Don't commit yet

#     item = AsyncTestItem(
#         name=f"Successful transaction item for {unique_id}",
#         description="This item will be committed",
#         user_id=user.id
#     )
#     await item.save(commit=False)  # Don't commit yet

#     # Now commit both changes at once
#     await user.commit_me()

#     # Verify both were saved
#     retrieved_user = await AsyncTestUser.get(user.id)
#     assert retrieved_user is not None

#     await AsyncTestUser.refresh(retrieved_user)
#     assert len(retrieved_user.items) == 1
#     assert retrieved_user.items[0].name == f"Successful transaction item for {unique_id}"

#     # Clean up
#     await user.delete_me()
#     await user.commit_me()


# @pytest.mark.asyncio
# async def test_integration_concurrency(engine_and_models, unique_id):
#     """Test concurrent operations"""
#     # Create a base user to attach items to
#     base_user = AsyncTestUser(
#         username=f"concurrent_user_{unique_id}",
#         email=f"concurrent_{unique_id}@example.com"
#     )
#     await base_user.save(commit=True)

#     # Function to create items asynchronously
#     async def create_item(i):
#         item = AsyncTestItem(
#             name=f"Concurrent Item {i} for {unique_id}",
#             description=f"Created concurrently {i}",
#             user_id=base_user.id
#         )
#         await item.save(commit=True)
#         return item.id

#     # Create multiple items concurrently
#     item_ids = await asyncio.gather(*[create_item(i) for i in range(10)])

#     # Verify all items were created
#     assert len(item_ids) == 10

#     # Retrieve the user and verify items
#     retrieved_user = await AsyncTestUser.get(base_user.id)
#     await AsyncTestUser.refresh(retrieved_user)

#     assert len(retrieved_user.items) == 10

#     # Function to update items asynchronously
#     async def update_item(item_id, i):
#         item = await AsyncTestItem.get(item_id)
#         item.description = f"Updated concurrently {i}"
#         await item.save(commit=True)

#     # Update all items concurrently
#     await asyncio.gather(*[update_item(item_id, i) for i, item_id in enumerate(item_ids)])

#     # Verify updates
#     for i, item_id in enumerate(item_ids):
#         item = await AsyncTestItem.get(item_id)
#         assert item.description == f"Updated concurrently {i}"

#     # Clean up
#     await base_user.delete_me()
#     await base_user.commit_me()
