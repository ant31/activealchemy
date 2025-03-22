"""
Integration tests for synchronous ActiveAlchemy
"""
import uuid

from activealchemy.demo.models import City, Country, Resident


def test_integration_create_retrieve_update_delete(clean_tables, unique_id):
    """Test the full CRUD cycle in an integration test"""
    # Create a user
    with Country.new_session() as session:

        user = Resident(
            name=f"intuser_{unique_id}",
            email=f"int_{unique_id}@example.com"
        )
        user.save(commit=True, session=session)
        user_id = user.id

        # Retrieve the user
        retrieved_user = Resident.get(user_id, session=session)
        assert retrieved_user is not None
        assert retrieved_user.name == f"intuser_{unique_id}"
        assert retrieved_user.email == f"int_{unique_id}@example.com"

        c1 = Country(name="country1", code="c1").save(commit=True, session=session)
        c1.refresh_me()
        assert c1.id != uuid.UUID("00000000-0000-0000-0000-000000000000")
        cities = []
        # Create items for the user
        for i in range(3):
            cities.append(City(name=f"city{i}", country_id=c1.id).save(commit=True, session=session))

        assert len(cities) == 3
        c1_new = Country.get(c1.id, session=session)
        assert c1_new is not None
        assert len(City.all()) == 3
        assert len(c1_new.cities) == 3
        assert all(city.country_id == c1.id for city in cities)

        # Update the user
        c1_new.code = "c2"
        c1_new = c1_new.save(commit=True, session=session)

#     # verify update
        c1_new = Country.find(c1.id, session=session)
        assert c1_new is not None
        assert c1_new.code == "c2"

        new_cities = City.where(City.country_id == c1.id, session=session).scalars().all()
        assert len(new_cities) == 3
        Country.delete(c1_new, session=session)
        session.commit()
        c1_new = Country.find(c1.id, session=session)
        assert c1_new is None

        new_cities = City.where(City.country_id == c1.id, session=session).scalars().all()
        assert len(new_cities) == 0
