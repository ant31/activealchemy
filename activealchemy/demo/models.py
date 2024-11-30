import uuid

from activealchemy.activerecord import ActiveRecord, PKMixin, UpdateMixin

from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, MappedAsDataclass

class DemoBase(MappedAsDataclass, DeclarativeBase, ActiveRecord):
    pass

class Resident(DemoBase, PKMixin, UpdateMixin):
    """User model."""

    __tablename__ = "resident"

    name: Mapped[str] = mapped_column(init=True, default=None)
    email: Mapped[str] = mapped_column(default=None, init=True)


class City(DemoBase, PKMixin, UpdateMixin):
    """City model."""

    __tablename__ = "city"

    name: Mapped[str] = mapped_column(init=True, default=None)
    code: Mapped[str] = mapped_column(init=True, default=None)
    country_id: Mapped[uuid.UUID] = mapped_column(init=False)
    # country: Mapped["Country"] = relationship("Country", back_populates="city", init=False)


class Country(DemoBase, PKMixin, UpdateMixin):
    """Country model."""

    __tablename__ = "country"

    name: Mapped[str] = mapped_column(init=True)
    code: Mapped[str] = mapped_column(init=True)
    # cities: Mapped[list["City"]] = relationship("City", back_populates="country", init=False)
