import uuid

from sqlalchemy import ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, MappedAsDataclass, mapped_column, relationship

from activealchemy.aio.activerecord import ActiveRecord, PKMixin, UpdateMixin


class ADemoBase(MappedAsDataclass, DeclarativeBase, ActiveRecord):
    pass


class AResident(ADemoBase, PKMixin, UpdateMixin):
    """User model."""

    __tablename__ = "resident"

    name: Mapped[str] = mapped_column(init=True, default=None)
    email: Mapped[str] = mapped_column(default=None, init=True)


class ACity(ADemoBase, PKMixin, UpdateMixin):
    """City model."""

    __tablename__ = "city"

    name: Mapped[str] = mapped_column(init=True, default=None)
    name: Mapped[str] = mapped_column(init=True, default=None)
    country_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("country.id"), default=None)
    country: Mapped["ACountry"] = relationship("ACountry", back_populates="cities", init=False, repr=False)


class ACountry(ADemoBase, PKMixin, UpdateMixin):
    """Country model."""

    __tablename__ = "country"

    name: Mapped[str] = mapped_column(init=True)
    code: Mapped[str] = mapped_column(init=True)
    cities: Mapped[list["ACity"]] = relationship(
        "ACity", back_populates="country", cascade="all, delete-orphan", default_factory=list, repr=False
    )
