import uuid

from activealchemy.activerecord import ActiveRecordMixin, PKMixin, UpdateMixin
from activealchemy.engine import ActiveEngine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class BaseMixin(DeclarativeBase, ActiveRecordMixin):
    """Base mixin class for all models."""

    __active_engine__: ActiveEngine


class Resident(BaseMixin, PKMixin, UpdateMixin):
    """User model."""

    __tablename__ = "resident"

    name: Mapped[str] = mapped_column(init=True)
    email: Mapped[str] = mapped_column(default=None)


class City(BaseMixin, PKMixin, UpdateMixin):
    """City model."""

    __tablename__ = "city"

    name: Mapped[str] = mapped_column(init=True)
    code: Mapped[str] = mapped_column(init=True)
    country_id: Mapped[uuid.UUID] = mapped_column(init=False)
    # country: Mapped["Country"] = relationship("Country", back_populates="city", init=False)


class Country(BaseMixin, PKMixin, UpdateMixin):
    """Country model."""

    __tablename__ = "country"

    name: Mapped[str] = mapped_column(init=True)
    code: Mapped[str] = mapped_column(init=True)
    # cities: Mapped[list["City"]] = relationship("City", back_populates="country", init=False)
