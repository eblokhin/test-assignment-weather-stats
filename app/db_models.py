from sqlalchemy.orm import mapped_column, DeclarativeBase
from sqlalchemy import String, Numeric, Date
from sqlalchemy.dialects.postgresql import JSONB


class Base(DeclarativeBase):
    pass


class LocationData(Base):
    __tablename__ = "location_data"

    longitude = mapped_column(Numeric(7, 4), nullable=False, primary_key=True)
    latitude = mapped_column(Numeric(7, 4), nullable=False, primary_key=True)
    date = mapped_column(Date, nullable=False, primary_key=True)
    timezone = mapped_column(String(50), nullable=True)
    data = mapped_column(JSONB, nullable=True)

    def __repr__(self):
        return f"<LocationData(id={self.id}, lat={self.latitude}, lon={self.longitude}, date={self.date})>"
