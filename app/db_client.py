from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert

from app.db_models import LocationData
from app.config import db_connection_string

engine = create_engine(db_connection_string, echo=False)


def save_records_data(records):
    with Session(engine) as session:
        session.begin()
        try:
            session.execute(
                insert(LocationData)
                .values(records)
                # TOFIX: figure out how to handle correctly
                .on_conflict_do_nothing(constraint='pk_location_data')
            )
        except:
            session.rollback()
            raise
        else:
            session.commit()
