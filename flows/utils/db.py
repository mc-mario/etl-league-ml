import os

from prefect.variables import Variable
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base


async def db_create_session():
    data_path = await Variable.get('data_path')
    db_path = f"{data_path.value}/etl_status.db"
    engine = create_engine(f'sqlite:///{db_path}')
    Session = sessionmaker(bind=engine)
    session = Session()

    if not os.path.exists(db_path):
        Base.metadata.create_all(engine)

    return session

Base = declarative_base()

class Match(Base):
    __tablename__ = 'matches'
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(String, unique=True, nullable=False)
    bronze = Column(Boolean, default=False)
    silver = Column(Boolean, default=False)
    gold = Column(Boolean, default=False)
    created_on = Column(DateTime, default=func.now())


def is_match_id_processed(session, match_id):
    return session.query(Match).filter_by(match_id=match_id).one_or_none() is not None


def add_match_id(session, match_id, bronze=False, silver=False, gold=False):
    if is_match_id_processed(session, match_id):
        return
    new_match = Match(match_id=match_id, bronze=bronze, silver=silver, gold=gold)
    session.add(new_match)
    session.commit()

def get_match_id(session, filters={'bronze': False}):
    elem = session.query(Match).filter_by(**filters).first()
    if elem is None:
        return None

    return elem.match_id

def complete_step(session, match_id, attr, value):
    elem = session.query(Match).filter_by(match_id=match_id).first()

    setattr(elem, attr, value)

    session.add(elem)
    session.commit()