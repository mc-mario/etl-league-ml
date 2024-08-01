from prefect.variables import Variable
from sqlalchemy import create_engine, Column, Integer, String, Boolean, DateTime, func
from sqlalchemy.orm import sessionmaker, declarative_base


data_path = Variable.get('data_path')
db_path = f"{data_path.value}/etl_status.db"
engine = create_engine(f'sqlite:///{db_path}')
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()
Base.metadata.create_all(engine)

class Match(Base):
    __tablename__ = 'matches'
    id = Column(Integer, primary_key=True, autoincrement=True)
    match_id = Column(String, unique=True, nullable=False)
    bronze = Column(Boolean, default=False)
    silver = Column(Boolean, default=False)
    gold = Column(Boolean, default=False)
    created_on = Column(DateTime, default=func.now())


def is_match_id_processed(match_id):
    return session.query(Match).filter_by(match_id=match_id).one_or_none() is None


def add_match_id(match_id, bronze=False, silver=False, gold=False):
    if is_match_id_processed(match_id):
        return
    new_match = Match(match_id=match_id, bronze=bronze, silver=silver, gold=gold)
    session.add(new_match)
    session.commit()
