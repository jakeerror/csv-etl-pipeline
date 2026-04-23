import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/etl_db"
)

engine = create_engine(DATABASE_URL, echo=False)
SessionLocal = sessionmaker(bind=engine)


class Base(DeclarativeBase):
    pass


def init_db():
    from src.db.models import Sale
    Base.metadata.create_all(bind=engine)


def get_session():
    return SessionLocal()
