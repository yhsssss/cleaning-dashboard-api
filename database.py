# database.py
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from models import Base

DB_URL = "postgresql+psycopg2://clinical_user:clinical_pass@localhost:5432/clinical_db"
# 운영 전환 시: "postgresql+psycopg2://user:pass@host/dbname"

engine = create_engine(
    DB_URL,
    echo=False,
)


SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def init_db():
    """테이블 생성 (없는 경우에만)."""
    Base.metadata.create_all(engine)


def get_db():
    """FastAPI Depends용 세션 제너레이터."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
