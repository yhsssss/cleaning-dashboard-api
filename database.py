# database.py
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, Session
from models import Base

DB_URL = "sqlite:///./clinical.db"
# 운영 전환 시: "postgresql+psycopg2://user:pass@host/dbname"

engine = create_engine(
    DB_URL,
    connect_args={"check_same_thread": False},  # SQLite 전용
    echo=False,
)

# SQLite 외래키 활성화
@event.listens_for(engine, "connect")
def set_sqlite_pragma(conn, _):
    conn.execute("PRAGMA foreign_keys=ON")

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
