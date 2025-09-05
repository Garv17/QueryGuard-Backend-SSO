from __future__ import annotations

import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase

# DB_CONFIG = {
#     "host": "ep-morning-lake-aetbhd21-pooler.c-2.us-east-2.aws.neon.tech",
#     "port": 5432,
#     "user": "neondb_owner",
#     "password": "npg_fjJlOyZh95oE",
#     "database": "neondb",
# }

# Build the connection URL
# DATABASE_URL = (
#     f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
#     f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
# )

from dotenv import load_dotenv
load_dotenv() 

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


class Base(DeclarativeBase):
    pass


def init_db() -> None:
    # Import models to register metadata before create_all
    from .utils import models  # noqa: F401
    Base.metadata.create_all(bind=engine)


# Dependency for FastAPI routes
from typing import Generator

def get_db() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

