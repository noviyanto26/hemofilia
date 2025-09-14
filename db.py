# db.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

DEFAULT_SQLITE = "data/hemofilia.db"

def get_engine():
    url = (os.getenv("DATABASE_URL") or "").strip()
    if url:
        # postgresql://USER:PASS@HOST:PORT/DBNAME
        return create_engine(url, pool_pre_ping=True)
    # fallback: SQLite lokal
    return create_engine(
        f"sqlite:///{DEFAULT_SQLITE}",
        connect_args={"check_same_thread": False},
        poolclass=NullPool,
    )

def read_sql_df(sql: str, params=None):
    eng = get_engine()
    with eng.connect() as conn:
        return pd.read_sql(sql, conn, params=params or {})

def exec_sql(sql: str, params=None, many: bool = False):
    eng = get_engine()
    with eng.begin() as conn:
        if many and isinstance(params, list):
            for p in params:
                conn.execute(text(sql), p or {})
        else:
            conn.execute(text(sql), params or {})

def ping() -> str:
    eng = get_engine()
    return eng.dialect.name  # 'postgresql' atau 'sqlite'
