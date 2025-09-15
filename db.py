# db.py
import os
import sys
import json
from contextlib import contextmanager
from sqlalchemy import create_engine, text
import pandas as pd

# --- Config dasar ---
DEFAULT_SQLITE = os.getenv("SQLITE_PATH", "hemofilia.db")

def _read_secret(key: str, default=None):
    """Baca dari Streamlit secrets jika ada, kalau tidak ke ENV."""
    try:
        import streamlit as st  # aman jika tak dipakai di CLI
        if "secrets" in dir(st) and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)

def get_database_url():
    """
    Ambil URL DB:
    - Prioritas: st.secrets["DATABASE_URL"]
    - Fallback:   os.environ["DATABASE_URL"]
    - Jika tidak ada, pakai SQLite lokal.
    """
    url = _read_secret("DATABASE_URL", "").strip()
    if not url:
        return f"sqlite:///{DEFAULT_SQLITE}"
    return url

def _normalize_supabase_url(url: str) -> str:
    """
    Pastikan ada sslmode & connect_timeout untuk Supabase/host publik.
    Tambah param hanya jika belum ada.
    """
    if not url.startswith("postgresql"):
        return url
    # tambahkan parameter jika belum ada
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    if "connect_timeout=" not in url:
        url += ("&" if "?" in url else "?") + "connect_timeout=10"
    return url

_ENGINE = None

def get_engine():
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    url = get_database_url()
    if url.startswith("postgresql"):
        url = _normalize_supabase_url(url)
        # psycopg/psycopg2: pastikan paket terpasang di requirements
        _ENGINE = create_engine(
            url,
            pool_pre_ping=True,
            pool_recycle=1800,
            future=True,
        )
    else:
        # SQLite fallback
        _ENGINE = create_engine(
            url,
            connect_args={"check_same_thread": False},
            pool_pre_ping=True,
            future=True,
        )
    return _ENGINE

def ping():
    """Tes koneksi & kembalikan dict hasil untuk ditampilkan/logging."""
    try:
        eng = get_engine()
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"ok": True, "url": safe_url(get_database_url())}
    except Exception as e:
        return {"ok": False, "url": safe_url(get_database_url()), "error": repr(e)}

def safe_url(url: str) -> str:
    """Mask password agar aman saat ditampilkan."""
    # postgresql://user:pass@host:port/db?...
    try:
        from urllib.parse import urlsplit, urlunsplit
        parts = urlsplit(url)
        if parts.password:
            netloc = parts.hostname or ""
            if parts.username:
                netloc = f"{parts.username}:******@{netloc}"
            if parts.port:
                netloc += f":{parts.port}"
            return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
        return url
    except Exception:
        return url

@contextmanager
def connect_ctx():
    eng = get_engine()
    with eng.connect() as conn:
        yield conn

def exec_sql(sql: str, params: dict | None = None):
    with connect_ctx() as conn:
        return conn.execute(text(sql), params or {})

def read_sql_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    try:
        with connect_ctx() as conn:
            return pd.read_sql_query(text(sql), conn, params=params or {})
    except Exception as e:
        # Beri error yang jelas di UI/log
        info = {
            "where": "read_sql_df",
            "sql": sql.strip().splitlines()[0][:120] + "...",
            "db_url": safe_url(get_database_url()),
            "error": repr(e),
        }
        print("DB ERROR:", json.dumps(info, ensure_ascii=False), file=sys.stderr)
        raise

def table_exists(table_name: str) -> bool:
    try:
        with connect_ctx() as conn:
            res = conn.execute(
                text("select to_regclass(:t) is not null as exists"),
                {"t": table_name},
            )
            row = res.fetchone()
            if row is None:
                return False
            return bool(row[0])
    except Exception:
        # fallback generic (works for SQLite & PG)
        try:
            with connect_ctx() as conn:
                res = conn.execute(
                    text("SELECT 1 FROM " + table_name + " LIMIT 1")
                )
                _ = res.fetchone()
                return True
        except Exception:
            return False
