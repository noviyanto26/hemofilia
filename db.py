# db.py
import os
import sys
import json
import socket
from contextlib import contextmanager
from urllib.parse import urlsplit, urlunsplit

from sqlalchemy import create_engine, text
import pandas as pd

# --- Config dasar ---
DEFAULT_SQLITE = os.getenv("SQLITE_PATH", "hemofilia.db")

# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------
def _read_secret(key: str, default=None):
    """Baca dari Streamlit secrets jika ada; fallback ke ENV."""
    try:
        import streamlit as st  # aman jika tak dipakai di CLI
        if "secrets" in dir(st) and key in st.secrets:
            return st.secrets[key]
    except Exception:
        pass
    return os.getenv(key, default)

def get_database_url() -> str:
    """
    Ambil URL DB:
    - Prioritas: st.secrets["DATABASE_URL"]
    - Fallback:   os.environ["DATABASE_URL"]
    - Jika tidak ada, pakai SQLite lokal.
    """
    url = (_read_secret("DATABASE_URL", "") or "").strip()
    if not url:
        return f"sqlite:///{DEFAULT_SQLITE}"
    return url

def _normalize_supabase_url(url: str) -> str:
    """
    Tambahkan parameter aman untuk koneksi publik Supabase jika belum ada:
    - sslmode=require
    - connect_timeout=10
    """
    if not url.startswith("postgresql"):
        return url
    if "sslmode=" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    if "connect_timeout=" not in url:
        url += ("&" if "?" in url else "?") + "connect_timeout=10"
    return url

def _validate_pooler_username_and_host(url: str) -> None:
    """
    Beri pesan jelas jika URL pooler salah format/host.
    Syarat Supabase Pooler:
      - host mengandung 'pooler.supabase.com'
      - username harus 'ROLE.<project_ref>' (contoh: postgres.okndwthzywdhkhsutioy)
    """
    parts = urlsplit(url)
    host = (parts.hostname or "").lower()
    user = parts.username or ""
    if "pooler.supabase.com" in host:
        if "." not in user:
            raise ValueError(
                "Supabase pooler memerlukan username 'ROLE.<project_ref>' "
                "(contoh: postgres.okndwthzywdhkhsutioy). Perbaiki DATABASE_URL."
            )
        # Hint kecil: beberapa proyek memakai aws-1, bukan aws-0
        if host.startswith("aws-0-"):
            print(
                "DB WARN: Host pooler menggunakan 'aws-0-'. "
                "Dashboard terbaru sering memakai 'aws-1-'. "
                "Pastikan host sesuai persis dengan yang ada di Dashboard.",
                file=sys.stderr,
            )

def _maybe_ipv4_connect_args(url: str) -> dict:
    """
    Opsional paksa IPv4 bila diperlukan.
    Aktifkan dengan ENV: PGFORCE_IPV4=1
    Implementasi: set 'hostaddr' ke A-record IPv4, sementara 'host'
    (di URL) tetap hostname untuk SNI/TLS.
    """
    if os.getenv("PGFORCE_IPV4", "").strip() != "1":
        return {}

    parts = urlsplit(url)
    host = parts.hostname or ""
    # Batasi hanya untuk host Supabase (pooler/direct)
    if not (host.endswith(".supabase.co") or "pooler.supabase.com" in host):
        return {}

    try:
        infos = socket.getaddrinfo(host, None, socket.AF_INET)
        if infos:
            ipv4 = infos[0][4][0]
            return {"hostaddr": ipv4}
    except Exception:
        pass
    return {}

def safe_url(url: str) -> str:
    """Mask password agar aman saat ditampilkan/logging."""
    try:
        parts = urlsplit(url)
        if parts.password:
            # Bangun ulang netloc dengan password dimask
            netloc = parts.hostname or ""
            if parts.username:
                netloc = f"{parts.username}:******@{netloc}"
            if parts.port:
                netloc += f":{parts.port}"
            return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))
        return url
    except Exception:
        return url

# ---------------------------------------------------------------------
# Engine & Context
# ---------------------------------------------------------------------
_ENGINE = None

def get_engine():
    """Inisialisasi dan cache SQLAlchemy Engine sesuai DATABASE_URL."""
    global _ENGINE
    if _ENGINE is not None:
        return _ENGINE

    url = get_database_url()
    if url.startswith("postgresql"):
        url = _normalize_supabase_url(url)
        _validate_pooler_username_and_host(url)
        connect_args = _maybe_ipv4_connect_args(url)

        _ENGINE = create_engine(
            url,
            pool_pre_ping=True,   # auto-cek koneksi sebelum dipakai
            pool_recycle=1800,    # recycle tiap 30 menit agar koneksi sehat
            future=True,
            connect_args=connect_args,  # kosong kecuali PGFORCE_IPV4=1
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

@contextmanager
def connect_ctx():
    """Context manager koneksi (preferred untuk eksekusi singkat)."""
    eng = get_engine()
    with eng.connect() as conn:
        yield conn

# ---------------------------------------------------------------------
# Health Check & Helpers
# ---------------------------------------------------------------------
def ping() -> dict:
    """Tes koneksi & kembalikan dict hasil (untuk UI/logging)."""
    try:
        eng = get_engine()
        with eng.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"ok": True, "url": safe_url(get_database_url())}
    except Exception as e:
        return {"ok": False, "url": safe_url(get_database_url()), "error": repr(e)}

def exec_sql(sql: str, params: dict | None = None):
    """
    Eksekusi SQL (DDL/DML) dengan optional params, auto-commit.
    Menggunakan transaction context (engine.begin()) agar TRUNCATE/CREATE/INSERT
    dan kawan-kawan benar-benar tersimpan.
    """
    eng = get_engine()
    with eng.begin() as conn:  # <- penting: auto-commit di akhir block
        return conn.execute(text(sql), params or {})

def read_sql_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    """Baca hasil query ke DataFrame (aman & logging ringkas jika error)."""
    try:
        with connect_ctx() as conn:
            return pd.read_sql_query(text(sql), conn, params=params or {})
    except Exception as e:
        info = {
            "where": "read_sql_df",
            "sql": sql.strip().splitlines()[0][:120] + "...",
            "db_url": safe_url(get_database_url()),
            "error": repr(e),
        }
        print("DB ERROR:", json.dumps(info, ensure_ascii=False), file=sys.stderr)
        raise

# --- Tambahan: alias nyaman agar kompatibel dengan halaman lain ---
def fetch_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    """
    Alias untuk read_sql_df(), sesuai kebutuhan halaman:
    from db import get_engine, exec_sql, fetch_df
    """
    return read_sql_df(sql, params=params)

def table_exists(table_name: str) -> bool:
    """Cek keberadaan tabel yang kompatibel untuk Postgres/SQLite."""
    # Coba cara Postgres (to_regclass)
    try:
        with connect_ctx() as conn:
            res = conn.execute(
                text("select to_regclass(:t) is not null as exists"),
                {"t": table_name},
            )
            row = res.fetchone()
            if row is not None:
                return bool(row[0])
    except Exception:
        pass

    # Fallback generic (SQLite & juga PG)
    try:
        with connect_ctx() as conn:
            _ = conn.execute(text(f"SELECT 1 FROM {table_name} LIMIT 1"))
            return True
    except Exception:
        return False

# ---------------------------------------------------------------------
# (Opsional) Dialect helpers yang berguna untuk kode halaman
# ---------------------------------------------------------------------
def is_postgres() -> bool:
    try:
        eng = get_engine()
        return (eng.dialect.name or "").lower() in ("postgresql", "postgres")
    except Exception:
        return False

def is_sqlite() -> bool:
    try:
        eng = get_engine()
        return (eng.dialect.name or "").lower() == "sqlite"
    except Exception:
        return False
