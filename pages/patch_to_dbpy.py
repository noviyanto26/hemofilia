# patch_to_dbpy.py
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).parent
TARGETS = [
    "1_identitas_organisasi.py",
    "2_jumlah_individu_hemofilia.py",
    "3_berdasarkan_kelompok_usia.py",
    "4_data_penyandang_vwd.py",
    "5_berdasarkan_jenis_kelamin.py",
    "6_tingkat_hemofilia_jenis_kelamin.py",
    "7_penyandang_hemofilia_anak_berat.py",
    "8_jumlah_penyandang_vwd_berat.py",
    "9_hemofilia_inhibitor.py",
    "10_pasien_nonfaktor.py",
]

def load(path: Path) -> str:
    return path.read_text(encoding="utf-8")

def save(path: Path, text: str):
    path.write_text(text, encoding="utf-8")

def ensure_backup(p: Path, backup_root: Path):
    backup_root.mkdir(parents=True, exist_ok=True)
    dst = backup_root / p.name
    shutil.copy2(p, dst)

def insert_after_imports(src: str, injection: str) -> str:
    lines = src.splitlines()
    insert_at = 0
    for i, line in enumerate(lines[:50]):  # cari import block di awal
        s = line.strip()
        if s.startswith("import ") or s.startswith("from "):
            insert_at = i + 1
    lines.insert(insert_at, injection)
    return "\n".join(lines)

def guard_init_db_for_pg(src: str) -> str:
    # Bungkus def init_db() agar skip di Postgres
    # Pola: def init_db(...): <body>
    pattern = r"(def\s+init_db\s*\([^\)]*\)\s*:\s*\n)([\s\S]+?)(?=\n\n|\n#|def\s|\Z)"
    def repl(m):
        head, body = m.group(1), m.group(2)
        guard = (
            head
            + "    # Skip DDL SQLite saat backend = Postgres\n"
              "    if IS_PG:\n"
              "        return\n"
        )
        # indent body ke 4 spasi jika belum
        body_lines = body.splitlines()
        body_lines = [("    " + l if not l.startswith("    ") else l) for l in body_lines]
        return guard + "\n".join(body_lines)
    return re.sub(pattern, repl, src, flags=re.MULTILINE)

def patch_file(p: Path):
    src = load(p)
    original = src

    # 0) Backup
    ensure_backup(p, ROOT / "backup_before_dbpy")

    # 1) pastikan helper db.py terimport + flag IS_PG
    if "from db import read_sql_df" not in src:
        inject = (
            "from db import read_sql_df, exec_sql, get_engine, ping\n"
            "from sqlalchemy import text\n"
            "from datetime import datetime\n"
            "IS_PG = (ping() == 'postgresql')\n"
        )
        src = insert_after_imports(src, inject)

    # 2) ganti pd.read_sql_query( ... , conn) -> read_sql_df("...", params)
    src = re.sub(
        r"pd\.read_sql_query\s*\(",
        "read_sql_df(",
        src
    )

    # 3) hapus def connect() lama berbasis sqlite3
    src = re.sub(
        r"\n\s*def\s+connect\s*\(\s*\)\s*:\s*\n\s*return\s+sqlite3\.connect\([^\)]*\)\s*\n",
        "\n",
        src
    )

    # 4) hilangkan import sqlite3 kalau sudah tidak dipakai
    if "sqlite3" in src:
        # biarkan, bisa saja masih dipakai untuk PRAGMA string; aman dibiarkan
        pass

    # 5) guard init_db (skip di Postgres)
    if "def init_db" in src and "IS_PG" in src:
        src = guard_init_db_for_pg(src)

    # 6) ubah pola cursor.execute paling umum menjadi exec_sql
    #    - INSERT/UPDATE/DELETE: conn.cursor().execute("SQL", params)
    #    - Commit tidak diperlukan (exec_sql pakai begin())
    src = re.sub(
        r"(\w+)\s*=\s*conn\.cursor\(\)\s*\n\s*\1\.execute\s*\(\s*f?([\"'])([\s\S]+?)\2\s*,\s*([^\n\)]+)\)\s*\n\s*conn\.commit\(\)",
        r"exec_sql(r\"\"\"\3\"\"\", params=\4)",
        src
    )
    src = re.sub(
        r"conn\.cursor\(\)\.execute\s*\(\s*f?([\"'])([\s\S]+?)\1\s*,\s*([^\n\)]+)\)\s*\n\s*conn\.commit\(\)",
        r"exec_sql(r\"\"\"\2\"\"\", params=\3)",
        src
    )

    # 7) tambahkan helper insert_row(payload...) → SQLAlchemy (jika pattern ada)
    #    (biarkan yang sudah benar; patcher ini fokus mengganti pola umum saja)

    if src != original:
        save(p, src)
        print(f"Patched: {p}")
    else:
        print(f"No change: {p}")

def main():
    for name in TARGETS:
        for candidate in (ROOT / name, ROOT / "pages" / name):
            if candidate.exists():
                patch_file(candidate)
            else:
                print(f"Skip (not found): {candidate}")

if __name__ == "__main__":
    main()
