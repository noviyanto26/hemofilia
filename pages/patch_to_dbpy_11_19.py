# patch_to_dbpy_11_19.py
import re
import shutil
from pathlib import Path

ROOT = Path(__file__).parent
TARGETS = [
    "11_rs_penangan_hemofilia.py",
    "12_replacement_therapy.py",
    "13_penanganan_kesehatan.py",
    "14_hemo_berat_prophylaxis_usia.py",
    "15_perkembangan_pelayanan_penanganan.py",
    "16_informasi_donasi.py",
    "17_infeksi_transfusi_darah.py",
    "18_kematian_hemofilia_2024_sekarang.py",
    "19_rekap_hemofilia.py",
]

def load(p: Path) -> str:
    return p.read_text(encoding="utf-8")

def save(p: Path, text: str):
    p.write_text(text, encoding="utf-8")

def ensure_backup(p: Path, backup_root: Path):
    backup_root.mkdir(parents=True, exist_ok=True)
    shutil.copy2(p, backup_root / p.name)

def insert_after_imports(src: str, injection: str) -> str:
    lines = src.splitlines()
    insert_at = 0
    for i, line in enumerate(lines[:80]):
        s = line.strip()
        if s.startswith("import ") or s.startswith("from "):
            insert_at = i + 1
    lines.insert(insert_at, injection)
    return "\n".join(lines)

def guard_init_and_migrate_for_pg(src: str) -> str:
    # Bungkus def init_db(...) dan def migrate_if_needed(...) agar skip di Postgres
    for func in ("init_db", "migrate_if_needed", "ensure_rumah_sakit_schema"):
        pattern = rf"(def\s+{func}\s*\([^\)]*\)\s*:\s*\n)([\s\S]+?)(?=\n\n|\n#|def\s|\Z)"
        def repl(m):
            head, body = m.group(1), m.group(2)
            guard = head + "    # Skip pada Postgres (DDL/migrasi khusus SQLite)\n    if IS_PG:\n        return\n"
            body_lines = body.splitlines()
            body_lines = [("    " + l if not l.startswith("    ") else l) for l in body_lines]
            return guard + "\n".join(body_lines)
        src = re.sub(pattern, repl, src, flags=re.MULTILINE)
    return src

def patch_sqlite_connects(src: str) -> str:
    # Hapus def connect() yang mengembalikan sqlite3.connect(...)
    src = re.sub(
        r"\n\s*def\s+connect\s*\(\s*\)\s*:\s*\n\s*return\s+sqlite3\.connect\([^\)]*\)\s*\n",
        "\n",
        src
    )
    # Ganti pd.read_sql_query( ... , conn[,...]) -> read_sql_df("...", params=?)
    src = re.sub(r"pd\.read_sql_query\s*\(", "read_sql_df(", src)
    return src

def patch_cursor_exec(src: str) -> str:
    # Pola INSERT/UPDATE/DELETE yang umum: cursor.execute("SQL", params) + conn.commit()
    # 1) blok dengan variabel cursor
    src = re.sub(
        r"(\w+)\s*=\s*conn\.cursor\(\)\s*\n\s*\1\.execute\s*\(\s*f?([\"'])([\s\S]+?)\2\s*,\s*([^\n\)]+)\)\s*\n\s*conn\.commit\(\)",
        r"exec_sql(r\"\"\"\3\"\"\", params=\4)",
        src
    )
    # 2) single line tanpa var
    src = re.sub(
        r"conn\.cursor\(\)\.execute\s*\(\s*f?([\"'])([\s\S]+?)\1\s*,\s*([^\n\)]+)\)\s*\n\s*conn\.commit\(\)",
        r"exec_sql(r\"\"\"\2\"\"\", params=\3)",
        src
    )
    # 3) CREATE/ALTER/DDL tanpa params → exec_sql("...") saja
    src = re.sub(
        r"(\w+)\s*=\s*conn\.cursor\(\)\s*\n\s*\1\.execute\s*\(\s*f?([\"'])([\s\S]+?)\2\s*\)\s*\n\s*conn\.commit\(\)",
        r"exec_sql(r\"\"\"\3\"\"\")",
        src
    )
    src = re.sub(
        r"conn\.cursor\(\)\.execute\s*\(\s*f?([\"'])([\s\S]+?)\1\s*\)\s*\n\s*conn\.commit\(\)",
        r"exec_sql(r\"\"\"\2\"\"\")",
        src
    )
    return src

def patch_imports_and_flags(src: str) -> str:
    # Tambah import helper + IS_PG jika belum ada
    if "from db import read_sql_df" not in src:
        inj = (
            "from db import read_sql_df, exec_sql, get_engine, ping\n"
            "from sqlalchemy import text\n"
            "IS_PG = (ping() == 'postgresql')\n"
        )
        src = insert_after_imports(src, inj)
    return src

def patch_file(p: Path):
    src = load(p)
    original = src

    # 0) imports & flags
    src = patch_imports_and_flags(src)

    # 1) sqlite connect & read_sql_query
    src = patch_sqlite_connects(src)

    # 2) guard DDL/migrasi untuk Postgres
    src = guard_init_and_migrate_for_pg(src)

    # 3) translate execute(...) → exec_sql(...)
    src = patch_cursor_exec(src)

    # 4) khusus file 19_rekap_hemofilia.py: hilangkan hardcoded path DB dan gunakan engine
    if p.name == "19_rekap_hemofilia.py":
        # Buang blok deteksi DB_PATH SQLite; gunakan read_sql_df langsung
        src = re.sub(
            r"# ======================== Lokasi DB[\s\S]+?# ======================== Util DB",
            "# ======================== Koneksi via db.py ========================\n",
            src
        )
        # Hilangkan def connect/list_tables/read_table yang pakai sqlite
        src = re.sub(r"\n\s*def\s+connect\s*\([\s\S]+?\n\s*return\s+conn\s*\n", "\n", src)
        # list_tables() → query via read_sql_df
        src = re.sub(
            r"def\s+list_tables\s*\(\)\s*:\s*\n\s*with\s+connect\(\)\s+as\s+conn:\s*\n\s*q\s*=\s*\"\"\".*?\"\"\"\s*\n\s*return\s*\[.*?\]\s*",
            "def list_tables():\n    df = read_sql_df(\"SELECT table_name AS name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog','information_schema')\")\n    return df['name'].tolist() if not df.empty else []",
            src, flags=re.DOTALL
        )
        # read_table(table) → read_sql_df(f"SELECT * FROM {table}")
        src = re.sub(
            r"def\s+read_table\s*\(\s*table:\s*str\s*\)\s*->\s*pd\.DataFrame\s*:\s*\n\s*with\s+connect\(\)\s+as\s+conn:\s*\n\s*try:\s*\n\s*return\s+pd\.read_sql_query\(f\"SELECT \* FROM \{table\}\",\s*conn\)\s*\n\s*except Exception:\s*\n\s*return\s+pd\.DataFrame\(\)\s*",
            "def read_table(table: str) -> pd.DataFrame:\n    try:\n        return read_sql_df(f\"SELECT * FROM {table}\")\n    except Exception:\n        return pd.DataFrame()",
            src
        )

    if src != original:
        save(p, src)
        print(f"Patched: {p.name}")
    else:
        print(f"No change: {p.name}")

def main():
    backup_root = ROOT / "backup_before_dbpy_11_19"
    for name in TARGETS:
        for candidate in (ROOT / name, ROOT / "pages" / name):
            if candidate.exists():
                ensure_backup(candidate, backup_root)
                patch_file(candidate)
            else:
                print(f"Skip (not found): {candidate}")

if __name__ == "__main__":
    main()
