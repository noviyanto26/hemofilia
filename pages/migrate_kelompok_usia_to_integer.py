import sqlite3

DB_PATH = "hemofilia.db"
TABLE = "kelompok_usia"
COLUMNS = [
    ("ha_ringan", "Hemofilia A - Ringan"),
    ("ha_sedang", "Hemofilia A - Sedang"),
    ("ha_berat",  "Hemofilia A - Berat"),
    ("hb_ringan", "Hemofilia B - Ringan"),
    ("hb_sedang", "Hemofilia B - Sedang"),
    ("hb_berat",  "Hemofilia B - Berat"),
    ("hemo_tipe_lain", "Hemofilia Tipe Lain"),
    ("vwd_tipe1", "vWD - Tipe 1"),
    ("vwd_tipe2", "vWD - Tipe 2"),
]

def main():
    cols_int = ", ".join([f"{name} INTEGER" for name, _ in COLUMNS])
    select_casts = ",\n              ".join(
        [f"CAST(COALESCE(NULLIF(TRIM({name}), ''), '0') AS INTEGER) AS {name}" for name, _ in COLUMNS]
    )
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    kelompok_usia TEXT,
                    {cols_int},
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
            cur.execute(f"""
                INSERT INTO {TABLE}_new (
                    id, kode_organisasi, created_at, kelompok_usia, {", ".join([n for n, _ in COLUMNS])}
                )
                SELECT
                    id,
                    kode_organisasi,
                    created_at,
                    kelompok_usia,
                    {select_casts}
                FROM {TABLE}
                ORDER BY id
            """)
            cur.execute(f"ALTER TABLE {TABLE} RENAME TO {TABLE}_backup")
            cur.execute(f"ALTER TABLE {TABLE}_new RENAME TO {TABLE}")
            conn.commit()
            print("Migrasi sukses. Backup disimpan sebagai:", f"{TABLE}_backup")
        except Exception as e:
            conn.rollback()
            print("Migrasi gagal:", e)
        finally:
            cur.execute("PRAGMA foreign_keys=ON")

if __name__ == "__main__":
    main()
