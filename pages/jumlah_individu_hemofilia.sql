PRAGMA foreign_keys=OFF;

-- 1. Buat tabel baru dengan tipe INTEGER pada kolom angka
CREATE TABLE IF NOT EXISTS jumlah_individu_hemofilia_new (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kode_organisasi TEXT NOT NULL,
    created_at TEXT NOT NULL,
    hemofilia_a INTEGER,
    hemofilia_b INTEGER,
    hemofilia_lain INTEGER,
    terduga INTEGER,
    vwd INTEGER,
    lainnya INTEGER,
    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
);

-- 2. Salin data sambil CAST kolom angka → INTEGER
INSERT INTO jumlah_individu_hemofilia_new (
    id, kode_organisasi, created_at,
    hemofilia_a, hemofilia_b, hemofilia_lain, terduga, vwd, lainnya
)
SELECT
    id,
    kode_organisasi,
    created_at,
    CAST(COALESCE(NULLIF(TRIM(hemofilia_a), ''), '0') AS INTEGER),
    CAST(COALESCE(NULLIF(TRIM(hemofilia_b), ''), '0') AS INTEGER),
    CAST(COALESCE(NULLIF(TRIM(hemofilia_lain), ''), '0') AS INTEGER),
    CAST(COALESCE(NULLIF(TRIM(terduga), ''), '0') AS INTEGER),
    CAST(COALESCE(NULLIF(TRIM(vwd), ''), '0') AS INTEGER),
    CAST(COALESCE(NULLIF(TRIM(lainnya), ''), '0') AS INTEGER)
FROM jumlah_individu_hemofilia;

-- 3. Tukar nama tabel
ALTER TABLE jumlah_individu_hemofilia RENAME TO jumlah_individu_hemofilia_backup;
ALTER TABLE jumlah_individu_hemofilia_new RENAME TO jumlah_individu_hemofilia;

PRAGMA foreign_keys=ON;

-- 4. (Opsional) Hapus backup jika sudah diverifikasi
-- DROP TABLE jumlah_individu_hemofilia_backup;
