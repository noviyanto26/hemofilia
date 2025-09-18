import os
import io
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# =========================
# Konfigurasi & Konstanta
# =========================
st.set_page_config(page_title="Berdasarkan Kelompok Usia", page_icon="🩸", layout="wide")
st.title("🩸 Data Berdasarkan Kelompok Usia")

# Gunakan path absolut agar tidak salah file DB
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = str((BASE_DIR / "hemofilia.db").resolve())

TABLE = "kelompok_usia"

# Urutan kelompok usia untuk template & tampilan
AGE_GROUPS = ["0-4", "5-13", "14-18", "19-44", ">45", "Tidak ada data usia"]
TEMPLATE_AGE_ORDER = [">45", "19-44", "14-18", "5-13", "0-4"]

USIA_COLUMNS = [
    ("ha_ringan", "Hemofilia A - Ringan"),
    ("ha_sedang", "Hemofilia A - Sedang"),
    ("ha_berat",  "Hemofilia A - Berat"),
    ("hb_ringan", "Hemofilia B - Ringan"),
    ("hb_sedang", "Hemofilia B - Sedang"),
    ("hb_berat",  "Hemofilia B - Berat"),
    ("hemo_tipe_lain", "Hemofilia Tipe Lain"),
    ("vwd_tipe1", "vWD - Tipe 1"),
    ("vwd_tipe2", "vWD - Tipe 2"),
    ("vwd_tipe3", "vWD - Tipe 3"),  # ✅ kolom baru
]

# ========== Template unggah ==========
TEMPLATE_COLUMNS = ["HMHI cabang", "Kelompok Usia"] + [lbl for _, lbl in USIA_COLUMNS]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Kelompok Usia": "kelompok_usia",
    **{lbl: key for key, lbl in USIA_COLUMNS},
}

# =========================
# Utilitas Database
# =========================
def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def _has_column(conn, table, col):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any((row[1] == col) for row in cur.fetchall())

def _table_exists(conn, table):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

# =========================
# Migrasi Skema (add kode_organisasi jika perlu)
# =========================
def migrate_add_kode_organisasi_if_needed():
    with connect() as conn:
        if not _table_exists(conn, TABLE):
            return
        if _has_column(conn, TABLE, "kode_organisasi"):
            return

        st.warning("Migrasi skema Kelompok Usia: menambahkan kolom kode_organisasi...")
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            cols_sql = ", ".join([f"{n} INTEGER" for n, _ in USIA_COLUMNS])
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT,
                    created_at TEXT NOT NULL,
                    kelompok_usia TEXT,
                    {cols_sql},
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
            select_cols = "created_at, kelompok_usia, " + ", ".join([n for n, _ in USIA_COLUMNS])
            cur.execute(f"""
                INSERT INTO {TABLE}_new (
                    id, kode_organisasi, created_at, kelompok_usia, {", ".join([n for n, _ in USIA_COLUMNS])}
                )
                SELECT id, NULL as kode_organisasi, {select_cols}
                FROM {TABLE}
                ORDER BY id
            """)
            cur.execute(f"ALTER TABLE {TABLE} RENAME TO {TABLE}_backup")
            cur.execute(f"ALTER TABLE {TABLE}_new RENAME TO {TABLE}")
            conn.commit()
            st.success("Migrasi Kelompok Usia selesai. Tabel lama disimpan sebagai _backup.")
        except Exception as e:
            conn.rollback()
            st.error(f"Migrasi Kelompok Usia gagal: {e}")
        finally:
            cur.execute("PRAGMA foreign_keys=ON")

# =========================
# Inisialisasi Tabel Final
# =========================
def init_db():
    cols_sql = ", ".join([f"{n} INTEGER" for n, _ in USIA_COLUMNS])
    with connect() as conn:
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT,
                created_at TEXT NOT NULL,
                kelompok_usia TEXT,
                {cols_sql},
                FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
            )
        """)
        conn.commit()

# =========================
# Helper CRUD & Loader
# =========================
def load_hmhi_to_kode():
    """
    Ambil pilihan dari identitas_organisasi.hmhi_cabang dan petakan ke kode_organisasi.
    Return:
        mapping: dict {hmhi_cabang -> kode_organisasi}
        options: list hmhi_cabang (urut alfabet)
    """
    with connect() as conn:
        try:
            df = pd.read_sql_query(
                "SELECT kode_organisasi, hmhi_cabang FROM identitas_organisasi ORDER BY id DESC",
                conn
            )
            if df.empty:
                return {}, []
            mapping = {}
            for _, row in df.iterrows():
                hmhi_val = str(row["hmhi_cabang"]).strip() if pd.notna(row["hmhi_cabang"]) else ""
                kode_val = str(row["kode_organisasi"]).strip() if pd.notna(row["kode_organisasi"]) else ""
                if hmhi_val:
                    mapping[hmhi_val] = kode_val
            options = sorted(mapping.keys())
            return mapping, options
        except Exception:
            return {}, []

def insert_row(row: dict, kode_organisasi: str):
    with connect() as conn:
        c = conn.cursor()
        keys = list(row.keys())
        cols = ", ".join(keys)
        placeholders = ", ".join(["?"] * len(keys))
        vals = [row[k] for k in keys]
        c.execute(
            f"INSERT INTO {TABLE} (kode_organisasi, created_at, {cols}) VALUES (?, ?, {placeholders})",
            [kode_organisasi, datetime.utcnow().isoformat()] + vals
        )
        conn.commit()

def read_with_join(limit=300):
    with connect() as conn:
        if not _has_column(conn, TABLE, "kode_organisasi"):
            st.error("Kolom 'kode_organisasi' belum tersedia pada tabel Kelompok Usia. Coba refresh setelah migrasi.")
            return pd.read_sql_query(f"SELECT * FROM {TABLE} ORDER BY id DESC LIMIT ?", conn, params=[limit])

        return pd.read_sql_query(
            f"""
            SELECT
              ku.id, ku.kode_organisasi, ku.created_at, ku.kelompok_usia,
              {", ".join([f"ku.{n}" for n, _ in USIA_COLUMNS])},
              io.kota_cakupan_cabang,
              io.hmhi_cabang
            FROM {TABLE} ku
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = ku.kode_organisasi
            ORDER BY ku.id DESC
            LIMIT ?
            """,
            conn, params=[limit]
        )

def to_nonneg_int(x) -> int:
    """Konversi ke int >=0; kosong/NaN -> 0, negatif -> 0."""
    try:
        if pd.isna(x) or str(x).strip() == "":
            return 0
        v = int(float(x))
        return max(v, 0)
    except Exception:
        return 0

# =========================
# Startup
# =========================
migrate_add_kode_organisasi_if_needed()
init_db()

# =========================
# Antarmuka
# =========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # Pilihan berdasarkan hmhi_cabang → dipetakan ke kode_organisasi
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom hmhi_cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="usia::hmhi_select"
        )

    # Editor default (baris = kelompok usia)
    df_default = pd.DataFrame(0, index=AGE_GROUPS, columns=[n for n, _ in USIA_COLUMNS])
    df_default.index.name = "Kelompok Usia"

    col_cfg = {n: st.column_config.NumberColumn(label=lbl, min_value=0, step=1) for n, lbl in USIA_COLUMNS}

    with st.form("usia::form"):
        edited = st.data_editor(
            df_default,
            key="usia::editor",
            column_config=col_cfg,
            use_container_width=True,
            num_rows="fixed",
        )
        save = st.form_submit_button("💾 Simpan Semua Baris")

    if save:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                rows_inserted = 0
                rows_skipped_all_zero = 0

                for usia, row in edited.iterrows():
                    payload = {"kelompok_usia": str(usia)}
                    nums = {}
                    for n, _ in USIA_COLUMNS:
                        val = row.get(n, 0)
                        num = pd.to_numeric(val, errors="coerce")
                        if pd.isna(num):
                            num = 0
                        try:
                            nums[n] = int(num)
                        except Exception:
                            nums[n] = 0

                    # Hanya simpan jika ada nilai ≠ 0
                    if all(v == 0 for v in nums.values()):
                        rows_skipped_all_zero += 1
                        continue

                    payload.update(nums)
                    insert_row(payload, kode_organisasi)
                    rows_inserted += 1

                # Verifikasi jumlah baris untuk kode_organisasi ini
                with connect() as conn_chk:
                    cnt = pd.read_sql_query(
                        f"SELECT COUNT(*) AS n FROM {TABLE} WHERE kode_organisasi = ?",
                        conn_chk, params=[kode_organisasi]
                    )["n"].iloc[0]

                st.success(
                    f"{rows_inserted} baris disimpan untuk **{selected_hmhi}** "
                    f"(dilewati {rows_skipped_all_zero} baris karena semua kolom bernilai 0). "
                    f"Total baris untuk kode ini sekarang: {cnt}."
                )

with tab_data:
    st.subheader("📄 Data Tersimpan")
    df_x = read_with_join()

    # ===== Unduh Template Excel (kolom lengkap, baris usia sesuai urutan yang diminta) =====
    st.caption("Gunakan template berikut saat mengunggah data (kolom & urutan baris Kelompok Usia disarankan).")
    tmpl_records = []
    for usia in TEMPLATE_AGE_ORDER:
        row = {"HMHI cabang": "", "Kelompok Usia": usia}
        for _key, lbl in USIA_COLUMNS:
            row[lbl] = 0
        tmpl_records.append(row)
    tmpl_df = pd.DataFrame(tmpl_records, columns=TEMPLATE_COLUMNS)

    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_kelompok_usia.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="usia::dl_template"
    )

    # ===== Tabel tampil (SEMBUNYIKAN Kota/Provinsi & Created At) =====
    if df_x.empty:
        st.info("Belum ada data.")
    else:
        order = (
            ["hmhi_cabang", "kelompok_usia"]  # tampilkan HMHI & Kelompok Usia
            + [n for n, _ in USIA_COLUMNS]    # semua metrik (termasuk vwd_tipe3)
            # sengaja tidak memasukkan: "kota_cakupan_cabang", "created_at"
        )
        order = [c for c in order if c in df_x.columns]

        nice = {n: lbl for n, lbl in USIA_COLUMNS}
        view = df_x[order].rename(columns={
            **nice,
            "hmhi_cabang": "HMHI cabang",
            "kelompok_usia": "Kelompok Usia",
        })

        st.dataframe(view, use_container_width=True)

        # Unduh Excel data tampilan sekarang (opsional)
        buf_now = io.BytesIO()
        with pd.ExcelWriter(buf_now, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="KelompokUsia")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buf_now.getvalue(),
            file_name="kelompok_usia.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="usia::download"
        )

    # ===== Unggah Excel =====
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan header persis seperti template",
        type=["xlsx"],
        key="usia::uploader"
    )

    if up is not None:
        try:
            raw = pd.read_excel(up)
            # Normalisasi header: trim spasi
            raw.columns = [str(c).strip() for c in raw.columns]
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        # Validasi header
        missing = [c for c in TEMPLATE_COLUMNS if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Kolom yang belum ada: " + ", ".join(missing))
            st.stop()

        # alias -> key internal
        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        # Pratinjau
        st.caption("Pratinjau 20 baris pertama dari file yang diunggah:")
        st.dataframe(raw.head(20), use_container_width=True)

        if st.button("🚀 Proses & Simpan", type="primary", key="usia::process"):
            # ⛳️ PENTING: UNPACK hasil fungsi agar hmhi_map berupa dict, bukan tuple
            hmhi_map, _ = load_hmhi_to_kode()
            results = []

            # ✅ iterasi berbasis indeks → setiap baris via iloc[i].to_dict()
            nrows = len(df_up)
            for i in range(nrows):
                try:
                    s = df_up.iloc[i]            # pandas.Series
                    # HMHI (wajib)
                    hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                    if not hmhi:
                        raise ValueError("Kolom 'HMHI cabang' kosong.")

                    kode_organisasi = hmhi_map.get(hmhi)
                    if not kode_organisasi:
                        raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                    # Normalisasi nilai numerik
                    payload = {}
                    for key, _lbl in USIA_COLUMNS:
                        val = s.get(key)
                        try:
                            if pd.isna(val) or str(val).strip() == "":
                                payload[key] = 0
                            else:
                                payload[key] = max(int(float(val)), 0)
                        except Exception:
                            payload[key] = 0

                    # Kelompok Usia (wajib)
                    kelompok = str((s.get("kelompok_usia") or "")).strip()
                    if not kelompok:
                        raise ValueError("Kolom 'Kelompok Usia' kosong.")
                    payload["kelompok_usia"] = kelompok

                    insert_row(payload, kode_organisasi)
                    results.append({"Baris": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {kelompok}"})
                except Exception as e:
                    results.append({"Baris": i + 2, "Status": "GAGAL", "Keterangan": str(e)})

            res_df = pd.DataFrame(results)
            st.write("**Hasil unggah:**")
            st.dataframe(res_df, use_container_width=True)

            # Ringkasan & unduh log
            ok = (res_df["Status"] == "OK").sum()
            fail = (res_df["Status"] == "GAGAL").sum()
            if ok:
                st.success(f"Berhasil menyimpan {ok} baris.")
            if fail:
                st.error(f"Gagal menyimpan {fail} baris.")

            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                res_df.to_excel(w, index=False, sheet_name="Hasil")
            st.download_button(
                "📄 Unduh Log Hasil",
                log_buf.getvalue(),
                file_name="log_hasil_unggah_kelompok_usia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="usia::dl_log"
            )
