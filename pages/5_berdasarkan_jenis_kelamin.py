import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

st.set_page_config(page_title="Berdasarkan Jenis Kelamin", page_icon="🩸", layout="wide")
st.title("🩸 Data Berdasarkan Jenis Kelamin per Kelainan")

DB_PATH = "hemofilia.db"
TABLE = "gender_per_kelainan"

KELAINAN_LIST = [
    "Hemofilia A",
    "Hemofilia B",
    "Hemofilia tipe lain/tidak dikenal",
    "Terduga Hemofilia/diagnosis belum ditegakkan",
    "VWD",
    "Kelainan pembekuan darah lain",
]
GENDER_COLS = [
    ("laki_laki", "Laki-laki"),
    ("perempuan", "Perempuan"),
    ("tidak_ada_data_gender", "Tidak ada data gender"),
]
TOTAL_COL = "total"
TOTAL_ROW_LABEL = "Total"

# ===== Template unggah & alias =====
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Kelainan",
    "Laki-laki",
    "Perempuan",
    "Tidak ada data gender",
    "Total",  # boleh kosong; akan dihitung jika kosong
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Kelainan": "kelainan",
    "Laki-laki": "laki_laki",
    "Perempuan": "perempuan",
    "Tidak ada data gender": "tidak_ada_data_gender",
    "Total": "total",
}

# ========== Util DB ==========
def connect():
    return sqlite3.connect(DB_PATH)

def _has_column(conn, table, col):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any((row[1] == col) for row in cur.fetchall())

def _table_exists(conn, table):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

# ========== Migrasi: tambah kolom kode_organisasi jika belum ada ==========
def migrate_add_kode_organisasi_if_needed():
    with connect() as conn:
        if not _table_exists(conn, TABLE):
            return
        if _has_column(conn, TABLE, "kode_organisasi"):
            return

        st.warning("Migrasi skema Jenis Kelamin: menambahkan kolom kode_organisasi...")
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            cols_sql = (
                "kelainan TEXT, "
                + ", ".join([f"{k} INTEGER" for k, _ in GENDER_COLS])
                + f", {TOTAL_COL} INTEGER, is_total_row TEXT"
            )
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT,
                    created_at TEXT NOT NULL,
                    {cols_sql},
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
            select_cols = "created_at, kelainan, " + ", ".join([k for k, _ in GENDER_COLS]) + f", {TOTAL_COL}, is_total_row"
            cur.execute(f"""
                INSERT INTO {TABLE}_new (id, kode_organisasi, {select_cols})
                SELECT id, NULL as kode_organisasi, {select_cols}
                FROM {TABLE}
                ORDER BY id
            """)
            cur.execute(f"ALTER TABLE {TABLE} RENAME TO {TABLE}_backup")
            cur.execute(f"ALTER TABLE {TABLE}_new RENAME TO {TABLE}")
            conn.commit()
            st.success("Migrasi Jenis Kelamin selesai. Tabel lama disimpan sebagai _backup.")
        except Exception as e:
            conn.rollback()
            st.error(f"Migrasi Jenis Kelamin gagal: {e}")
        finally:
            cur.execute("PRAGMA foreign_keys=ON")

# ========== Init tabel (skema final) ==========
def init_db():
    cols_sql = (
        "kelainan TEXT, "
        + ", ".join([f"{k} INTEGER" for k, _ in GENDER_COLS])
        + f", {TOTAL_COL} INTEGER, is_total_row TEXT"
    )
    with connect() as conn:
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT,
                created_at TEXT NOT NULL,
                {cols_sql},
                FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
            )
        """)
        conn.commit()

# ========== Helpers ==========
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

def insert_row(payload: dict, kode_organisasi: str):
    with connect() as conn:
        c = conn.cursor()
        cols = ", ".join(payload.keys())
        placeholders = ", ".join(["?"] * len(payload))
        vals = [payload[k] for k in payload]
        c.execute(
            f"INSERT INTO {TABLE} (kode_organisasi, created_at, {cols}) VALUES (?, ?, {placeholders})",
            [kode_organisasi, datetime.utcnow().isoformat()] + vals
        )
        conn.commit()

def read_with_kota(limit=300):
    with connect() as conn:
        if not _has_column(conn, TABLE, "kode_organisasi"):
            st.error("Kolom 'kode_organisasi' belum tersedia pada tabel Jenis Kelamin. Coba refresh setelah migrasi.")
            return pd.read_sql_query(f"SELECT * FROM {TABLE} ORDER BY id DESC LIMIT ?", conn, params=[limit])

        return pd.read_sql_query(
            f"""
            SELECT
              g.id, g.kode_organisasi, g.created_at, g.kelainan,
              {", ".join([f"g.{k}" for k, _ in GENDER_COLS])},
              g.{TOTAL_COL}, g.is_total_row,
              io.hmhi_cabang, io.kota_cakupan_cabang
            FROM {TABLE} g
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = g.kode_organisasi
            ORDER BY g.id DESC
            LIMIT ?
            """, conn, params=[limit]
        )

def _safe_int(v):
    try:
        x = pd.to_numeric(v, errors="coerce")
        if pd.isna(x):
            return 0
        return int(x)
    except Exception:
        return 0

# ========== Startup ==========
migrate_add_kode_organisasi_if_needed()
init_db()

# ========== UI ==========
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    st.caption("Isi gender untuk tiap Kelainan. **Total per baris** & **baris Total** dihitung otomatis.")

    # 🔁 Ambil dari HMHI cabang → mapping ke kode_organisasi
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="jk::hmhi_select"
        )

    df_default = pd.DataFrame(
        0,
        index=KELAINAN_LIST + [TOTAL_ROW_LABEL],
        columns=[k for k, _ in GENDER_COLS] + [TOTAL_COL],
    )
    df_default.index.name = "Kelainan"

    col_cfg = {k: st.column_config.NumberColumn(label=lbl, min_value=0, step=1) for k, lbl in GENDER_COLS}
    col_cfg[TOTAL_COL] = st.column_config.NumberColumn(label="Total", min_value=0, step=1, disabled=True)

    with st.form("jk::editor_form"):
        edited = st.data_editor(
            df_default,
            key="jk::editor",
            column_config=col_cfg,
            use_container_width=True,
            num_rows="fixed",
        )

        # total per baris
        for kel in KELAINAN_LIST:
            a = _safe_int(edited.loc[kel, "laki_laki"])
            b = _safe_int(edited.loc[kel, "perempuan"])
            c = _safe_int(edited.loc[kel, "tidak_ada_data_gender"])
            edited.loc[kel, TOTAL_COL] = a + b + c

        # baris total (agregasi)
        edited.loc[TOTAL_ROW_LABEL, "laki_laki"] = sum(_safe_int(edited.loc[k, "laki_laki"]) for k in KELAINAN_LIST)
        edited.loc[TOTAL_ROW_LABEL, "perempuan"] = sum(_safe_int(edited.loc[k, "perempuan"]) for k in KELAINAN_LIST)
        edited.loc[TOTAL_ROW_LABEL, "tidak_ada_data_gender"] = sum(_safe_int(edited.loc[k, "tidak_ada_data_gender"]) for k in KELAINAN_LIST)
        edited.loc[TOTAL_ROW_LABEL, TOTAL_COL] = sum(_safe_int(edited.loc[k, TOTAL_COL]) for k in KELAINAN_LIST)

        submitted = st.form_submit_button("💾 Simpan")

    if submitted:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                total_bawah = _safe_int(edited.loc[TOTAL_ROW_LABEL, TOTAL_COL])
                total_atas = sum(_safe_int(edited.loc[k, TOTAL_COL]) for k in KELAINAN_LIST)
                if total_bawah != total_atas:
                    st.error("Konsistensi gagal: Total baris bawah ≠ jumlah total 6 baris.")
                elif total_atas == 0:
                    st.error("Semua nilai 0. Mohon isi setidaknya satu nilai > 0.")
                else:
                    for kel in KELAINAN_LIST + [TOTAL_ROW_LABEL]:
                        payload = {
                            "kelainan": str(kel),
                            "laki_laki": _safe_int(edited.loc[kel, "laki_laki"]),
                            "perempuan": _safe_int(edited.loc[kel, "perempuan"]),
                            "tidak_ada_data_gender": _safe_int(edited.loc[kel, "tidak_ada_data_gender"]),
                            TOTAL_COL: _safe_int(edited.loc[kel, TOTAL_COL]),
                            "is_total_row": "1" if kel == TOTAL_ROW_LABEL else "0",
                        }
                        insert_row(payload, kode_organisasi)
                    st.success(f"Data berhasil disimpan untuk **{selected_hmhi}**.")

with tab_data:
    st.subheader("📄 Data Tersimpan")
    df_x = read_with_kota(limit=300)

    # ===== Unduh Template Excel =====
    st.caption("Gunakan template berikut saat mengunggah data (kolom sesuai contoh).")
    tmpl_records = []
    # Template default: baris 6 kelainan + 1 baris Total (boleh dihapus jika tidak ingin mengunggah baris Total)
    for kel in KELAINAN_LIST + [TOTAL_ROW_LABEL]:
        tmpl_records.append({
            "HMHI cabang": "",
            "Kelainan": kel,
            "Laki-laki": 0,
            "Perempuan": 0,
            "Tidak ada data gender": 0,
            "Total": 0,
        })
    tmpl_df = pd.DataFrame(tmpl_records, columns=TEMPLATE_COLUMNS)

    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_jk_per_kelainan.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="jk::dl_template"
    )

    # ===== Tabel tampil =====
    if df_x.empty:
        st.info("Belum ada data.")
    else:
        # Versi lengkap (untuk unduh Excel)
        full_df = df_x.rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "kelainan": "Kelainan",
            "laki_laki": "Laki-laki",
            "perempuan": "Perempuan",
            "tidak_ada_data_gender": "Tidak ada data gender",
            "total": "Total"
        })

        # === TAMPILAN LAYAR: SEMBUNYIKAN "Kota/Provinsi Cakupan Cabang" & "Created At"
        screen_cols = [
            "HMHI cabang",
            "Kelainan",
            "Laki-laki",
            "Perempuan",
            "Tidak ada data gender",
            "Total",
        ]
        screen_cols = [c for c in screen_cols if c in full_df.columns]
        st.dataframe(full_df[screen_cols], use_container_width=True)

        # === UNDUH EXCEL: tetap lengkap (termasuk Created At & Kota/Provinsi)
        export_cols = [
            "HMHI cabang",
            "Kota/Provinsi Cakupan Cabang",
            "Created At",
            "Kelainan",
            "Laki-laki",
            "Perempuan",
            "Tidak ada data gender",
            "Total",
        ]
        export_cols = [c for c in export_cols if c in full_df.columns]

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as w:
            full_df[export_cols].to_excel(w, index=False, sheet_name="JenisKelamin")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buffer.getvalue(),
            file_name="berdasarkan_jenis_kelamin.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="jk::download"
        )

    # ===== Unggah Excel =====
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan header persis seperti template",
        type=["xlsx"],
        key="jk::uploader"
    )

    if up is not None:
        try:
            raw = pd.read_excel(up)
            raw.columns = [str(c).strip() for c in raw.columns]
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        # Validasi header
        missing = [c for c in TEMPLATE_COLUMNS if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Kolom yang belum ada: " + ", ".join(missing))
            st.stop()

        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        # Pratinjau
        st.caption("Pratinjau 20 baris pertama dari file yang diunggah:")
        st.dataframe(raw.head(20), use_container_width=True)

        if st.button("🚀 Proses & Simpan", type="primary", key="jk::process"):
            hmhi_map, _ = load_hmhi_to_kode()
            results = []

            nrows = len(df_up)
            for i in range(nrows):
                try:
                    s = df_up.iloc[i]  # pandas.Series
                    hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                    if not hmhi:
                        raise ValueError("Kolom 'HMHI cabang' kosong.")
                    kode_organisasi = hmhi_map.get(hmhi)
                    if not kode_organisasi:
                        raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                    kel = str((s.get("kelainan") or "")).strip()
                    if not kel:
                        raise ValueError("Kolom 'Kelainan' kosong.")

                    lk = _safe_int(s.get("laki_laki"))
                    pr = _safe_int(s.get("perempuan"))
                    nd = _safe_int(s.get("tidak_ada_data_gender"))
                    total = _safe_int(s.get("total"))
                    if total == 0 and (lk or pr or nd):
                        total = lk + pr + nd

                    payload = {
                        "kelainan": kel,
                        "laki_laki": max(lk, 0),
                        "perempuan": max(pr, 0),
                        "tidak_ada_data_gender": max(nd, 0),
                        TOTAL_COL: max(total, 0),
                        "is_total_row": "1" if kel.strip().lower() == TOTAL_ROW_LABEL.lower() else "0",
                    }
                    insert_row(payload, kode_organisasi)
                    results.append({"Baris": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {kel}"})
                except Exception as e:
                    results.append({"Baris": i + 2, "Status": "GAGAL", "Keterangan": str(e)})

            res_df = pd.DataFrame(results)
            st.write("**Hasil unggah:**")
            st.dataframe(res_df, use_container_width=True)

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
                file_name="log_hasil_unggah_jk.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="jk::dl_log"
            )
