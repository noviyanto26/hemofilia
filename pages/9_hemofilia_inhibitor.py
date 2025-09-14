import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

st.set_page_config(page_title="Jumlah Penyandang Hemofilia dengan Inhibitor", page_icon="🩸", layout="wide")
st.title("🩸 Jumlah Penyandang Hemofilia dengan Inhibitor\n(Kasus sebelum 2024 dan masih mengidap hingga kini)")

DB_PATH = "hemofilia.db"
TABLE = "hemofilia_inhibitor"

# Baris input
ROW_LABELS = ["Hemofilia A", "Hemofilia B"]

# Kolom angka
COLS = [
    ("terdiagnosis_aktif", "Terdiagnosis inhibitor aktif"),
    ("kasus_baru_2025", "Kasus baru 2025"),
    ("penanganan", "Penanganan"),
]

# ===== Template unggah & alias =====
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Jenis Hemofilia",
    "Terdiagnosis inhibitor aktif",
    "Kasus baru 2025",
    "Penanganan",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Jenis Hemofilia": "label",
    "Terdiagnosis inhibitor aktif": "terdiagnosis_aktif",
    "Kasus baru 2025": "kasus_baru_2025",
    "Penanganan": "penanganan",
}

# ======================== Util DB ========================
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

def migrate_if_needed():
    """Pastikan skema final tersedia (termasuk kode_organisasi & kolom angka)."""
    with connect() as conn:
        if not _table_exists(conn, TABLE):
            return
        needed = ["kode_organisasi", "label", "terdiagnosis_aktif", "kasus_baru_2025", "penanganan"]
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({TABLE})")
        have = [r[1] for r in cur.fetchall()]
        if all(col in have for col in needed):
            return

        st.warning("Migrasi skema: menyesuaikan tabel Hemofilia Inhibitor…")
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT,
                    created_at TEXT NOT NULL,
                    label TEXT,
                    terdiagnosis_aktif INTEGER,
                    kasus_baru_2025 INTEGER,
                    penanganan INTEGER,
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
            # Salin kolom yang ada saja (jika tabel lama ada)
            cur.execute(f"PRAGMA table_info({TABLE})")
            old_cols = [r[1] for r in cur.fetchall() if r[1] != "id"]
            if old_cols:
                cols_csv = ", ".join(old_cols)
                cur.execute(f"INSERT INTO {TABLE}_new ({cols_csv}) SELECT {cols_csv} FROM {TABLE}")
            cur.execute(f"ALTER TABLE {TABLE} RENAME TO {TABLE}_backup")
            cur.execute(f"ALTER TABLE {TABLE}_new RENAME TO {TABLE}")
            conn.commit()
            st.success("Migrasi selesai. Tabel lama disimpan sebagai _backup.")
        except Exception as e:
            conn.rollback()
            st.error(f"Migrasi gagal: {e}")
        finally:
            cur.execute("PRAGMA foreign_keys=ON")

def init_db():
    with connect() as conn:
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT,
                created_at TEXT NOT NULL,
                label TEXT,
                terdiagnosis_aktif INTEGER,
                kasus_baru_2025 INTEGER,
                penanganan INTEGER,
                FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
            )
        """)
        conn.commit()

# ======================== Helpers ========================
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
                hmhi_val = (str(row["hmhi_cabang"]).strip() if pd.notna(row["hmhi_cabang"]) else "")
                kode_val = (str(row["kode_organisasi"]).strip() if pd.notna(row["kode_organisasi"]) else "")
                if hmhi_val:
                    mapping[hmhi_val] = kode_val
            options = sorted(mapping.keys())
            return mapping, options
        except Exception:
            return {}, []

def safe_int(val):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return 0
        return int(x)
    except Exception:
        return 0

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

def read_with_kota(limit=500):
    with connect() as conn:
        if not _has_column(conn, TABLE, "kode_organisasi"):
            st.error("Kolom 'kode_organisasi' belum tersedia. Coba refresh setelah migrasi.")
            return pd.read_sql_query(f"SELECT * FROM {TABLE} ORDER BY id DESC LIMIT ?", conn, params=[limit])

        return pd.read_sql_query(
            f"""
            SELECT
              t.id, t.kode_organisasi, t.created_at, t.label,
              t.terdiagnosis_aktif, t.kasus_baru_2025, t.penanganan,
              io.hmhi_cabang, io.kota_cakupan_cabang
            FROM {TABLE} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            ORDER BY t.id DESC
            LIMIT ?
            """, conn, params=[limit]
        )

# ======================== Startup ========================
migrate_if_needed()
init_db()

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # 🔁 Pilihan dari HMHI cabang → dipetakan ke kode_organisasi
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="inhib::hmhi_select"
        )

    # Template editor
    df_default = pd.DataFrame(0, index=ROW_LABELS, columns=[c for c, _ in COLS])
    df_default.index.name = "Jenis Hemofilia"

    col_cfg = {
        "terdiagnosis_aktif": st.column_config.NumberColumn("Terdiagnosis inhibitor aktif", min_value=0, step=1),
        "kasus_baru_2025": st.column_config.NumberColumn("Kasus baru 2025", min_value=0, step=1),
        "penanganan": st.column_config.NumberColumn("Penanganan", min_value=0, step=1),
    }

    with st.form("inhib::form"):
        edited = st.data_editor(
            df_default,
            key="inhib::editor",
            column_config=col_cfg,
            use_container_width=True,
            num_rows="fixed",
        )
        submitted = st.form_submit_button("💾 Simpan")

    if submitted:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                for label in ROW_LABELS:
                    payload = {
                        "label": label,
                        "terdiagnosis_aktif": safe_int(edited.loc[label, "terdiagnosis_aktif"]),
                        "kasus_baru_2025": safe_int(edited.loc[label, "kasus_baru_2025"]),
                        "penanganan": safe_int(edited.loc[label, "penanganan"]),
                    }
                    insert_row(payload, kode_organisasi)
                st.success(f"Data berhasil disimpan untuk **{selected_hmhi}**.")

# ======================== Data Tersimpan & Unggah Excel ========================
with tab_data:
    st.subheader("📄 Data Tersimpan")
    df = read_with_kota(limit=500)

    # ===== Unduh Template Excel =====
    st.caption("Gunakan template berikut saat mengunggah data (kolom harus sesuai).")
    tmpl_rows = [
        {"HMHI cabang": "", "Jenis Hemofilia": "Hemofilia A", "Terdiagnosis inhibitor aktif": 0, "Kasus baru 2025": 0, "Penanganan": 0},
        {"HMHI cabang": "", "Jenis Hemofilia": "Hemofilia B", "Terdiagnosis inhibitor aktif": 0, "Kasus baru 2025": 0, "Penanganan": 0},
    ]
    tmpl_df = pd.DataFrame(tmpl_rows, columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_hemofilia_inhibitor.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="inhib::dl_template"
    )

    # ===== Tabel tampilan =====
    if df.empty:
        st.info("Belum ada data.")
    else:
        cols_order = ["hmhi_cabang", "kota_cakupan_cabang", "created_at", "label",
                      "terdiagnosis_aktif", "kasus_baru_2025", "penanganan"]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "label": "Jenis Hemofilia",
            "terdiagnosis_aktif": "Terdiagnosis inhibitor aktif",
            "kasus_baru_2025": "Kasus baru 2025",
            "penanganan": "Penanganan",
        })
        st.dataframe(view, use_container_width=True)

        # Unduh Excel (tampilan)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="Hemofilia_Inhibitor")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buffer.getvalue(),
            file_name="jumlah_penyandang_hemofilia_inhibitor.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="inhib::download"
        )

    # ===== Unggah Excel =====
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan header persis seperti template",
        type=["xlsx"],
        key="inhib::uploader"
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

        if st.button("🚀 Proses & Simpan", type="primary", key="inhib::process"):
            hmhi_map, _ = load_hmhi_to_kode()
            results = []

            for i in range(len(df_up)):
                try:
                    s = df_up.iloc[i]  # pandas.Series (hindari error tuple)
                    hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                    if not hmhi:
                        raise ValueError("Kolom 'HMHI cabang' kosong.")
                    kode_organisasi = hmhi_map.get(hmhi)
                    if not kode_organisasi:
                        raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                    label = str((s.get("label") or "")).strip()
                    if label not in ROW_LABELS:
                        raise ValueError(f"Jenis Hemofilia tidak valid: '{label}'. Harus salah satu dari {ROW_LABELS}.")

                    payload = {
                        "label": label,
                        "terdiagnosis_aktif": safe_int(s.get("terdiagnosis_aktif")),
                        "kasus_baru_2025": safe_int(s.get("kasus_baru_2025")),
                        "penanganan": safe_int(s.get("penanganan")),
                    }
                    insert_row(payload, kode_organisasi)
                    results.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {label}"})
                except Exception as e:
                    results.append({"Baris Excel": i + 2, "Status": "GAGAL", "Keterangan": str(e)})

            res_df = pd.DataFrame(results)
            st.write("**Hasil unggah:**")
            st.dataframe(res_df, use_container_width=True)

            ok = (res_df["Status"] == "OK").sum()
            fail = (res_df["Status"] == "GAGAL").sum()
            if ok:
                st.success(f"Berhasil menyimpan {ok} baris.")
            if fail:
                st.error(f"Gagal menyimpan {fail} baris.")

            # Unduh log hasil
            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                res_df.to_excel(w, index=False, sheet_name="Hasil")
            st.download_button(
                "📄 Unduh Log Hasil",
                log_buf.getvalue(),
                file_name="log_hasil_unggah_hemofilia_inhibitor.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="inhib::dl_log"
            )
