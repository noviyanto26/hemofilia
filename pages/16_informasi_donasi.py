import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Informasi Donasi", page_icon="🎁", layout="wide")
st.title("🎁 Informasi Donasi")

DB_PATH = "hemofilia.db"
TABLE = "informasi_donasi"

# ======================== Util DB ========================
def connect():
    return sqlite3.connect(DB_PATH)

def _table_exists(conn, table):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def _has_column(conn, table, col):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any((row[1] == col) for row in cur.fetchall())

def _get_first_existing_col(conn, table, candidates):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    for c in candidates:
        if c in cols:
            return c
    return None

def migrate_if_needed(table_name: str):
    with connect() as conn:
        if not _table_exists(conn, table_name):
            return
        cur = conn.cursor()
        required = [
            "kode_organisasi",
            "jenis_donasi",
            "merk",
            "jumlah_total_iu_setahun",
            "kegunaan",
        ]
        cur.execute(f"PRAGMA table_info({table_name})")
        have = [r[1] for r in cur.fetchall()]
        if all(c in have for c in required):
            return
        st.warning(f"Migrasi skema: menyesuaikan tabel {table_name} …")
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name}_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT,
                created_at TEXT NOT NULL,
                jenis_donasi TEXT,
                merk TEXT,
                jumlah_total_iu_setahun REAL,
                kegunaan TEXT,
                FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
            )
        """)
        old_cols = [r[1] for r in cur.fetchall() if r[1] != "id"]
        if old_cols:
            cols_csv = ", ".join(old_cols)
            cur.execute(f"INSERT INTO {table_name}_new ({cols_csv}) SELECT {cols_csv} FROM {table_name}")
        cur.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_backup")
        cur.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
        conn.commit()
        cur.execute("PRAGMA foreign_keys=ON")

def init_db(table_name: str):
    with connect() as conn:
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT,
                created_at TEXT NOT NULL,
                jenis_donasi TEXT,
                merk TEXT,
                jumlah_total_iu_setahun REAL,
                kegunaan TEXT,
                FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
            )
        """)
        conn.commit()

def insert_row(table_name: str, payload: dict, kode_organisasi: str):
    with connect() as conn:
        c = conn.cursor()
        cols = ", ".join(payload.keys())
        placeholders = ", ".join(["?"] * len(payload))
        vals = [payload[k] for k in payload]
        c.execute(
            f"INSERT INTO {table_name} (kode_organisasi, created_at, {cols}) VALUES (?, ?, {placeholders})",
            [kode_organisasi, datetime.utcnow().isoformat()] + vals
        )
        conn.commit()

def read_with_label(table_name: str, limit=1000):
    with connect() as conn:
        label_col = _get_first_existing_col(conn, "identitas_organisasi",
                                            ["HMHI_cabang", "hmhi_cabang", "HMHI cabang", "kota_cakupan_cabang"])
        select_label = f"io.[{label_col}]" if label_col and " " in label_col else f"io.{label_col}" if label_col else "NULL"
        return pd.read_sql_query(
            f"""
            SELECT
              t.id, t.created_at,
              t.jenis_donasi, t.merk, t.jumlah_total_iu_setahun, t.kegunaan,
              {select_label} AS hmhi_cabang
            FROM {table_name} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            ORDER BY t.id DESC
            LIMIT ?
            """, conn, params=[limit]
        )

# ======================== Helpers UI ========================
def load_kode_organisasi_with_label():
    with connect() as conn:
        label_col = _get_first_existing_col(conn, "identitas_organisasi",
                                            ["HMHI_cabang", "hmhi_cabang", "HMHI cabang", "kota_cakupan_cabang"])
        col_ref = f"[{label_col}]" if label_col and " " in label_col else label_col
        df = pd.read_sql_query(f"SELECT kode_organisasi, {col_ref} AS label FROM identitas_organisasi ORDER BY id DESC", conn)
        mapping = {row["kode_organisasi"]: str(row["label"] or "-") for _, row in df.iterrows()}
        return mapping, list(mapping.keys())

def safe_float(val, default=0.0):
    try:
        x = pd.to_numeric(val, errors="coerce")
        return default if pd.isna(x) else float(x)
    except Exception:
        return default

# ======================== Startup ========================
migrate_if_needed(TABLE)
init_db(TABLE)

# ======================== Label statis untuk Jenis Donasi ========================
JENIS_DONASI_LABELS = [
    "Konsentrat Faktor VIII",
    "Konsentrat Faktor IX",
    "Bypassing Agent",
]

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    mapping, kode_list = load_kode_organisasi_with_label()
    if not kode_list:
        st.warning("Belum ada data Identitas Organisasi.")
        kode_organisasi = None
    else:
        kode_organisasi = st.selectbox(
            "Pilih Organisasi (HMHI Cabang)",
            options=kode_list,
            format_func=lambda x: mapping.get(x, "-"),
            key="idn::kode_select"
        )

    st.subheader("Form Informasi Donasi")

    df_default = pd.DataFrame({
        "jenis_donasi": JENIS_DONASI_LABELS,
        "merk": ["", "", ""],
        "jumlah_total_iu_setahun": [0.0, 0.0, 0.0],
        "kegunaan": ["", "", ""],
    })

    with st.form("idn::form"):
        ed = st.data_editor(
            df_default,
            key="idn::editor",
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            disabled=["jenis_donasi"],
        )
        submit = st.form_submit_button("💾 Simpan")

    if submit and kode_organisasi:
        for _, row in ed.iterrows():
            if row["merk"] or row["jumlah_total_iu_setahun"] > 0 or row["kegunaan"]:
                payload = {
                    "jenis_donasi": row["jenis_donasi"],
                    "merk": row["merk"],
                    "jumlah_total_iu_setahun": safe_float(row["jumlah_total_iu_setahun"], 0.0),
                    "kegunaan": row["kegunaan"],
                }
                insert_row(TABLE, payload, kode_organisasi)
        st.success(f"Data tersimpan untuk {mapping.get(kode_organisasi, '-')}")

with tab_data:
    st.subheader("📄 Data Tersimpan — Informasi Donasi")

    up = st.file_uploader("⬆️ Unggah Excel (.xlsx)", type=["xlsx"], key="idn::uploader")
    if up is not None:
        try:
            df_up = pd.read_excel(up)
            st.dataframe(df_up.head(), use_container_width=True)
            norm_map = {c: c.strip().lower().replace(" ", "_") for c in df_up.columns}
            df_norm = df_up.rename(columns=norm_map)
            if all(c in df_norm.columns for c in ["jenis_donasi", "merk", "jumlah_total_iu_setahun", "kegunaan", "kode_organisasi"]):
                for _, r in df_norm.iterrows():
                    insert_row(TABLE, {
                        "jenis_donasi": str(r["jenis_donasi"] or ""),
                        "merk": str(r["merk"] or ""),
                        "jumlah_total_iu_setahun": safe_float(r["jumlah_total_iu_setahun"], 0.0),
                        "kegunaan": str(r["kegunaan"] or ""),
                    }, str(r["kode_organisasi"]))
                st.success("Impor data selesai.")
            else:
                st.error("Kolom wajib tidak lengkap di file Excel.")
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")

    df = read_with_label(TABLE, limit=1000)
    if df.empty:
        st.info("Belum ada data.")
    else:
        view = df.rename(columns={
            "hmhi_cabang": "HMHI Cabang",
            "created_at": "Created At",
            "jenis_donasi": "Jenis Donasi",
            "merk": "Merk",
            "jumlah_total_iu_setahun": "Jumlah Total (IU) Setahun",
            "kegunaan": "Kegunaan",
        })
        st.dataframe(view, use_container_width=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="Informasi_Donasi")
        st.download_button(
            "⬇️ Unduh Excel",
            buf.getvalue(),
            file_name="informasi_donasi.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
