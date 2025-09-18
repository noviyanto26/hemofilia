import io
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# =========================
# Konfigurasi & Konstanta
# =========================
st.set_page_config(page_title="Pasien Nonfaktor", page_icon="🩸", layout="wide")
st.title("🩸 Pasien Pengguna Nonfaktor — Total Dengan & Tanpa Inhibitor")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = str((BASE_DIR / "hemofilia.db").resolve())

TABLE = "pasien_nonfaktor"   # skema tunggal

TEMPLATE_COLUMNS = ["HMHI cabang", "Dengan inhibitor", "Tanpa inhibitor"]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Dengan inhibitor": "dengan_inhibitor",
    "Tanpa inhibitor": "tanpa_inhibitor",
}

# ======================== Util DB ========================
def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def _has_column(conn, table, col):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any((r[1] == col) for r in cur.fetchall())

def create_main_schema(conn):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kode_organisasi TEXT,
            created_at TEXT NOT NULL,
            dengan_inhibitor INTEGER NOT NULL DEFAULT 0,
            tanpa_inhibitor  INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
        )
    """)

def init_db():
    with connect() as conn:
        create_main_schema(conn)
        conn.commit()

# ======================== Helpers ========================
def load_hmhi_to_kode():
    """Map hmhi_cabang -> kode_organisasi dari identitas_organisasi."""
    with connect() as conn:
        try:
            df = pd.read_sql_query(
                "SELECT kode_organisasi, hmhi_cabang FROM identitas_organisasi ORDER BY id DESC",
                conn
            )
            if df.empty:
                return {}, []
            mapping = {str(r["hmhi_cabang"]).strip(): str(r["kode_organisasi"]).strip()
                       for _, r in df.iterrows()
                       if pd.notna(r["hmhi_cabang"]) and str(r["hmhi_cabang"]).strip()}
            return mapping, sorted(mapping.keys())
        except Exception:
            return {}, []

def _to_nonneg_int(v):
    try:
        x = pd.to_numeric(v, errors="coerce")
        if pd.isna(x):
            return 0
        return max(int(x), 0)
    except Exception:
        return 0

def insert_row(kode_organisasi: str, di: int, ti: int):
    with connect() as conn:
        conn.execute(
            f"INSERT INTO {TABLE} (kode_organisasi, created_at, dengan_inhibitor, tanpa_inhibitor) VALUES (?, ?, ?, ?)",
            [kode_organisasi, datetime.utcnow().isoformat(), di, ti]
        )
        conn.commit()

def read_with_join(limit=500):
    with connect() as conn:
        if not _has_column(conn, TABLE, "kode_organisasi"):
            st.error("Kolom 'kode_organisasi' belum tersedia. Coba refresh setelah inisialisasi.")
            return pd.read_sql_query(f"SELECT * FROM {TABLE} ORDER BY id DESC LIMIT ?", conn, params=[limit])
        return pd.read_sql_query(
            f"""
            SELECT t.id, t.kode_organisasi, t.created_at,
                   t.dengan_inhibitor, t.tanpa_inhibitor,
                   io.hmhi_cabang, io.kota_cakupan_cabang
            FROM {TABLE} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            ORDER BY t.id DESC
            LIMIT ?
            """, conn, params=[limit]
        )

# ======================== Startup ========================
init_db()

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    st.caption("Isi total pasien nonfaktor untuk satu HMHI cabang. Kolom **Total** dihitung otomatis.")
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox("Pilih HMHI Cabang (Provinsi)", options=hmhi_list, key="nf1::hmhi")

    # === Editor berbasis tabel ===
    df_default = pd.DataFrame(
        [{"Dengan inhibitor": 0, "Tanpa inhibitor": 0, "Total": 0}],
        index=["Input"]
    )
    col_cfg = {
        "Dengan inhibitor": st.column_config.NumberColumn("Dengan inhibitor", min_value=0, step=1),
        "Tanpa inhibitor": st.column_config.NumberColumn("Tanpa inhibitor", min_value=0, step=1),
        "Total": st.column_config.NumberColumn("Total", min_value=0, step=1, disabled=True),
    }

    with st.form("nf1::form_editor"):
        edited = st.data_editor(
            df_default,
            key="nf1::editor",
            column_config=col_cfg,
            use_container_width=True,
            num_rows="fixed",
        )

        # Hitung total otomatis
        di_val = _to_nonneg_int(edited.loc["Input", "Dengan inhibitor"])
        ti_val = _to_nonneg_int(edited.loc["Input", "Tanpa inhibitor"])
        edited.loc["Input", "Total"] = di_val + ti_val

        submitted = st.form_submit_button("💾 Simpan")

    if submitted:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode = hmhi_map.get(selected_hmhi)
            if not kode:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            elif di_val == 0 and ti_val == 0:
                st.error("Minimal salah satu nilai > 0.")
            else:
                insert_row(kode, di_val, ti_val)
                st.success(f"Data tersimpan untuk **{selected_hmhi}** (DI={di_val}, TI={ti_val}).")

with tab_data:
    st.subheader("📄 Data Tersimpan")
    df = read_with_join(limit=500)

    # ===== Unduh Template Excel =====
    st.caption("Template unggah (format ringkas sesuai skema baru).")
    tmpl_df = pd.DataFrame([
        {"HMHI cabang": "", "Dengan inhibitor": 0, "Tanpa inhibitor": 0},
    ], columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button("📥 Unduh Template Excel", buf_tmpl.getvalue(),
                       file_name="template_pasien_nonfaktor_total.xlsx",
                       mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                       key="nf1::dl_template")

    if df.empty:
        st.info("Belum ada data.")
    else:
        view = df.rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "dengan_inhibitor": "Dengan inhibitor",
            "tanpa_inhibitor": "Tanpa inhibitor",
        })
        order = ["HMHI cabang", "Kota/Provinsi Cakupan Cabang", "Created At", "Dengan inhibitor", "Tanpa inhibitor"]
        order = [c for c in order if c in view.columns]
        st.dataframe(view[order], use_container_width=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view[order].to_excel(w, index=False, sheet_name="PasienNonfaktor")
        st.download_button("⬇️ Unduh Excel (Data Tersimpan)", buf.getvalue(),
                           file_name="pasien_nonfaktor_total.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                           key="nf1::download")

    # ===== Unggah Excel =====
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader("Pilih file Excel (.xlsx) dengan header persis seperti template",
                          type=["xlsx"], key="nf1::uploader")

    if up is not None:
        try:
            raw = pd.read_excel(up)
            raw.columns = [str(c).strip() for c in raw.columns]
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        missing = [c for c in TEMPLATE_COLUMNS if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Kolom yang belum ada: " + ", ".join(missing))
            st.stop()

        st.caption("Pratinjau 20 baris pertama dari file yang diunggah:")
        st.dataframe(raw.head(20), use_container_width=True)

        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        if st.button("🚀 Proses & Simpan", type="primary", key="nf1::process"):
            hmhi_map, _ = load_hmhi_to_kode()
            results, ok, fail = [], 0, 0

            for i in range(len(df_up)):
                try:
                    s = df_up.iloc[i]
                    hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                    if not hmhi:
                        raise ValueError("Kolom 'HMHI cabang' kosong.")
                    kode = hmhi_map.get(hmhi)
                    if not kode:
                        raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                    di = _to_nonneg_int(s.get("dengan_inhibitor"))
                    ti = _to_nonneg_int(s.get("tanpa_inhibitor"))
                    if di == 0 and ti == 0:
                        raise ValueError("Minimal salah satu dari 'Dengan inhibitor' atau 'Tanpa inhibitor' harus > 0.")

                    insert_row(kode, di, ti)
                    results.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} (DI={di}, TI={ti})"})
                    ok += 1
                except Exception as e:
                    results.append({"Baris Excel": i + 2, "Status": "GAGAL", "Keterangan": str(e)})
                    fail += 1

            res_df = pd.DataFrame(results)
            st.write("**Hasil unggah:**")
            st.dataframe(res_df, use_container_width=True)

            if ok:
                st.success(f"Berhasil menyimpan {ok} baris.")
            if fail:
                st.error(f"Gagal menyimpan {fail} baris.")

            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                res_df.to_excel(w, index=False, sheet_name="Hasil")
            st.download_button("📄 Unduh Log Hasil", log_buf.getvalue(),
                               file_name="log_hasil_unggah_pasien_nonfaktor_total.xlsx",
                               mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                               key="nf1::dl_log")
