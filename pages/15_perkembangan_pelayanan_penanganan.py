import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Perkembangan Pelayanan Penanganan Hemofilia", page_icon="📈", layout="wide")
st.title("📈 Perkembangan Pelayanan Penanganan Hemofilia")

# ======================== Konfigurasi DB & Tabel ========================
DB_PATH = "hemofilia.db"
TABLE_NAME = "perkembangan_pelayanan_penanganan"
IDENT_TABLE = "identitas_organisasi"
RS_TABLE = "rumah_sakit"

# ======================== Konektor & Inisialisasi DB ========================
def db_exec_sql(sql: str, params=None):
    """Menjalankan perintah SQL (INSERT, UPDATE, DELETE)."""
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(sql, params or [])
        conn.commit()

def db_fetch_df(sql: str, params=None) -> pd.DataFrame:
    """Menjalankan query SELECT dan mengembalikan hasilnya sebagai DataFrame."""
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(sql, conn, params=params or [])

def init_db():
    """Memastikan tabel utama tersedia dengan skema yang benar."""
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        # Membuat tabel utama jika belum ada
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT,
                created_at TEXT NOT NULL,
                jenis TEXT,
                jumlah_terapi_gen INTEGER,
                tahun INTEGER,
                nama_rumah_sakit TEXT,
                lokasi TEXT,
                propinsi TEXT,
                FOREIGN KEY (kode_organisasi) REFERENCES {IDENT_TABLE}(kode_organisasi)
            )
        """)
        # Pastikan tabel pendukung juga ada (opsional, untuk menghindari error)
        c.execute(f"CREATE TABLE IF NOT EXISTS {IDENT_TABLE} (kode_organisasi TEXT UNIQUE, hmhi_cabang TEXT)")
        c.execute(f"CREATE TABLE IF NOT EXISTS {RS_TABLE} (Nama TEXT, Lokasi TEXT, Propinsi TEXT)")
        conn.commit()

# Panggil inisialisasi saat aplikasi dimulai
init_db()

# ======================== Label Statis & Opsi ========================
JENIS_LABELS = [
    "Hemofilia A Berat",
    "Hemofilia B Berat",
    "Hemofilia tipe lain",
    "vWD",
]

# ======================== Template Unggah ========================
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Jenis",
    "Jumlah Terapi Gen",
    "Tahun",
    "Nama Rumah Sakit",
    "Lokasi",
    "Propinsi",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Jenis": "jenis",
    "Jumlah Terapi Gen": "jumlah_terapi_gen",
    "Tahun": "tahun",
    "Nama Rumah Sakit": "nama_rumah_sakit",
    "Lokasi": "lokasi",
    "Propinsi": "propinsi",
}

# ======================== Helper Umum ========================
def safe_int(val, default=0):
    """Mengonversi nilai ke integer dengan aman."""
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return default
        return int(x)
    except (ValueError, TypeError):
        return default

def load_org_map():
    """
    Mengambil pemetaan 'HMHI cabang' -> 'kode_organisasi'.
    Mengembalikan:
        mapping: dict {hmhi_cabang -> kode_organisasi}
        options: list[str] hmhi_cabang (diurutkan)
    """
    try:
        df = db_fetch_df(f"SELECT hmhi_cabang, kode_organisasi FROM {IDENT_TABLE} WHERE COALESCE(hmhi_cabang, '') <> ''")
        if df.empty:
            return {}, []
        mapping = {}
        for _, row in df.iterrows():
            hmhi = str(row["hmhi_cabang"]).strip()
            kode = str(row["kode_organisasi"]).strip()
            if hmhi and kode and hmhi not in mapping:
                mapping[hmhi] = kode
        return mapping, sorted(mapping.keys())
    except Exception:
        return {}, []

def load_rs_options():
    """
    Mengambil daftar RS untuk SelectboxColumn.
    Mengembalikan:
        option_labels: list "Nama - Lokasi - Propinsi"
        label_to_parts: dict untuk memetakan kembali label ke komponennya
    """
    try:
        df_rs = db_fetch_df(f"SELECT Nama, Lokasi, Propinsi FROM {RS_TABLE} ORDER BY Nama")
    except Exception:
        return [], {}

    def to_str(x): return "" if pd.isna(x) else str(x).strip()
    label_to_parts = {}
    for _, r in df_rs.iterrows():
        nama, lokasi, prop = to_str(r["Nama"]), to_str(r["Lokasi"]), to_str(r["Propinsi"])
        if not nama: continue
        label = f"{nama} - {lokasi or '-'} - {prop or '-'}"
        if label not in label_to_parts:
            label_to_parts[label] = {"nama": nama, "lokasi": lokasi, "propinsi": prop}

    return [""] + sorted(label_to_parts.keys()), label_to_parts

def insert_row(payload: dict, kode_organisasi: str):
    """Menyisipkan 1 baris data ke tabel utama."""
    sql = f"""
    INSERT INTO {TABLE_NAME} (
        kode_organisasi, created_at, jenis, jumlah_terapi_gen, tahun,
        nama_rumah_sakit, lokasi, propinsi
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        kode_organisasi, datetime.utcnow().isoformat(), payload.get("jenis"),
        safe_int(payload.get("jumlah_terapi_gen")), safe_int(payload.get("tahun")),
        payload.get("nama_rumah_sakit"), payload.get("lokasi"), payload.get("propinsi"),
    )
    db_exec_sql(sql, params)

def read_with_org_info(limit=1000):
    """Mengambil data dan menggabungkannya dengan info organisasi."""
    q = f"""
    SELECT t.*, io.hmhi_cabang
    FROM {TABLE_NAME} t
    LEFT JOIN {IDENT_TABLE} io ON io.kode_organisasi = t.kode_organisasi
    ORDER BY t.id DESC LIMIT ?
    """
    return db_fetch_df(q, params=[limit])

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    org_map, org_list = load_org_map()
    if not org_list:
        st.warning("Belum ada data Identitas Organisasi (HMHI cabang) di database.")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=org_list,
            key="ppph::hmhi_select"
        )

    rs_option_labels, rs_label_to_parts = load_rs_options()
    if not rs_option_labels:
        st.error("Gagal memuat daftar Rumah Sakit. Pastikan tabel 'rumah_sakit' tersedia.")
        st.stop()

    st.subheader("Form Perkembangan Pelayanan Penanganan Hemofilia")
    df_default = pd.DataFrame({
        "jenis": JENIS_LABELS,
        "jumlah_terapi_gen": [0] * len(JENIS_LABELS),
        "tahun": [datetime.utcnow().year] * len(JENIS_LABELS),
        "nama_rumah_sakit": [""] * len(JENIS_LABELS),
    })

    with st.form("ppph::form"):
        ed = st.data_editor(
            df_default, key="ppph::editor", use_container_width=True, num_rows="fixed",
            hide_index=True, disabled=["jenis"],
            column_config={
                "jenis": st.column_config.TextColumn("Jenis"),
                "jumlah_terapi_gen": st.column_config.NumberColumn("Jumlah Terapi Gen", min_value=0, step=1, format="%d"),
                "tahun": st.column_config.NumberColumn("Tahun", min_value=1900, max_value=2100, step=1, format="%d"),
                "nama_rumah_sakit": st.column_config.SelectboxColumn("Nama Rumah Sakit", options=rs_option_labels, required=False),
            },
        )
        submit = st.form_submit_button("💾 Simpan")

    if submit:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = org_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                n_saved = 0
                for _, row in ed.iterrows():
                    if safe_int(row.get("jumlah_terapi_gen", 0)) == 0 and not row.get("nama_rumah_sakit"): continue
                    rs_parts = rs_label_to_parts.get(str(row.get("nama_rumah_sakit") or ""), {})
                    payload = {
                        "jenis": row.get("jenis"), "jumlah_terapi_gen": row.get("jumlah_terapi_gen"),
                        "tahun": row.get("tahun"), "nama_rumah_sakit": rs_parts.get("nama"),
                        "lokasi": rs_parts.get("lokasi"), "propinsi": rs_parts.get("propinsi"),
                    }
                    try:
                        insert_row(payload, kode_organisasi)
                        n_saved += 1
                    except Exception as e:
                        st.error(f"Gagal menyimpan baris ({payload['jenis']}): {e}")

                if n_saved > 0:
                    st.success(f"{n_saved} baris tersimpan untuk **{selected_hmhi}**.")
                else:
                    st.info("Tidak ada baris valid untuk disimpan.")

with tab_data:
    st.subheader("📄 Data Tersimpan")
    try:
        df = read_with_org_info(limit=1000)
    except Exception as e:
        st.error(f"Gagal membaca data dari database: {e}")
        df = pd.DataFrame()

    if df.empty:
        st.info("Belum ada data tersimpan.")
    else:
        cols_order = ["hmhi_cabang", "created_at", "jenis", "jumlah_terapi_gen", "tahun", "nama_rumah_sakit", "lokasi", "propinsi"]
        view = df[cols_order].rename(columns={
            "hmhi_cabang": "HMHI Cabang", "created_at": "Waktu Input", "jenis": "Jenis",
            "jumlah_terapi_gen": "Jumlah Terapi Gen", "tahun": "Tahun", "nama_rumah_sakit": "Nama Rumah Sakit",
            "lokasi": "Lokasi", "propinsi": "Propinsi",
        })
        st.dataframe(view, use_container_width=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="Data_Tersimpan")
        st.download_button("⬇️ Unduh Excel (Data Tersimpan)", buf.getvalue(),
                          file_name="perkembangan_pelayanan_penanganan.xlsx",
                          mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                          key="ppph::download_data")

    st.divider()
    st.markdown("### 📥 Template & Unggah Excel")

    template_df = pd.DataFrame([{"HMHI cabang": "", "Jenis": "Hemofilia A Berat", "Jumlah Terapi Gen": 0, "Tahun": datetime.utcnow().year, "Nama Rumah Sakit": "", "Lokasi": "", "Propinsi": ""}], columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        template_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button("📄 Unduh Template Excel", buf_tmpl.getvalue(),
                      file_name="template_perkembangan_penanganan.xlsx",
                      mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                      key="ppph::dl_template")

    up = st.file_uploader("Unggah file Excel (.xlsx) sesuai template", type=["xlsx"], key="ppph::uploader")

    def process_upload(df_up: pd.DataFrame):
        org_map, _ = load_org_map()
        results, n_ok = [], 0
        for i, s in df_up.iterrows():
            try:
                hmhi = str(s.get("hmhi_cabang_info") or "").strip()
                if not hmhi: raise ValueError("Kolom 'HMHI cabang' kosong.")
                kode_organisasi = org_map.get(hmhi)
                if not kode_organisasi: raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan.")

                if safe_int(s.get("jumlah_terapi_gen", 0)) == 0 and not str(s.get("nama_rumah_sakit") or "").strip() and not str(s.get("jenis") or "").strip():
                    results.append({"Baris Excel": i + 2, "Status": "LEWAT", "Keterangan": "Baris kosong — dilewati"})
                    continue

                payload = {k: str(s.get(k) or "").strip() for k in ALIAS_TO_DB.values() if k != "hmhi_cabang_info"}
                insert_row(payload, kode_organisasi)
                info = f"Simpan → {hmhi} / {payload['jenis'] or '(tanpa jenis)'}"
                results.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": info})
                n_ok += 1
            except Exception as e:
                results.append({"Baris Excel": i + 2, "Status": "GAGAL", "Keterangan": str(e)})
        return pd.DataFrame(results), n_ok

    if up is not None:
        try:
            raw = pd.read_excel(up)
            raw.columns = [str(c).strip() for c in raw.columns]
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        if "HMHI cabang" not in raw.columns:
            st.error("Header kolom tidak sesuai. Minimal harus ada 'HMHI cabang'.")
            st.stop()

        for c in TEMPLATE_COLUMNS:
            if c not in raw.columns: raw[c] = 0 if c in ("Jumlah Terapi Gen", "Tahun") else ""
        
        st.caption("Pratinjau 20 baris pertama:")
        st.dataframe(raw[TEMPLATE_COLUMNS].head(20), use_container_width=True)
        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        if st.button("🚀 Proses & Simpan Unggahan", type="primary", key="ppph::process"):
            with st.spinner("Memproses..."):
                log_df, _ = process_upload(df_up)
            
            st.write("**Hasil unggah:**"); st.dataframe(log_df, use_container_width=True)
            ok, fail, skip = [(log_df["Status"] == s).sum() for s in ["OK", "GAGAL", "LEWAT"]]
            if ok: st.success(f"Berhasil menyimpan {ok} baris.")
            if fail: st.error(f"Gagal menyimpan {fail} baris.")
            if skip: st.info(f"Dilewati {skip} baris kosong.")

            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                log_df.to_excel(w, index=False, sheet_name="Hasil_Unggah")
            st.download_button("📄 Unduh Log Hasil", log_buf.getvalue(),
                              file_name="log_hasil_unggah_perkembangan.xlsx",
                              mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                              key="ppph::dl_log")
