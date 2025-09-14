import streamlit as st
import sqlite3
import pandas as pd
from datetime import date, datetime
import re
import io

st.set_page_config(page_title="Identitas Organisasi", page_icon="🏢", layout="wide")
st.title("🏢 Identitas Organisasi")

DB_PATH = "hemofilia.db"
DB_WILAYAH = "wilayah.db"
TABLE = "identitas_organisasi"
CATATAN_COL = "catatan"

# Kolom baru yang diminta
NEW_COLS = {
    "hmhi_cabang": "TEXT",
    "diisi_oleh": "TEXT",
    "jabatan": "TEXT",
}

# ====== Alias tampilan <-> nama kolom DB ======
ALIAS_MAP = {
    "kode_organisasi": "Kode Organisasi",
    "hmhi_cabang": "HMHI cabang",
    "diisi_oleh": "Diisi oleh",
    "jabatan": "Jabatan",
    "no_telp": "No. Telp",
    "email": "Email",
    "sumber_data": "Sumber Data",
    "tanggal": "Tanggal",
    "kota_cakupan_cabang": "Kota cakupan cabang",
    CATATAN_COL: "Catatan",
}
ORDER_COLS = [
    "kode_organisasi",
    "hmhi_cabang",
    "diisi_oleh",
    "jabatan",
    "no_telp",
    "email",
    "sumber_data",
    "tanggal",
    "kota_cakupan_cabang",
    CATATAN_COL,
]
HIDE_COLS = {"id", "created_at"}  # disembunyikan dari tampilan

# ---------------- DB helpers ----------------
def connect():
    return sqlite3.connect(DB_PATH)

def _table_columns(conn, table_name: str) -> set:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return {row[1] for row in cur.fetchall()}  # set nama kolom

def _add_missing_columns(conn, table_name: str, colspec: dict):
    existing = _table_columns(conn, table_name)
    cur = conn.cursor()
    altered = False
    for col, typ in colspec.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE {table_name} ADD COLUMN {col} {typ}")
            altered = True
    if altered:
        conn.commit()

def init_db():
    with connect() as conn:
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT UNIQUE NOT NULL,
                created_at TEXT NOT NULL,
                no_telp TEXT,
                email TEXT,
                sumber_data TEXT,
                tanggal TEXT,
                kota_cakupan_cabang TEXT,
                {CATATAN_COL} TEXT,
                hmhi_cabang TEXT,
                diisi_oleh TEXT,
                jabatan TEXT
            )
        """)
        conn.commit()
        _add_missing_columns(conn, TABLE, NEW_COLS)

        # index lama (kota_cakupan_cabang)
        try:
            c.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE}_provinsi ON {TABLE}(kota_cakupan_cabang)")
            conn.commit()
        except Exception:
            pass
        # index unik berbasis hmhi_cabang
        try:
            c.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{TABLE}_hmhi ON {TABLE}(hmhi_cabang)")
            conn.commit()
        except Exception:
            pass

def hmhi_cabang_sudah_ada(hmhi: str) -> bool:
    hmhi = (hmhi or "").strip()
    if not hmhi:
        return False
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(1) FROM {TABLE} WHERE hmhi_cabang = ?", (hmhi,))
        n = cur.fetchone()[0]
    return n > 0

def kode_organisasi_sudah_ada(kode: str) -> bool:
    if not kode:
        return False
    with connect() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(1) FROM {TABLE} WHERE kode_organisasi = ?", (kode,))
        n = cur.fetchone()[0]
    return n > 0

def gen_kode():
    return f"ORG-{int(datetime.utcnow().timestamp())}"

def insert_row(payload: dict):
    """Insert dari form (kode otomatis)."""
    hmhi = (payload.get("hmhi_cabang") or "").strip()
    if hmhi_cabang_sudah_ada(hmhi):
        raise ValueError(f"Identitas untuk HMHI cabang/Provinsi '{hmhi}' sudah ada. Penginputan ulang ditolak.")
    with connect() as conn:
        c = conn.cursor()
        cols = ", ".join(payload.keys())
        placeholders = ", ".join(["?"] * len(payload))
        vals = [payload[k] for k in payload]
        kode_organisasi = gen_kode()
        c.execute(
            f"INSERT INTO {TABLE} (kode_organisasi, created_at, {cols}) VALUES (?, ?, {placeholders})",
            [kode_organisasi, datetime.utcnow().isoformat()] + vals
        )
        conn.commit()

def insert_row_excel(payload: dict, kode_opt: str | None):
    """Insert untuk unggahan Excel (boleh kirim kode_organisasi; jika kosong → generate)."""
    hmhi = (payload.get("hmhi_cabang") or "").strip()
    if hmhi_cabang_sudah_ada(hmhi):
        raise ValueError(f"HMHI cabang/Provinsi '{hmhi}' sudah ada di database.")

    kode = (kode_opt or "").strip() or gen_kode()
    if kode_organisasi_sudah_ada(kode):
        raise ValueError(f"Kode Organisasi '{kode}' sudah ada di database.")

    with connect() as conn:
        c = conn.cursor()
        cols = ", ".join(payload.keys())
        placeholders = ", ".join(["?"] * len(payload))
        vals = [payload[k] for k in payload]
        c.execute(
            f"INSERT INTO {TABLE} (kode_organisasi, created_at, {cols}) VALUES (?, ?, {placeholders})",
            [kode, datetime.utcnow().isoformat()] + vals
        )
        conn.commit()

def read_data(limit=500):
    with connect() as conn:
        return pd.read_sql_query(f"SELECT * FROM {TABLE} ORDER BY id DESC LIMIT ?", conn, params=[limit])

# ---------------- Wilayah (provinsi) ----------------
def load_provinsi_options():
    try:
        with sqlite3.connect(DB_WILAYAH) as conn:
            df = pd.read_sql_query(
                "SELECT nama AS provinsi FROM wilayah WHERE length(kode) = 2 ORDER BY kode;",
                conn
            )
        return df["provinsi"].dropna().astype(str).tolist()
    except Exception:
        return []

# ---------------- Validasi util ----------------
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def norm_email(x: str) -> str:
    x = (str(x) if pd.notna(x) else "").strip()
    if x and not EMAIL_RE.match(x):
        raise ValueError(f"Format email tidak valid: {x}")
    return x

def norm_tanggal(x) -> str:
    """Terima string/Excel date/datetime → kembalikan ISO YYYY-MM-DD atau ''."""
    if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and not x.strip()):
        return ""
    if isinstance(x, (datetime, pd.Timestamp)):
        return x.date().isoformat()
    if isinstance(x, date):
        return x.isoformat()
    s = str(x).strip()
    # coba parse ISO dulu
    try:
        return datetime.fromisoformat(s).date().isoformat()
    except Exception:
        pass
    # coba dengan pandas
    try:
        return pd.to_datetime(s, errors="raise").date().isoformat()
    except Exception:
        raise ValueError(f"Format tanggal tidak dikenali: {x}")

# ---------------- UI ----------------
init_db()
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data Tersimpan"])

with tab_input:
    st.caption("Isi identitas organisasi. Daftar **Provinsi** diambil dari **wilayah.db**. Setiap provinsi hanya dapat diinput **sekali** (dibatasi pada HMHI cabang).")

    provinsi_options = load_provinsi_options()

    with st.form(key="identitas::form", clear_on_submit=True):
        # HMHI cabang = pilihan Provinsi
        if provinsi_options:
            hmhi_cabang = st.selectbox("HMHI cabang (Provinsi)", provinsi_options, index=0)
        else:
            hmhi_cabang = st.text_input("HMHI cabang (Provinsi)", placeholder="Provinsi")

        diisi_oleh = st.text_input("Diisi oleh")
        jabatan = st.text_input("Jabatan")

        # Kolom lama
        no_telp = st.text_input("No. Telp")
        email = st.text_input("Email", placeholder="nama@contoh.com")
        sumber_data = st.text_input("Sumber data")

        tgl = st.date_input("Tanggal (YYYY-MM-DD)", value=date.today())
        tanggal = tgl.isoformat() if tgl else ""

        # Kota cakupan cabang = teks biasa
        kota_cakupan_cabang = st.text_input("Kota cakupan cabang", placeholder="Mis. Kota/Kabupatén yang dicakup")

        catatan = st.text_area("Catatan (opsional)")
        submitted = st.form_submit_button("💾 Simpan")

    if submitted:
        errs = []
        # Validasi email (jika diisi)
        try:
            email_val = norm_email(email)
        except ValueError as e:
            errs.append(str(e))

        # Validasi tanggal
        try:
            tanggal_val = norm_tanggal(tanggal)
        except ValueError as e:
            errs.append(str(e))
            tanggal_val = ""

        hmhi_val = (hmhi_cabang or "").strip()
        if not hmhi_val:
            errs.append("HMHI cabang (Provinsi) wajib diisi.")
        if hmhi_val and hmhi_cabang_sudah_ada(hmhi_val):
            errs.append(f"Data untuk HMHI cabang/Provinsi **{hmhi_val}** sudah pernah diinput. Penginputan ulang ditolak.")

        if errs:
            st.error("\n".join(errs))
        else:
            payload = {
                "hmhi_cabang": hmhi_val,
                "diisi_oleh": (diisi_oleh or "").strip(),
                "jabatan": (jabatan or "").strip(),
                "no_telp": (no_telp or "").strip(),
                "email": email_val,
                "sumber_data": (sumber_data or "").strip(),
                "tanggal": tanggal_val,
                "kota_cakupan_cabang": (kota_cakupan_cabang or "").strip(),
                CATATAN_COL: (catatan or "").strip(),
            }
            try:
                insert_row(payload)
                st.success(f"Data berhasil disimpan untuk HMHI cabang/Provinsi **{hmhi_val}**.")
            except ValueError as ve:
                st.error(str(ve))
            except Exception as e:
                st.error(f"Gagal menyimpan data: {e}")

with tab_data:
    st.subheader("📄 Data Tersimpan")

    # ===== Download Template Excel =====
    st.caption("Format unggahan yang diterima harus memiliki header kolom persis seperti di bawah ini.")
    template_cols_alias = [ALIAS_MAP[c] for c in ORDER_COLS]
    tmpl_buf = io.BytesIO()
    with pd.ExcelWriter(tmpl_buf, engine="xlsxwriter") as writer:
        pd.DataFrame(columns=template_cols_alias).to_excel(writer, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        tmpl_buf.getvalue(),
        file_name="template_identitas_organisasi.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="identitas::template"
    )

    # ===== Data grid saat ini =====
    df = read_data()
    if df.empty:
        st.info("Belum ada data.")
    else:
        existing_cols = [c for c in ORDER_COLS if c in df.columns]
        df_view = df[[c for c in existing_cols if c not in HIDE_COLS]].rename(columns=ALIAS_MAP)
        st.dataframe(df_view, use_container_width=True)

    # ===== Upload Excel =====
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader("Pilih file Excel (.xlsx) dengan header sesuai template", type=["xlsx"], key="identitas::uploader")

    if up is not None:
        try:
            raw = pd.read_excel(up)
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        # Cek kolom wajib (alias)
        expected_alias = [ALIAS_MAP[c] for c in ORDER_COLS]
        missing = [c for c in expected_alias if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Kolom yang belum ada: " + ", ".join(missing))
            st.stop()

        # Map alias -> nama kolom DB
        reverse_alias = {v: k for k, v in ALIAS_MAP.items()}
        df_up = raw.rename(columns=reverse_alias)

        # Bersihkan whitespace & NaN → ''
        for col in ORDER_COLS:
            if col in df_up.columns:
                df_up[col] = df_up[col].astype(object).where(pd.notna(df_up[col]), "")

        # Preview
        st.caption("Pratinjau data yang akan diproses:")
        st.dataframe(raw.head(20), use_container_width=True)

        # Proses unggah
        if st.button("🚀 Proses & Simpan ke Database", type="primary", key="identitas::process"):
            results = []
            seen_hmhi = set()  # duplikat dalam file
            for idx, row in df_up.iterrows():
                try:
                    hmhi_val = str(row.get("hmhi_cabang", "") or "").strip()
                    if not hmhi_val:
                        raise ValueError("HMHI cabang (Provinsi) kosong.")

                    # duplikat di file
                    if hmhi_val in seen_hmhi:
                        raise ValueError(f"Duplikat HMHI cabang di file: {hmhi_val}")
                    seen_hmhi.add(hmhi_val)

                    # normalisasi email & tanggal
                    email_val = norm_email(row.get("email", ""))
                    tanggal_val = norm_tanggal(row.get("tanggal", ""))

                    payload = {
                        "hmhi_cabang": hmhi_val,
                        "diisi_oleh": str(row.get("diisi_oleh", "") or "").strip(),
                        "jabatan": str(row.get("jabatan", "") or "").strip(),
                        "no_telp": str(row.get("no_telp", "") or "").strip(),
                        "email": email_val,
                        "sumber_data": str(row.get("sumber_data", "") or "").strip(),
                        "tanggal": tanggal_val,
                        "kota_cakupan_cabang": str(row.get("kota_cakupan_cabang", "") or "").strip(),
                        CATATAN_COL: str(row.get(CATATAN_COL, "") or "").strip(),
                    }

                    kode = str(row.get("kode_organisasi", "") or "").strip()
                    insert_row_excel(payload, kode_opt=kode)

                    results.append({"Baris": idx + 2, "Status": "OK", "Keterangan": f"Simpan: {hmhi_val}"})
                except Exception as e:
                    results.append({"Baris": idx + 2, "Status": "GAGAL", "Keterangan": str(e)})

            res_df = pd.DataFrame(results)
            st.write("**Hasil unggah:**")
            st.dataframe(res_df, use_container_width=True)

            # Ringkasan
            ok = (res_df["Status"] == "OK").sum()
            fail = (res_df["Status"] == "GAGAL").sum()
            if ok > 0:
                st.success(f"Berhasil menyimpan {ok} baris.")
            if fail > 0:
                st.error(f"Gagal menyimpan {fail} baris.")

            # Unduh log hasil
            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as writer:
                res_df.to_excel(writer, index=False, sheet_name="Hasil")
            st.download_button(
                "📄 Unduh Log Hasil",
                log_buf.getvalue(),
                file_name="log_hasil_unggah_identitas.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="identitas::logdownload"
            )

    # ===== Unduh Excel data terkini (sesuai alias & urutan) =====
    st.markdown("### ⬇️ Unduh Data Saat Ini")
    df_now = read_data()
    if not df_now.empty:
        export_cols = [c for c in ORDER_COLS if c in df_now.columns and c not in HIDE_COLS]
        df_export = df_now[export_cols].rename(columns=ALIAS_MAP)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df_export.to_excel(writer, index=False, sheet_name="IdentitasOrganisasi")
        st.download_button(
            "⬇️ Unduh Excel (Data Terkini)",
            buffer.getvalue(),
            file_name="identitas_organisasi.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="identitas::download"
        )
