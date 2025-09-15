import streamlit as st
import pandas as pd
from datetime import date, datetime
import re
import io

st.set_page_config(page_title="Identitas Organisasi", page_icon="🏢", layout="wide")
st.title("🏢 Identitas Organisasi")

# ====== Supabase (Postgres) target ======
SUPABASE_TABLE = "public.identitas_organisasi"
WILAYAH_TABLE  = "public.wilayah"   # opsional; dipakai untuk dropdown provinsi jika tersedia

# Konektor ke Postgres (dari db.py yang sudah kita siapkan)
from db import fetch_df as pg_fetch_df, exec_sql as pg_exec_sql, safe_url

TABLE = "identitas_organisasi"
CATATAN_COL = "catatan"

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

# ---------------- Util Validasi ----------------
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

# ---------------- Helper: Wilayah (Provinsi) dari Postgres (opsional) ----------------
def load_provinsi_options_pg():
    """Ambil daftar provinsi dari public.wilayah (jika ada). Jika gagal, kembalikan []"""
    try:
        df = pg_fetch_df(f"""
            SELECT nama AS provinsi
            FROM {WILAYAH_TABLE}
            WHERE length(kode) = 2
            ORDER BY kode
        """)
        return df["provinsi"].dropna().astype(str).tolist()
    except Exception:
        return []

# ---------------- Helper: DB checks & CRUD ke Supabase ----------------
def gen_kode():
    return f"ORG-{int(datetime.utcnow().timestamp())}"

def hmhi_cabang_sudah_ada_pg(hmhi: str) -> bool:
    hmhi = (hmhi or "").strip()
    if not hmhi:
        return False
    df = pg_fetch_df(
        f"SELECT COUNT(1) AS n FROM {SUPABASE_TABLE} WHERE hmhi_cabang = :hmhi",
        {"hmhi": hmhi},
    )
    return int(df.iloc[0]["n"]) > 0

def kode_organisasi_sudah_ada_pg(kode: str) -> bool:
    kode = (kode or "").strip()
    if not kode:
        return False
    df = pg_fetch_df(
        f"SELECT COUNT(1) AS n FROM {SUPABASE_TABLE} WHERE kode_organisasi = :kode",
        {"kode": kode},
    )
    return int(df.iloc[0]["n"]) > 0

def insert_row_pg(payload: dict):
    """INSERT dari form → Supabase. created_at = NOW() (server)."""
    hmhi = (payload.get("hmhi_cabang") or "").strip()
    if hmhi and hmhi_cabang_sudah_ada_pg(hmhi):
        raise ValueError(f"Identitas untuk HMHI cabang/Provinsi '{hmhi}' sudah ada. Penginputan ulang ditolak.")

    # generate kode baru
    kode_organisasi = gen_kode()

    cols = [
        "kode_organisasi", "created_at",
        "hmhi_cabang", "diisi_oleh", "jabatan",
        "no_telp", "email", "sumber_data",
        "tanggal", "kota_cakupan_cabang", CATATAN_COL
    ]
    params = {
        "kode_organisasi": kode_organisasi,
        "hmhi_cabang": (payload.get("hmhi_cabang") or "").strip(),
        "diisi_oleh": (payload.get("diisi_oleh") or "").strip(),
        "jabatan": (payload.get("jabatan") or "").strip(),
        "no_telp": (payload.get("no_telp") or "").strip(),
        "email": (payload.get("email") or "").strip(),
        "sumber_data": (payload.get("sumber_data") or "").strip(),
        "tanggal": (payload.get("tanggal") or None),  # biarkan NULL jika kosong
        "kota_cakupan_cabang": (payload.get("kota_cakupan_cabang") or "").strip(),
        CATATAN_COL: (payload.get(CATATAN_COL) or "").strip(),
    }

    # created_at pakai NOW(); tanggal cast ke DATE jika ada
    sql = f"""
    INSERT INTO {SUPABASE_TABLE} (
        kode_organisasi, created_at,
        hmhi_cabang, diisi_oleh, jabatan,
        no_telp, email, sumber_data,
        tanggal, kota_cakupan_cabang, {CATATAN_COL}
    )
    VALUES (
        :kode_organisasi, NOW(),
        :hmhi_cabang, :diisi_oleh, :jabatan,
        :no_telp, :email, :sumber_data,
        CASE WHEN :tanggal IS NULL OR :tanggal = '' THEN NULL ELSE CAST(:tanggal AS date) END,
        :kota_cakupan_cabang, :{CATATAN_COL}
    )
    """
    pg_exec_sql(sql, params)

def insert_row_excel_pg(payload: dict, kode_opt: str | None):
    """INSERT dari unggahan Excel → Supabase. Boleh kirim kode; jika kosong → generate."""
    hmhi = (payload.get("hmhi_cabang") or "").strip()
    if hmhi and hmhi_cabang_sudah_ada_pg(hmhi):
        raise ValueError(f"HMHI cabang/Provinsi '{hmhi}' sudah ada di database.")

    kode = (kode_opt or "").strip() or gen_kode()
    if kode_organisasi_sudah_ada_pg(kode):
        raise ValueError(f"Kode Organisasi '{kode}' sudah ada di database.")

    params = {
        "kode_organisasi": kode,
        "hmhi_cabang": (payload.get("hmhi_cabang") or "").strip(),
        "diisi_oleh": (payload.get("diisi_oleh") or "").strip(),
        "jabatan": (payload.get("jabatan") or "").strip(),
        "no_telp": (payload.get("no_telp") or "").strip(),
        "email": (payload.get("email") or "").strip(),
        "sumber_data": (payload.get("sumber_data") or "").strip(),
        "tanggal": (payload.get("tanggal") or None),
        "kota_cakupan_cabang": (payload.get("kota_cakupan_cabang") or "").strip(),
        CATATAN_COL: (payload.get(CATATAN_COL) or "").strip(),
    }

    sql = f"""
    INSERT INTO {SUPABASE_TABLE} (
        kode_organisasi, created_at,
        hmhi_cabang, diisi_oleh, jabatan,
        no_telp, email, sumber_data,
        tanggal, kota_cakupan_cabang, {CATATAN_COL}
    )
    VALUES (
        :kode_organisasi, NOW(),
        :hmhi_cabang, :diisi_oleh, :jabatan,
        :no_telp, :email, :sumber_data,
        CASE WHEN :tanggal IS NULL OR :tanggal = '' THEN NULL ELSE CAST(:tanggal AS date) END,
        :kota_cakupan_cabang, :{CATATAN_COL}
    )
    """
    pg_exec_sql(sql, params)

def read_data_pg(limit=500):
    lim = int(limit)
    return pg_fetch_df(f"""
        SELECT
            id, kode_organisasi, created_at,
            hmhi_cabang, diisi_oleh, jabatan,
            no_telp, email, sumber_data,
            tanggal, kota_cakupan_cabang, {CATATAN_COL}
        FROM {SUPABASE_TABLE}
        ORDER BY id DESC
        LIMIT {lim}
    """)

# ---------------- UI ----------------
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data Tersimpan"])

with tab_input:
    st.caption("Isi identitas organisasi. Opsi **Provinsi** akan diambil dari tabel Postgres `public.wilayah` (jika tersedia).")

    provinsi_options = load_provinsi_options_pg()

    with st.form(key="identitas::form", clear_on_submit=True):
        # HMHI cabang = pilihan Provinsi (jika tabel wilayah ada); kalau tidak, fallback input teks
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
        submitted = st.form_submit_button("💾 Simpan ke Supabase")

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
        if hmhi_val and hmhi_cabang_sudah_ada_pg(hmhi_val):
            errs.append(f"Data untuk HMHI cabang/Provinsi **{hmhi_val}** sudah pernah diinput.")

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
                insert_row_pg(payload)
                st.success(f"Data berhasil disimpan ke Supabase untuk HMHI cabang/Provinsi **{hmhi_val}**.")
            except Exception as e:
                st.error(f"Gagal menyimpan ke Supabase: {e}")

with tab_data:
    st.subheader("📄 Data Tersimpan")
    st.caption("Sumber data: **Supabase** (`public.identitas_organisasi`).")

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

    # ===== Data grid saat ini (Postgres) =====
    try:
        df = read_data_pg(limit=1000)
    except Exception as e:
        st.error(f"Gagal membaca dari Supabase: {e}")
        df = pd.DataFrame()

    if df.empty:
        st.info("Belum ada data.")
    else:
        existing_cols = [c for c in ORDER_COLS if c in df.columns]
        df_view = df[[c for c in existing_cols if c not in HIDE_COLS]].rename(columns=ALIAS_MAP)
        st.dataframe(df_view, use_container_width=True)

    # ===== Upload Excel → simpan ke Supabase =====
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

        # Proses unggah ke Supabase
        if st.button("🚀 Proses & Simpan ke Supabase", type="primary", key="identitas::process"):
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
                    insert_row_excel_pg(payload, kode_opt=kode)

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

    # ===== Unduh Excel data terkini (Supabase) =====
    st.markdown("### ⬇️ Unduh Data Saat Ini")
    df_now = df if 'df' in locals() else pd.DataFrame()
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
    else:
        st.info("Tidak ada data untuk diunduh.")
