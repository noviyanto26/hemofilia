# 2_jumlah_individu_hemofilia.py
import streamlit as st
import pandas as pd
import io
from datetime import datetime, date

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="🩸 Jumlah Individu Hemofilia", page_icon="🩸", layout="wide")
st.title("🩸 Jumlah Individu Hemofilia")

# ======================== Target Supabase (Postgres) =================
SUPABASE_TABLE = "public.jumlah_individu_hemofilia"
ORG_TABLE      = "public.identitas_organisasi"

# Konektor ke Postgres (mengikuti pola di 1_identitas_organisasi.py)
from db import fetch_df as pg_fetch_df, exec_sql as pg_exec_sql, safe_url

# ======================== Struktur Kolom (DB) ========================
# Struktur baru: tanpa hemofilia_a & hemofilia_b; gunakan jumlah_total_ab
FIELDS = [
    ("jumlah_total_ab", "Jumlah total penyandang hemofilia A dan B"),
    ("hemofilia_lain", "Hemofilia lain/tidak dikenal"),
    ("terduga", "Terduga hemofilia/diagnosis belum ditegakkan"),
    ("vwd", "Von Willebrand Disease (vWD)"),
    ("lainnya", "Kelainan pembekuan darah genetik lainnya"),
]

# Alias tampilan (untuk template Excel & grid)
ALIAS_MAP = {
    "hmhi_cabang": "HMHI Cabang",
    "kota_cakupan_cabang": "Kota cakupan cabang",
    "jumlah_total_ab": "Jumlah total penyandang hemofilia A dan B",
    "hemofilia_lain": "Hemofilia lain/tidak dikenal",
    "terduga": "Terduga hemofilia/diagnosis belum ditegakkan",
    "vwd": "Von Willebrand Disease (vWD)",
    "lainnya": "Kelainan pembekuan darah genetik lainnya",
}
ORDER_COLS = [
    "hmhi_cabang",
    "kota_cakupan_cabang",
    "jumlah_total_ab",
    "hemofilia_lain",
    "terduga",
    "vwd",
    "lainnya",
]
HIDE_COLS = {"id", "kode_organisasi", "created_at"}

# ======================== Util: Normalisasi & Validasi =================
def _to_int_nonneg(x):
    """
    Konversi ke int >= 0. Izinkan '', None → 0.
    Jika bukan angka >=0 → raise ValueError.
    """
    if x is None or (isinstance(x, float) and pd.isna(x)) or (isinstance(x, str) and not x.strip()):
        return 0
    try:
        v = int(float(str(x).strip()))
    except Exception:
        raise ValueError(f"Nilai numerik tidak valid: {x}")
    if v < 0:
        raise ValueError(f"Nilai tidak boleh negatif: {x}")
    return v

def _load_hmhi_options():
    """Ambil daftar HMHI cabang dari identitas_organisasi (Supabase)."""
    try:
        df = pg_fetch_df(f"SELECT hmhi_cabang FROM {ORG_TABLE} ORDER BY hmhi_cabang")
        opts = df["hmhi_cabang"].dropna().astype(str).tolist()
        return opts
    except Exception:
        return []

def _kode_for_hmhi(hmhi: str) -> str | None:
    """Ambil kode_organisasi berdasarkan hmhi_cabang; None jika tidak ketemu."""
    if not hmhi:
        return None
    df = pg_fetch_df(
        f"SELECT kode_organisasi FROM {ORG_TABLE} WHERE hmhi_cabang = :h LIMIT 1",
        {"h": hmhi},
    )
    if df.empty:
        return None
    return str(df.iloc[0]["kode_organisasi"])

# ======================== CRUD: Postgres ========================
def insert_row_pg(kode_organisasi: str, payload: dict):
    """
    INSERT 1 baris ke public.jumlah_individu_hemofilia.
    created_at diisi NOW() (server).
    """
    params = {
        "kode": kode_organisasi,
        "jumlah_total_ab": _to_int_nonneg(payload.get("jumlah_total_ab")),
        "hemofilia_lain": _to_int_nonneg(payload.get("hemofilia_lain")),
        "terduga": _to_int_nonneg(payload.get("terduga")),
        "vwd": _to_int_nonneg(payload.get("vwd")),
        "lainnya": _to_int_nonneg(payload.get("lainnya")),
    }
    sql = f"""
        INSERT INTO {SUPABASE_TABLE} (
            kode_organisasi, created_at,
            jumlah_total_ab, hemofilia_lain, terduga, vwd, lainnya
        )
        VALUES (
            :kode, NOW(),
            :jumlah_total_ab, :hemofilia_lain, :terduga, :vwd, :lainnya
        )
    """
    pg_exec_sql(sql, params)

def bulk_insert_from_excel(df_up: pd.DataFrame) -> pd.DataFrame:
    """
    Proses unggah DataFrame (kolom alias) → insert ke Supabase.
    Mengembalikan DataFrame hasil (Baris, Status, Keterangan).
    """
    results = []
    for idx, row in df_up.iterrows():
        try:
            hmhi = str(row.get("hmhi_cabang", "") or "").strip()
            if not hmhi:
                raise ValueError("HMHI Cabang kosong.")
            kode = _kode_for_hmhi(hmhi)
            if not kode:
                raise ValueError(f"HMHI Cabang '{hmhi}' belum terdaftar di identitas_organisasi.")

            payload = {
                "jumlah_total_ab": row.get("jumlah_total_ab", 0),
                "hemofilia_lain": row.get("hemofilia_lain", 0),
                "terduga": row.get("terduga", 0),
                "vwd": row.get("vwd", 0),
                "lainnya": row.get("lainnya", 0),
            }
            # validasi numerik via insert_row_pg (akan raise bila invalid)
            insert_row_pg(kode, payload)

            results.append({"Baris": idx + 2, "Status": "OK", "Keterangan": f"Simpan: {hmhi}"})
        except Exception as e:
            results.append({"Baris": idx + 2, "Status": "GAGAL", "Keterangan": str(e)})
    return pd.DataFrame(results)

def read_with_kota(limit=500) -> pd.DataFrame:
    """
    Baca data gabungan (JOIN) dari Supabase untuk ditampilkan.
    """
    sql = f"""
        SELECT
            j.id,
            j.kode_organisasi,
            j.created_at,
            j.jumlah_total_ab,
            j.hemofilia_lain,
            j.terduga,
            j.vwd,
            j.lainnya,
            io.kota_cakupan_cabang,
            io.hmhi_cabang
        FROM {SUPABASE_TABLE} j
        LEFT JOIN {ORG_TABLE} io
               ON io.kode_organisasi = j.kode_organisasi
        ORDER BY j.id DESC
        LIMIT {int(limit)}
    """
    return pg_fetch_df(sql)

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data Tersimpan"])

# --------- Tab Input ---------
with tab_input:
    st.caption("Isi data jumlah individu hemofilia. Organisasi diambil dari **Supabase** (`public.identitas_organisasi`).")

    hmhi_options = _load_hmhi_options()
    if not hmhi_options:
        st.warning("Belum ada data **Identitas Organisasi**. Silakan isi terlebih dahulu di halaman terkait.")
    with st.form("jumlah_individu::form", clear_on_submit=True):
        hmhi_cabang = st.selectbox("HMHI Cabang (Provinsi)", options=hmhi_options, index=0 if hmhi_options else None)

        c1, c2, c3 = st.columns(3)
        with c1:
            jumlah_total_ab = st.number_input(ALIAS_MAP["jumlah_total_ab"], min_value=0, step=1, value=0)
            hemofilia_lain = st.number_input(ALIAS_MAP["hemofilia_lain"], min_value=0, step=1, value=0)
        with c2:
            terduga = st.number_input(ALIAS_MAP["terduga"], min_value=0, step=1, value=0)
            vwd = st.number_input(ALIAS_MAP["vwd"], min_value=0, step=1, value=0)
        with c3:
            lainnya = st.number_input(ALIAS_MAP["lainnya"], min_value=0, step=1, value=0)

        submitted = st.form_submit_button("💾 Simpan ke Supabase")

    if submitted:
        if not hmhi_cabang:
            st.error("HMHI Cabang wajib dipilih.")
        else:
            kode = _kode_for_hmhi(hmhi_cabang)
            if not kode:
                st.error(f"HMHI Cabang **{hmhi_cabang}** belum terdaftar di identitas_organisasi.")
            else:
                try:
                    insert_row_pg(
                        kode,
                        {
                            "jumlah_total_ab": jumlah_total_ab,
                            "hemofilia_lain": hemofilia_lain,
                            "terduga": terduga,
                            "vwd": vwd,
                            "lainnya": lainnya,
                        },
                    )
                    st.success(f"Data berhasil disimpan untuk **{hmhi_cabang}**.")
                except Exception as e:
                    st.error(f"Gagal menyimpan: {e}")

# --------- Tab Data ---------
with tab_data:
    st.subheader("📄 Data Tersimpan")
    st.caption("Sumber data: **Supabase** (`public.jumlah_individu_hemofilia` bergabung dengan `public.identitas_organisasi`).")

    # ====== Unduh Template Excel ======
    st.caption("Format unggahan harus memiliki header persis seperti di bawah ini.")
    template_cols_alias = [
        ALIAS_MAP["hmhi_cabang"],
        ALIAS_MAP["kota_cakupan_cabang"],
        ALIAS_MAP["jumlah_total_ab"],
        ALIAS_MAP["hemofilia_lain"],
        ALIAS_MAP["terduga"],
        ALIAS_MAP["vwd"],
        ALIAS_MAP["lainnya"],
    ]
    tmpl_buf = io.BytesIO()
    with pd.ExcelWriter(tmpl_buf, engine="xlsxwriter") as writer:
        pd.DataFrame(columns=template_cols_alias).to_excel(writer, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        tmpl_buf.getvalue(),
        file_name="template_jumlah_individu_hemofilia.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="jumlah_individu::template"
    )

    # ====== Grid data saat ini ======
    try:
        df = read_with_kota(limit=1000)
    except Exception as e:
        st.error(f"Gagal membaca dari Supabase: {e}")
        df = pd.DataFrame()

    if df.empty:
        st.info("Belum ada data.")
    else:
        # susun tampilan: sembunyikan kolom teknis; tampilkan alias
        cols_show = []
        # urutan tampilan yang nyaman
        preferred = [
            "hmhi_cabang",
            "kota_cakupan_cabang",
            "jumlah_total_ab",
            "hemofilia_lain",
            "terduga",
            "vwd",
            "lainnya",
        ]
        # tambahkan kolom preferensial jika ada
        for c in preferred:
            if c in df.columns:
                cols_show.append(c)
        # tambahkan sisanya
        for c in df.columns:
            if c not in HIDE_COLS and c not in cols_show:
                cols_show.append(c)

        df_view = df[cols_show].rename(columns=ALIAS_MAP)
        st.dataframe(df_view, use_container_width=True)

    # ====== Unggah Excel → simpan ke Supabase ======
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader("Pilih file Excel (.xlsx) dengan header sesuai template", type=["xlsx"], key="jumlah_individu::uploader")

    if up is not None:
        try:
            raw = pd.read_excel(up)
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        expected_alias = template_cols_alias
        missing = [c for c in expected_alias if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Kolom yang belum ada: " + ", ".join(missing))
            st.stop()

        # Map alias → nama kolom DB internal
        reverse_alias = {v: k for k, v in ALIAS_MAP.items()}
        df_up = raw.rename(columns=reverse_alias)

        # Normalisasi tipe dasar: NaN → '', trim string
        for c in df_up.columns:
            df_up[c] = df_up[c].astype(object).where(pd.notna(df_up[c]), "")

        # Preview
        st.caption("Pratinjau data yang akan diproses:")
        st.dataframe(raw.head(20), use_container_width=True)

        if st.button("🚀 Proses & Simpan ke Supabase", type="primary", key="jumlah_individu::process"):
            try:
                res_df = bulk_insert_from_excel(df_up)
                st.write("**Hasil unggah:**")
                st.dataframe(res_df, use_container_width=True)

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
                    file_name="log_hasil_unggah_jumlah_individu.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="jumlah_individu::logdownload"
                )
            except Exception as e:
                st.error(f"Gagal memproses unggahan: {e}")

    # ====== Unduh Excel data terkini ======
    st.markdown("### ⬇️ Unduh Data Saat Ini")
    if df is not None and not df.empty:
        export_cols = [c for c in ORDER_COLS if c in df.columns]
        df_export = df[export_cols].rename(columns=ALIAS_MAP)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
            df_export.to_excel(writer, index=False, sheet_name="JumlahIndividu")
        st.download_button(
            "⬇️ Unduh Excel (Data Terkini)",
            buffer.getvalue(),
            file_name="jumlah_individu_hemofilia.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="jumlah_individu::download"
        )
    else:
        st.info("Tidak ada data untuk diunduh.")
