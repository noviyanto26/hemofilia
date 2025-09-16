import streamlit as st
import pandas as pd
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Replacement Therapy", page_icon="🧪", layout="wide")
st.title("🧪 Ketersediaan Produk Replacement Therapy")

# ======================== Koneksi DB (pakai helper seperti referensi) ========================
# Mengikuti pola file referensi: import helper dari db.py
from db import fetch_df as pg_fetch_df, exec_sql as pg_exec_sql  # <- sama seperti file "Identitas Organisasi"

# ======================== Konstanta Tabel ========================
TABLE = "public.ketersediaan_produk_replacement"
TABLE_ORG = "public.identitas_organisasi"

# ======================== Helpers DB (insert / read) ========================
def insert_row(table_name: str, payload: dict, kode_organisasi: str):
    """
    Insert baris baru. Kolom created_at akan menggunakan default NOW() di Postgres.
    Skema kolom baru:
      id, kode_organisasi, created_at, produk, ketersediaan, digunakan, merk,
      jumlah_pengguna, jumlah_iu_per_kemasan, harga, perkiraan_penggunaan_tahun
    """
    sql = f"""
        INSERT INTO {table_name}
            (kode_organisasi, produk, ketersediaan, digunakan, merk,
             jumlah_pengguna, jumlah_iu_per_kemasan, harga, perkiraan_penggunaan_tahun)
        VALUES
            (:kode_organisasi, :produk, :ketersediaan, :digunakan, :merk,
             :jumlah_pengguna, :jumlah_iu_per_kemasan, :harga, :perkiraan_penggunaan_tahun)
    """
    params = {
        "kode_organisasi": kode_organisasi,
        "produk": payload.get("produk"),
        "ketersediaan": payload.get("ketersediaan"),
        "digunakan": payload.get("digunakan"),
        "merk": payload.get("merk"),
        "jumlah_pengguna": int(payload.get("jumlah_pengguna") or 0),
        "jumlah_iu_per_kemasan": int(payload.get("jumlah_iu_per_kemasan") or 0),
        "harga": float(payload.get("harga") or 0.0),
        "perkiraan_penggunaan_tahun": int(payload.get("perkiraan_penggunaan_tahun") or 0),
    }
    pg_exec_sql(sql, params)

def read_with_kota(table_name: str, limit=1000):
    """
    Ambil data + join identitas_organisasi untuk hmhi_cabang dan kota_cakupan_cabang.
    """
    sql = f"""
        SELECT
          t.id, t.kode_organisasi, t.created_at,
          t.produk, t.ketersediaan, t.digunakan, t.merk,
          t.jumlah_pengguna, t.jumlah_iu_per_kemasan, t.harga,
          t.perkiraan_penggunaan_tahun,
          io.hmhi_cabang, io.kota_cakupan_cabang
        FROM {table_name} t
        LEFT JOIN {TABLE_ORG} io ON io.kode_organisasi = t.kode_organisasi
        ORDER BY t.id DESC
        LIMIT :lim
    """
    return pg_fetch_df(sql, {"lim": int(limit)})

# ======================== Helpers UI/Input ========================
def load_hmhi_to_kode():
    """
    Ambil HMHI cabang → kode_organisasi dari identitas_organisasi.
    Return:
        mapping: dict {hmhi_cabang -> kode_organisasi}
        options: list hmhi_cabang (urut alfabet)
    """
    try:
        df = pg_fetch_df(
            f"SELECT hmhi_cabang, kode_organisasi FROM {TABLE_ORG} "
            "WHERE hmhi_cabang IS NOT NULL AND hmhi_cabang <> '' "
            "ORDER BY id DESC"
        )
        if df.empty:
            return {}, []
        mapping = {}
        for _, row in df.iterrows():
            hmhi = (str(row["hmhi_cabang"]).strip() if pd.notna(row["hmhi_cabang"]) else "")
            kode = (str(row["kode_organisasi"]).strip() if pd.notna(row["kode_organisasi"]) else "")
            if hmhi:
                mapping[hmhi] = kode
        return mapping, sorted(mapping.keys())
    except Exception:
        return {}, []

def safe_int(val, default=0):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return default
        return int(x)
    except Exception:
        return default

def safe_float(val, default=0.0):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default

# ======================== Konstanta UI ========================
PRODUK_OPTIONS = [
    "Plasma (FFP)",
    "Cryoprecipitate",
    "Konsentrat (plasma derived)",
    "Konsentrat (rekombinan)",
    "Konsentrat (prolonged half life)",
    "Prothrombin Complex",
    "DDAVP",
    "Emicizumab (Hemlibra)",
    "Konsentrat Bypassing Agent",
]
KETERSEDIAAN_OPTIONS = ["Tersedia", "Tidak Tersedia"]
YA_TIDAK_OPTIONS = ["Ya", "Tidak"]

# ======================== Template Unggah ========================
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Produk",
    "Ketersediaan",
    "Digunakan",
    "Merk",
    "Jumlah Pengguna",
    "Jumlah iu/vial per kemasan",
    "Harga",
    "Perkiraan Jumlah Penggunaan/Tahun",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",            # hanya untuk mapping ke kode_organisasi
    "Produk": "produk",
    "Ketersediaan": "ketersediaan",
    "Digunakan": "digunakan",
    "Merk": "merk",
    "Jumlah Pengguna": "jumlah_pengguna",
    "Jumlah iu/vial per kemasan": "jumlah_iu_per_kemasan",
    "Harga": "harga",
    "Perkiraan Jumlah Penggunaan/Tahun": "perkiraan_penggunaan_tahun",
}

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # --- Pilihan dari HMHI Cabang ---
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="rt::hmhi_select"
        )

    st.subheader("Form Ketersediaan Produk Replacement Therapy")

    # Template data awal (dinamis)
    df_default = pd.DataFrame({
        "produk": ["", "", "", "", ""],
        "ketersediaan": ["", "", "", "", ""],
        "digunakan": ["", "", "", "", ""],
        "merk": ["", "", "", "", ""],
        "jumlah_pengguna": [0, 0, 0, 0, 0],
        "jumlah_iu_per_kemasan": [0, 0, 0, 0, 0],
        "harga": [0.0, 0.0, 0.0, 0.0, 0.0],
        "perkiraan_penggunaan_tahun": [0, 0, 0, 0, 0],
    })

    with st.form("rt::form"):
        ed = st.data_editor(
            df_default,
            key="rt::editor",
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
            column_config={
                "produk": st.column_config.SelectboxColumn(
                    "Produk",
                    options=[""] + PRODUK_OPTIONS,
                    required=False
                ),
                "ketersediaan": st.column_config.SelectboxColumn(
                    "Ketersediaan",
                    options=[""] + KETERSEDIAAN_OPTIONS,
                    required=False
                ),
                "digunakan": st.column_config.SelectboxColumn(
                    "Digunakan",
                    options=[""] + YA_TIDAK_OPTIONS,
                    required=False
                ),
                "merk": st.column_config.TextColumn("Merk", help="Contoh: merek dagang/brand produk"),
                "jumlah_pengguna": st.column_config.NumberColumn(
                    "Jumlah Pengguna", min_value=0, step=1
                ),
                "jumlah_iu_per_kemasan": st.column_config.NumberColumn(
                    "Jumlah iu/vial dalam 1 kemasan", min_value=0, step=1
                ),
                "harga": st.column_config.NumberColumn(
                    "Harga", help="Nilai numerik (contoh: 1250000)", min_value=0.0, step=1000.0, format="%.0f"
                ),
                "perkiraan_penggunaan_tahun": st.column_config.NumberColumn(
                    "Perkiraan Jumlah Penggunaan/Tahun", min_value=0, step=1
                ),
            },
        )
        submit = st.form_submit_button("💾 Simpan")

    if submit:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                n_saved = 0
                for _, row in ed.iterrows():
                    produk = str(row.get("produk") or "").strip()
                    ketersediaan = str(row.get("ketersediaan") or "").strip()
                    digunakan = str(row.get("digunakan") or "").strip()
                    merk = str(row.get("merk") or "").strip()
                    jml_pengguna = safe_int(row.get("jumlah_pengguna", 0))
                    jml_iu = safe_int(row.get("jumlah_iu_per_kemasan", 0))
                    harga = safe_float(row.get("harga", 0.0))
                    perkir_tahun = safe_int(row.get("perkiraan_penggunaan_tahun", 0))

                    # baris kosong dilewati
                    is_all_empty = (
                        not produk and not ketersediaan and not digunakan and not merk
                        and jml_pengguna == 0 and jml_iu == 0 and harga == 0.0 and perkir_tahun == 0
                    )
                    if is_all_empty:
                        continue

                    payload = {
                        "produk": produk,
                        "ketersediaan": ketersediaan,
                        "digunakan": digunakan,
                        "merk": merk,
                        "jumlah_pengguna": jml_pengguna,
                        "jumlah_iu_per_kemasan": jml_iu,
                        "harga": harga,
                        "perkiraan_penggunaan_tahun": perkir_tahun,
                    }
                    insert_row(TABLE, payload, kode_organisasi)
                    n_saved += 1

                if n_saved > 0:
                    st.success(f"{n_saved} baris tersimpan untuk **{selected_hmhi}**.")
                else:
                    st.info("Tidak ada baris valid untuk disimpan.")

with tab_data:
    st.subheader("📄 Data Tersimpan — Replacement Therapy")
    df = read_with_kota(TABLE, limit=1000)
    if df.empty:
        st.info("Belum ada data tersimpan.")
    else:
        # Susun tampilan kolom
        cols_order = [
            "hmhi_cabang", "kota_cakupan_cabang", "created_at",
            "produk", "ketersediaan", "digunakan", "merk",
            "jumlah_pengguna", "jumlah_iu_per_kemasan", "harga",
            "perkiraan_penggunaan_tahun"
        ]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "produk": "Produk",
            "ketersediaan": "Ketersediaan",
            "digunakan": "Digunakan",
            "merk": "Merk",
            "jumlah_pengguna": "Jumlah Pengguna",
            "jumlah_iu_per_kemasan": "Jumlah iu/vial per kemasan",
            "harga": "Harga",
            "perkiraan_penggunaan_tahun": "Perkiraan Jumlah Penggunaan/Tahun",
        })

        # Format kolom Harga ke tampilan ribuan (tanpa mengubah data asli)
        view_fmt = view.copy()
        if "Harga" in view_fmt.columns:
            def _fmt_harga(x):
                try:
                    xi = int(float(x))
                    return f"{xi:,}".replace(",", ".")
                except Exception:
                    return x
            view_fmt["Harga"] = view_fmt["Harga"].apply(_fmt_harga)

        st.dataframe(view_fmt, use_container_width=True)

        # Unduh Excel (pakai data asli, bukan string terformat)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="ReplacementTherapy")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buf.getvalue(),
            file_name="ketersediaan_produk_replacement_therapy.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="rt::download"
        )

    st.divider()
    st.markdown("### 📥 Template & Unggah Excel")

    # --- Unduh Template Kosong
    template_df = pd.DataFrame([{
        "HMHI cabang": "",
        "Produk": "",
        "Ketersediaan": "",
        "Digunakan": "",
        "Merk": "",
        "Jumlah Pengguna": 0,
        "Jumlah iu/vial per kemasan": 0,
        "Harga": 0,
        "Perkiraan Jumlah Penggunaan/Tahun": 0,
    }], columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        template_df.to_excel(w, index=False, sheet_name="Template_RT")
    st.download_button(
        "📄 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_replacement_therapy.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="rt::dl_template"
    )

    # --- Unggah File
    up = st.file_uploader(
        "Unggah file Excel (.xlsx) sesuai template",
        type=["xlsx"],
        key="rt::uploader"
    )

    def process_upload(df_up: pd.DataFrame):
        """Validasi & simpan unggahan. Kembalikan (log_df, n_ok)."""
        hmhi_map, _ = load_hmhi_to_kode()
        results = []
        n_ok = 0

        for i in range(len(df_up)):
            try:
                s = df_up.iloc[i]  # Series -> punya .get
                hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                if not hmhi:
                    raise ValueError("Kolom 'HMHI cabang' kosong.")
                kode_organisasi = hmhi_map.get(hmhi)
                if not kode_organisasi:
                    raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                produk = str((s.get("produk") or "")).strip()
                if produk and produk not in PRODUK_OPTIONS:
                    raise ValueError(f"Produk tidak valid: '{produk}'")

                ketersediaan = str((s.get("ketersediaan") or "")).strip()
                if ketersediaan and ketersediaan not in KETERSEDIAAN_OPTIONS:
                    raise ValueError("Nilai 'Ketersediaan' harus 'Tersedia' atau 'Tidak Tersedia'")

                digunakan = str((s.get("digunakan") or "")).strip()
                if digunakan and digunakan not in YA_TIDAK_OPTIONS:
                    raise ValueError("Nilai 'Digunakan' harus 'Ya' atau 'Tidak'")

                merk = str((s.get("merk") or "")).strip()
                jml_pengguna = safe_int(s.get("jumlah_pengguna", 0))
                jml_iu       = safe_int(s.get("jumlah_iu_per_kemasan", 0))
                harga_val    = safe_float(s.get("harga", 0.0))
                perkir_tahun = safe_int(s.get("perkiraan_penggunaan_tahun", 0))

                # jika benar-benar kosong, lewati
                is_all_empty = (
                    not produk and not ketersediaan and not digunakan and not merk
                    and jml_pengguna == 0 and jml_iu == 0 and harga_val == 0.0 and perkir_tahun == 0
                )
                if is_all_empty:
                    results.append({"Baris Excel": i+2, "Status": "LEWAT", "Keterangan": "Baris kosong — dilewati"})
                    continue

                payload = {
                    "produk": produk,
                    "ketersediaan": ketersediaan,
                    "digunakan": digunakan,
                    "merk": merk,
                    "jumlah_pengguna": jml_pengguna,
                    "jumlah_iu_per_kemasan": jml_iu,
                    "harga": harga_val,
                    "perkiraan_penggunaan_tahun": perkir_tahun,
                }
                insert_row(TABLE, payload, kode_organisasi)
                results.append({"Baris Excel": i+2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {produk or '(tanpa produk)'}"})
                n_ok += 1
            except Exception as e:
                results.append({"Baris Excel": i+2, "Status": "GAGAL", "Keterangan": str(e)})

        return pd.DataFrame(results), n_ok

    if up is not None:
        try:
            raw = pd.read_excel(up)
            raw.columns = [str(c).strip() for c in raw.columns]
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        # Validasi header minimal
        minimal = ["HMHI cabang"]
        missing = [c for c in minimal if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Minimal harus ada: " + ", ".join(minimal))
            st.stop()

        # Pastikan semua kolom template tersedia (tambahkan jika tidak ada)
        for c in TEMPLATE_COLUMNS:
            if c not in raw.columns:
                raw[c] = ""

        st.caption("Pratinjau 20 baris pertama:")
        st.dataframe(raw[TEMPLATE_COLUMNS].head(20), use_container_width=True)

        # Ubah header ke nama kolom database
        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        if st.button("🚀 Proses & Simpan Unggahan", type="primary", key="rt::process"):
            log_df, n_ok = process_upload(df_up)
            st.write("**Hasil unggah:**")
            st.dataframe(log_df, use_container_width=True)

            ok = (log_df["Status"] == "OK").sum()
            fail = (log_df["Status"] == "GAGAL").sum()
            skip = (log_df["Status"] == "LEWAT").sum()
            if ok:
                st.success(f"Berhasil menyimpan {ok} baris.")
            if fail:
                st.error(f"Gagal menyimpan {fail} baris.")
            if skip:
                st.info(f"Dilewati {skip} baris kosong.")

            # Unduh log hasil
            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                log_df.to_excel(w, index=False, sheet_name="Hasil")
            st.download_button(
                "📄 Unduh Log Hasil",
                log_buf.getvalue(),
                file_name="log_hasil_unggah_replacement_therapy.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="rt::dl_log"
            )
