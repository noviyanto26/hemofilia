import streamlit as st
import pandas as pd
from datetime import datetime
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Jumlah Individu Hemofilia", page_icon="🩸", layout="wide")
st.title("🩸 Jumlah Individu Hemofilia")

# ======================== Koneksi Postgres (Supabase) ========================
# Mengikuti pola referensi: gunakan helper dari db.py
from db import fetch_df as pg_fetch_df, exec_sql as pg_exec_sql

TABLE      = "public.jumlah_individu_hemofilia"
ORG_TABLE  = "public.identitas_organisasi"

# Struktur: tanpa hemofilia_a & hemofilia_b; gunakan jumlah_total_ab
FIELDS = [
    ("jumlah_total_ab", "Jumlah total penyandang hemofilia A dan B"),
    ("hemofilia_lain", "Hemofilia lain/tidak dikenal"),
    ("terduga", "Terduga hemofilia/diagnosis belum ditegakkan"),
    ("vwd", "Von Willebrand Disease (vWD)"),
    ("lainnya", "Kelainan pembekuan darah genetik lainnya"),
]

# ===== Template unggah (kota/provinsi tidak ada di template) =====
TEMPLATE_ALIAS_TO_DB = {
    "Kode Organisasi": "kode_organisasi",   # boleh kosong → dipetakan dari HMHI cabang
    "HMHI cabang": "hmhi_cabang_info",      # info untuk pemetaan (tidak disimpan di tabel ini)
    "Jumlah total penyandang hemofilia A dan B": "jumlah_total_ab",
    "Hemofilia lain/tidak dikenal": "hemofilia_lain",
    "Terduga hemofilia/diagnosis belum ditegakkan": "terduga",
    "Von Willebrand Disease (vWD)": "vwd",
    "Kelainan pembekuan darah genetik lainnya": "lainnya",
}
TEMPLATE_COLUMNS = list(TEMPLATE_ALIAS_TO_DB.keys())

# ======================== Helpers (Postgres) ========================
def load_hmhi_to_kode() -> dict:
    """hmhi_cabang -> kode_organisasi (unik) dari public.identitas_organisasi."""
    try:
        df = pg_fetch_df(f"""
            SELECT kode_organisasi, hmhi_cabang
            FROM {ORG_TABLE}
            WHERE COALESCE(hmhi_cabang,'') <> ''
            ORDER BY id DESC
        """)
        if df.empty:
            return {}
        return {str(r["hmhi_cabang"]).strip(): str(r["kode_organisasi"]).strip()
                for _, r in df.iterrows() if pd.notna(r["hmhi_cabang"])}
    except Exception:
        return {}

def kode_organisasi_exists(kode: str) -> bool:
    if not kode:
        return False
    df = pg_fetch_df(
        f"SELECT 1 FROM {ORG_TABLE} WHERE kode_organisasi = :k LIMIT 1",
        {"k": kode}
    )
    return not df.empty

def insert_row(values: dict, kode_organisasi: str):
    """Insert 1 baris; created_at diisi NOW() oleh server."""
    cols = ", ".join(values.keys())
    placeholders = ", ".join([f":{k}" for k in values.keys()])
    params = {"kode": kode_organisasi, **values}
    sql = f"""
        INSERT INTO {TABLE} (
            kode_organisasi, created_at, {cols}
        ) VALUES (
            :kode, NOW(), {placeholders}
        )
    """
    pg_exec_sql(sql, params)

def read_with_kota(limit=300) -> pd.DataFrame:
    sql = f"""
        SELECT
          j.id,
          j.kode_organisasi,
          j.created_at,
          {", ".join([f"j.{n}" for n, _ in FIELDS])},
          io.kota_cakupan_cabang,
          io.hmhi_cabang
        FROM {TABLE} j
        LEFT JOIN {ORG_TABLE} io ON io.kode_organisasi = j.kode_organisasi
        ORDER BY j.id DESC
        LIMIT :lim
    """
    return pg_fetch_df(sql, {"lim": int(limit)})

def to_nonneg_int(x) -> int:
    """Konversi ke int >=0; kosong/NaN -> 0, negatif -> 0."""
    try:
        if pd.isna(x) or str(x).strip() == "":
            return 0
        v = int(float(x))
        return max(v, 0)
    except Exception:
        return 0

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # Pilih HMHI cabang untuk input manual
    hmhi_to_kode = load_hmhi_to_kode()
    hmhi_list = sorted(hmhi_to_kode.keys())
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (hmhi_cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="jml::hmhi_select"
        )

    df_input = pd.DataFrame({
        "Jenis": [lbl for _, lbl in FIELDS],
        "Jumlah": [0]*len(FIELDS)
    })
    with st.form("jml::form"):
        edited = st.data_editor(
            df_input,
            key="jml::editor",
            column_config={"Jumlah": st.column_config.NumberColumn("Jumlah", min_value=0, step=1)},
            hide_index=True,
            use_container_width=True,
        )
        submitted = st.form_submit_button("💾 Simpan")

    if submitted:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_to_kode.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                values = {name: int(edited.loc[i, "Jumlah"]) for i, (name, _) in enumerate(FIELDS)}
                insert_row(values, kode_organisasi)
                st.success(f"Data untuk **{selected_hmhi}** berhasil disimpan.")

with tab_data:
    st.subheader("📄 Data Tersimpan")
    df_x = read_with_kota()

    # ===== Unduh Template Excel =====
    st.caption("Gunakan template berikut saat mengunggah data:")
    tmpl = pd.DataFrame(columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_jumlah_individu_hemofilia.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="jml::dl_template"
    )

    # ===== Tabel tampil (UI menyembunyikan Kode Organisasi & Kota/Provinsi) =====
    if df_x.empty:
        st.info("Belum ada data.")
    else:
        export_order = [
            "kode_organisasi",
            "hmhi_cabang",
            "kota_cakupan_cabang",
        ] + [n for n, _ in FIELDS]
        export_order = [c for c in export_order if c in df_x.columns]

        rename_map = {
            "kode_organisasi": "Kode Organisasi",
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            **{n: lbl for n, lbl in FIELDS},
        }

        export_view = df_x[export_order].rename(columns=rename_map)
        hide_cols_ui = {"Kode Organisasi", "Kota/Provinsi Cakupan Cabang"}
        display_cols = [c for c in export_view.columns if c not in hide_cols_ui]
        display_view = export_view[display_cols]
        st.dataframe(display_view, use_container_width=True)

        # Unduh data tersimpan (lengkap)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            export_view.to_excel(writer, index=False, sheet_name='Data Tersimpan')

        st.download_button(
            label="💾 Unduh Data sebagai Excel",
            data=output.getvalue(),
            file_name="data_tersimpan_jumlah_individu_hemofilia.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="jml::dl_data"
        )

    # ===== Unggah Excel =====
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan header persis seperti template",
        type=["xlsx"],
        key="jml::uploader"
    )

    if up is not None:
        try:
            raw = pd.read_excel(up)
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        missing = [c for c in TEMPLATE_COLUMNS if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Kolom yang belum ada: " + ", ".join(missing))
            st.stop()

        df_up = raw.rename(columns=TEMPLATE_ALIAS_TO_DB).copy()

        st.caption("Pratinjau 20 baris pertama dari file yang diunggah:")
        st.dataframe(raw.head(20), use_container_width=True)

        if st.button("🚀 Proses & Simpan", type="primary", key="jml::process"):
            hmhi_map = load_hmhi_to_kode()
            results = []

            for i, row in df_up.iterrows():
                try:
                    kode = str(row.get("kode_organisasi", "") or "").strip()
                    hmhi = str(row.get("hmhi_cabang_info", "") or "").strip()

                    # Tentukan kode_organisasi: prioritas kode, fallback hmhi_cabang
                    if kode:
                        if not kode_organisasi_exists(kode):
                            raise ValueError(f"Kode Organisasi '{kode}' tidak ditemukan di identitas_organisasi.")
                        if hmhi:
                            kode_by_hmhi = hmhi_map.get(hmhi)
                            if kode_by_hmhi and kode_by_hmhi != kode:
                                raise ValueError(f"Kode Organisasi tidak cocok dengan HMHI cabang ('{hmhi}').")
                    else:
                        if not hmhi:
                            raise ValueError("Kode Organisasi kosong dan HMHI cabang juga kosong.")
                        kode = hmhi_map.get(hmhi)
                        if not kode:
                            raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                    payload = {
                        "jumlah_total_ab": to_nonneg_int(row.get("jumlah_total_ab", 0)),
                        "hemofilia_lain": to_nonneg_int(row.get("hemofilia_lain", 0)),
                        "terduga": to_nonneg_int(row.get("terduga", 0)),
                        "vwd": to_nonneg_int(row.get("vwd", 0)),
                        "lainnya": to_nonneg_int(row.get("lainnya", 0)),
                    }

                    insert_row(payload, kode)
                    results.append({"Baris": i + 2, "Status": "OK", "Keterangan": f"Simpan → {kode} ({hmhi or '-'})"})
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
                file_name="log_hasil_unggah_jumlah_individu_hemofilia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="jml::dl_log"
            )
