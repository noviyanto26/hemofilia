import streamlit as st
import pandas as pd
from datetime import datetime
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Informasi Donasi", page_icon="🎁", layout="wide")
st.title("🎁 Informasi Donasi")

# ======================== Koneksi DB (pakai helper seperti referensi) ========================
# Mengikuti pola file referensi: gunakan helper dari db.py (Supabase/Postgres)
from db import fetch_df as pg_fetch_df, exec_sql as pg_exec_sql  # ← sama seperti referensi

TABLE      = "public.informasi_donasi"
TABLE_ORG  = "public.identitas_organisasi"

# ======================== Label statis untuk Jenis Donasi ========================
JENIS_DONASI_LABELS = [
    "Konsentrat Faktor VIII",
    "Konsentrat Faktor IX",
    "Bypassing Agent",
]

# ======================== Helper umum ========================
def safe_float(val, default=0.0):
    try:
        x = pd.to_numeric(val, errors="coerce")
        return default if pd.isna(x) else float(x)
    except Exception:
        return default

def load_hmhi_to_kode():
    """
    Ambil mapping HMHI cabang -> kode_organisasi dari public.identitas_organisasi.
    Return:
        mapping: dict {hmhi_cabang -> kode_organisasi}
        options: list[str] hmhi_cabang (urut alfabet)
    """
    try:
        df = pg_fetch_df(f"""
            SELECT hmhi_cabang, kode_organisasi
            FROM {TABLE_ORG}
            WHERE COALESCE(hmhi_cabang, '') <> ''
            ORDER BY id DESC
        """)
        if df.empty:
            return {}, []
        mapping = {}
        for _, row in df.iterrows():
            hmhi = (str(row["hmhi_cabang"]).strip() if pd.notna(row["hmhi_cabang"]) else "")
            kode = (str(row["kode_organisasi"]).strip() if pd.notna(row["kode_organisasi"]) else "")
            if hmhi and kode:
                mapping[hmhi] = kode
        return mapping, sorted(mapping.keys())
    except Exception:
        return {}, []

def insert_row(payload: dict, kode_organisasi: str):
    """
    Insert baris baru ke public.informasi_donasi.
    Kolom created_at menggunakan NOW() di Postgres.
    Skema:
      id, kode_organisasi, created_at, jenis_donasi, merk, jumlah_total_iu_setahun, kegunaan
    """
    sql = f"""
        INSERT INTO {TABLE} (
            kode_organisasi, created_at,
            jenis_donasi, merk, jumlah_total_iu_setahun, kegunaan
        ) VALUES (
            :kode_organisasi, NOW(),
            :jenis_donasi, :merk, :jumlah_total_iu_setahun, :kegunaan
        )
    """
    params = {
        "kode_organisasi": kode_organisasi,
        "jenis_donasi": payload.get("jenis_donasi"),
        "merk": payload.get("merk"),
        "jumlah_total_iu_setahun": safe_float(payload.get("jumlah_total_iu_setahun"), 0.0),
        "kegunaan": payload.get("kegunaan"),
    }
    pg_exec_sql(sql, params)

def read_with_label(limit=1000):
    """
    Ambil data + join identitas_organisasi untuk hmhi_cabang.
    """
    lim = int(limit)
    sql = f"""
        SELECT
          t.id, t.created_at,
          t.jenis_donasi, t.merk, t.jumlah_total_iu_setahun, t.kegunaan,
          io.hmhi_cabang
        FROM {TABLE} t
        LEFT JOIN {TABLE_ORG} io
          ON io.kode_organisasi = t.kode_organisasi
        ORDER BY t.id DESC
        LIMIT :lim
    """
    return pg_fetch_df(sql, {"lim": lim})

# ======================== Template Unggah ========================
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Jenis Donasi",
    "Merk",
    "Jumlah Total (IU) Setahun",
    "Kegunaan",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",         # untuk mapping ke kode_organisasi
    "Jenis Donasi": "jenis_donasi",
    "Merk": "merk",
    "Jumlah Total (IU) Setahun": "jumlah_total_iu_setahun",
    "Kegunaan": "kegunaan",
}

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="idn::hmhi_select"
        )

    st.subheader("Form Informasi Donasi")

    # 3 baris default sesuai jenis donasi, kolom jenis dikunci
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
            column_config={
                "jenis_donasi": st.column_config.TextColumn("Jenis Donasi"),
                "merk": st.column_config.TextColumn("Merk"),
                "jumlah_total_iu_setahun": st.column_config.NumberColumn(
                    "Jumlah Total (IU) Setahun", min_value=0.0, step=100.0, format="%.0f"
                ),
                "kegunaan": st.column_config.TextColumn("Kegunaan"),
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
                    # lewati baris benar-benar kosong
                    if not row.get("merk") and safe_float(row.get("jumlah_total_iu_setahun"), 0) == 0 and not row.get("kegunaan"):
                        continue
                    payload = {
                        "jenis_donasi": row.get("jenis_donasi"),
                        "merk": row.get("merk"),
                        "jumlah_total_iu_setahun": safe_float(row.get("jumlah_total_iu_setahun"), 0.0),
                        "kegunaan": row.get("kegunaan"),
                    }
                    try:
                        insert_row(payload, kode_organisasi)
                        n_saved += 1
                    except Exception as e:
                        st.error(f"Gagal menyimpan baris ({payload['jenis_donasi']}): {e}")

                if n_saved > 0:
                    st.success(f"{n_saved} baris tersimpan untuk **{selected_hmhi}**.")
                else:
                    st.info("Tidak ada baris valid untuk disimpan.")

with tab_data:
    st.subheader("📄 Data Tersimpan — Informasi Donasi")

    # ====== Unggah Excel (opsional) ======
    up = st.file_uploader("⬆️ Unggah Excel (.xlsx) sesuai template", type=["xlsx"], key="idn::uploader")

    def process_upload(df_up: pd.DataFrame):
        """Validasi & simpan unggahan. Return (log_df, n_ok)."""
        hmhi_map, _ = load_hmhi_to_kode()
        results, n_ok = [], 0
        for i in range(len(df_up)):
            try:
                s = df_up.iloc[i]
                hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                if not hmhi:
                    raise ValueError("Kolom 'HMHI cabang' kosong.")
                kode_organisasi = hmhi_map.get(hmhi)
                if not kode_organisasi:
                    raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan.")

                jenis = str((s.get("jenis_donasi") or "")).strip()
                merk  = str((s.get("merk") or "")).strip()
                total = safe_float(s.get("jumlah_total_iu_setahun"), 0.0)
                guna  = str((s.get("kegunaan") or "")).strip()

                if not jenis and not merk and total == 0 and not guna:
                    results.append({"Baris Excel": i+2, "Status": "LEWAT", "Keterangan": "Baris kosong — dilewati"})
                    continue

                insert_row({
                    "jenis_donasi": jenis, "merk": merk,
                    "jumlah_total_iu_setahun": total, "kegunaan": guna
                }, kode_organisasi)

                results.append({"Baris Excel": i+2, "Status": "OK",
                                "Keterangan": f"Simpan → {hmhi} / {jenis or '(tanpa jenis)'}"})
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

        # Pastikan semua kolom template tersedia
        for c in TEMPLATE_COLUMNS:
            if c not in raw.columns:
                raw[c] = "" if c not in ["Jumlah Total (IU) Setahun"] else 0

        # Normalisasi header → kolom DB internal
        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        st.caption("Pratinjau 20 baris pertama:")
        st.dataframe(raw[TEMPLATE_COLUMNS].head(20), use_container_width=True)

        if st.button("🚀 Proses & Simpan Unggahan", type="primary", key="idn::process"):
            log_df, _ = process_upload(df_up)
            st.write("**Hasil unggah:**")
            st.dataframe(log_df, use_container_width=True)

            ok = (log_df["Status"] == "OK").sum()
            fail = (log_df["Status"] == "GAGAL").sum()
            skip = (log_df["Status"] == "LEWAT").sum()
            if ok: st.success(f"Berhasil menyimpan {ok} baris.")
            if fail: st.error(f"Gagal menyimpan {fail} baris.")
            if skip: st.info(f"Dilewati {skip} baris kosong.")

            # Unduh log
            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                log_df.to_excel(w, index=False, sheet_name="Hasil")
            st.download_button(
                "📄 Unduh Log Hasil",
                log_buf.getvalue(),
                file_name="log_hasil_unggah_informasi_donasi.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="idn::dl_log"
            )

    # ====== Tabel data tersimpan ======
    try:
        df = read_with_label(limit=1000)
    except Exception as e:
        st.error(f"Gagal membaca data dari database: {e}")
        df = pd.DataFrame()

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
        }).copy()

        # Hindari masalah timezone saat ekspor
        if "Created At" in view.columns:
            view["Created At"] = view["Created At"].astype(str)

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
