import os
import io
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import text

from db import read_sql_df, exec_sql, get_engine, ping

IS_PG = (ping() == "postgresql")
TABLE = "kelompok_usia"
ORG_TABLE = "identitas_organisasi"

# =========================
# Konfigurasi & Konstanta
# =========================
st.set_page_config(page_title="Berdasarkan Kelompok Usia", page_icon="🩸", layout="wide")
st.title("🩸 Data Berdasarkan Kelompok Usia")

# Urutan kelompok usia untuk template & tampilan
AGE_GROUPS = ["0-4", "5-13", "14-18", "19-44", ">45", "Tidak ada data usia"]
TEMPLATE_AGE_ORDER = [">45", "19-44", "14-18", "5-13", "0-4"]

USIA_COLUMNS = [
    ("ha_ringan", "Hemofilia A - Ringan"),
    ("ha_sedang", "Hemofilia A - Sedang"),
    ("ha_berat",  "Hemofilia A - Berat"),
    ("hb_ringan", "Hemofilia B - Ringan"),
    ("hb_sedang", "Hemofilia B - Sedang"),
    ("hb_berat",  "Hemofilia B - Berat"),
    ("hemo_tipe_lain", "Hemofilia Tipe Lain"),
    ("vwd_tipe1", "vWD - Tipe 1"),
    ("vwd_tipe2", "vWD - Tipe 2"),
    ("vwd_tipe3", "vWD - Tipe 3"),
]

# ========== Template unggah ==========
TEMPLATE_COLUMNS = ["HMHI cabang", "Kelompok Usia"] + [lbl for _, lbl in USIA_COLUMNS]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Kelompok Usia": "kelompok_usia",
    **{lbl: key for key, lbl in USIA_COLUMNS},
}

# =========================
# Helper CRUD & Loader
# =========================
def load_hmhi_to_kode():
    try:
        df = read_sql_df(
            f"SELECT kode_organisasi, hmhi_cabang FROM {ORG_TABLE} ORDER BY id DESC"
        )
        if df.empty:
            return {}, []
        mapping = {
            str(r["hmhi_cabang"]).strip(): str(r["kode_organisasi"]).strip()
            for _, r in df.iterrows() if pd.notna(r["hmhi_cabang"])
        }
        return mapping, sorted(mapping.keys())
    except Exception:
        return {}, []

def insert_row(row: dict, kode_organisasi: str):
    keys = list(row.keys())
    cols = ", ".join(keys)
    placeholders = ", ".join([f":{k}" for k in keys])
    sql = text(f"""
        INSERT INTO {TABLE} (kode_organisasi, created_at, {cols})
        VALUES (:kode_organisasi, :created_at, {placeholders})
    """)
    params = {"kode_organisasi": kode_organisasi,
              "created_at": datetime.utcnow().isoformat(),
              **row}
    exec_sql(sql, params)

def read_with_join(limit=300):
    usia_cols = ", ".join([f"ku.{n}" for n, _ in USIA_COLUMNS])
    sql = f"""
        SELECT
          ku.id, ku.kode_organisasi, ku.created_at, ku.kelompok_usia,
          {usia_cols},
          io.kota_cakupan_cabang,
          io.hmhi_cabang
        FROM {TABLE} ku
        LEFT JOIN {ORG_TABLE} io ON io.kode_organisasi = ku.kode_organisasi
        ORDER BY ku.id DESC
        LIMIT :lim
    """
    return read_sql_df(sql, params={"lim": int(limit)})

def to_nonneg_int(x) -> int:
    try:
        if pd.isna(x) or str(x).strip() == "":
            return 0
        return max(int(float(x)), 0)
    except Exception:
        return 0

# =========================
# Antarmuka
# =========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom hmhi_cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="usia::hmhi_select"
        )

    df_default = pd.DataFrame(0, index=AGE_GROUPS, columns=[n for n, _ in USIA_COLUMNS])
    df_default.index.name = "Kelompok Usia"
    col_cfg = {n: st.column_config.NumberColumn(label=lbl, min_value=0, step=1)
               for n, lbl in USIA_COLUMNS}

    with st.form("usia::form"):
        edited = st.data_editor(
            df_default,
            key="usia::editor",
            column_config=col_cfg,
            use_container_width=True,
            num_rows="fixed",
        )
        save = st.form_submit_button("💾 Simpan Semua Baris")

    if save:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                rows_inserted = 0
                rows_skipped_all_zero = 0
                for usia, row in edited.iterrows():
                    payload = {"kelompok_usia": str(usia)}
                    nums = {n: to_nonneg_int(row.get(n, 0)) for n, _ in USIA_COLUMNS}
                    if all(v == 0 for v in nums.values()):
                        rows_skipped_all_zero += 1
                        continue
                    payload.update(nums)
                    insert_row(payload, kode_organisasi)
                    rows_inserted += 1
                cnt_df = read_sql_df(
                    f"SELECT COUNT(*) AS n FROM {TABLE} WHERE kode_organisasi=:k",
                    params={"k": kode_organisasi}
                )
                cnt = cnt_df["n"].iloc[0] if not cnt_df.empty else 0
                st.success(
                    f"{rows_inserted} baris disimpan untuk **{selected_hmhi}** "
                    f"(dilewati {rows_skipped_all_zero} baris karena semua kolom bernilai 0). "
                    f"Total baris untuk kode ini sekarang: {cnt}."
                )

with tab_data:
    st.subheader("📄 Data Tersimpan")
    df_x = read_with_join()
    st.caption("Gunakan template berikut saat mengunggah data (kolom & urutan baris Kelompok Usia disarankan).")

    # Template Excel
    tmpl_records = []
    for usia in TEMPLATE_AGE_ORDER:
        row = {"HMHI cabang": "", "Kelompok Usia": usia}
        for _key, lbl in USIA_COLUMNS:
            row[lbl] = 0
        tmpl_records.append(row)
    tmpl_df = pd.DataFrame(tmpl_records, columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_kelompok_usia.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="usia::dl_template"
    )

    # Tabel tampilan
    if df_x.empty:
        st.info("Belum ada data.")
    else:
        order = ["hmhi_cabang", "kelompok_usia"] + [n for n, _ in USIA_COLUMNS]
        order = [c for c in order if c in df_x.columns]
        nice = {n: lbl for n, lbl in USIA_COLUMNS}
        view = df_x[order].rename(columns={
            **nice,
            "hmhi_cabang": "HMHI cabang",
            "kelompok_usia": "Kelompok Usia",
        })
        st.dataframe(view, use_container_width=True)

        # Unduh Excel data tampilan
        buf_now = io.BytesIO()
        with pd.ExcelWriter(buf_now, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="KelompokUsia")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buf_now.getvalue(),
            file_name="kelompok_usia.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="usia::download"
        )

    # Unggah Excel
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan header persis seperti template",
        type=["xlsx"],
        key="usia::uploader"
    )
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

        df_up = raw.rename(columns=ALIAS_TO_DB).copy()
        st.caption("Pratinjau 20 baris pertama dari file yang diunggah:")
        st.dataframe(raw.head(20), use_container_width=True)

        if st.button("🚀 Proses & Simpan", type="primary", key="usia::process"):
            hmhi_map, _ = load_hmhi_to_kode()
            results = []
            for i, s in df_up.iterrows():
                try:
                    hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                    if not hmhi:
                        raise ValueError("Kolom 'HMHI cabang' kosong.")
                    kode_organisasi = hmhi_map.get(hmhi)
                    if not kode_organisasi:
                        raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                    payload = {key: to_nonneg_int(s.get(key, 0)) for key, _ in USIA_COLUMNS}
                    kelompok = str((s.get("kelompok_usia") or "")).strip()
                    if not kelompok:
                        raise ValueError("Kolom 'Kelompok Usia' kosong.")
                    payload["kelompok_usia"] = kelompok

                    insert_row(payload, kode_organisasi)
                    results.append({"Baris": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {kelompok}"})
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
                file_name="log_hasil_unggah_kelompok_usia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="usia::dl_log"
            )
