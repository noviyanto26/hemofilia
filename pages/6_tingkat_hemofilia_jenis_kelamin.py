import io
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import text

# 👉 modul koneksi Postgres milik proyek Anda
from db import exec_sql, read_sql_df

# =========================
# Konfigurasi & Konstanta
# =========================
st.set_page_config(page_title="Tingkat Hemofilia & Jenis Kelamin", page_icon="🩸", layout="wide")
st.title("🩸 Tingkat Hemofilia dan Jenis Kelamin")

TABLE = "public.tingkat_hemofilia_jenis_kelamin"

SEVERITY_COLS = [
    ("ringan", "Ringan (>5%)"),
    ("sedang", "Sedang (1-5%)"),
    ("berat", "Berat (<1%)"),
    ("tidak_diketahui", "Tidak diketahui"),
]
TOTAL_COL = "total"

# ===== Template unggah & alias (untuk upload Excel) =====
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Baris",
    "Ringan (>5%)",
    "Sedang (1-5%)",
    "Berat (<1%)",
    "Tidak diketahui",
    "Total",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Baris": "label",
    "Ringan (>5%)": "ringan",
    "Sedang (1-5%)": "sedang",
    "Berat (<1%)": "berat",
    "Tidak diketahui": "tidak_diketahui",
    "Total": "total",
}

# 🆕 Template unggah khusus Penyandang Perempuan
FEMALE_TEMPLATE_COLUMNS = [
    "HMHI Cabang",
    "Jenis Hemofilia",
    "Carrier (>40%)",
    "Ringan (>5%)",
    "Sedang (1-5%)",
    "Berat (<1%)",
]
FEMALE_ALIAS = {
    "HMHI Cabang": "hmhi_cabang",
    "Jenis Hemofilia": "jenis_hemofilia",
    "Carrier (>40%)": "carrier",
    "Ringan (>5%)": "ringan",
    "Sedang (1-5%)": "sedang",
    "Berat (<1%)": "berat",
}

# =========================
# Utilitas Postgres
# =========================
def load_hmhi_to_kode():
    df = read_sql_df("""
        SELECT kode_organisasi, hmhi_cabang
        FROM public.identitas_organisasi
        WHERE COALESCE(TRIM(hmhi_cabang),'') <> ''
        ORDER BY hmhi_cabang ASC
    """)
    if df is None or df.empty:
        return {}, []
    mapping = {str(r.hmhi_cabang).strip(): str(r.kode_organisasi).strip() for _, r in df.iterrows()}
    options = sorted(mapping.keys())
    return mapping, options


def _to_nonneg_int(v):
    try:
        x = pd.to_numeric(v, errors="coerce")
        if pd.isna(x):
            return 0
        return max(int(x), 0)
    except Exception:
        return 0


def insert_row(payload: dict, kode_organisasi: str):
    exec_sql(
        text(f"""
            INSERT INTO {TABLE}
                (kode_organisasi, label, ringan, sedang, berat, tidak_diketahui, total, is_total_row)
            VALUES
                (:kode_organisasi, :label, :ringan, :sedang, :berat, :td, :total, :is_total_row)
        """),
        {
            "kode_organisasi": kode_organisasi,
            "label": payload.get("label"),
            "ringan": payload.get("ringan", 0),
            "sedang": payload.get("sedang", 0),
            "berat": payload.get("berat", 0),
            "td": payload.get("tidak_diketahui", 0),
            "total": payload.get("total", 0),
            "is_total_row": payload.get("is_total_row", "0"),
        }
    )


def read_with_join(limit=500):
    sql = f"""
        SELECT
            t.id, t.kode_organisasi, t.created_at, t.label,
            t.ringan, t.sedang, t.berat, t.tidak_diketahui, t.total, t.is_total_row,
            io.kota_cakupan_cabang, io.hmhi_cabang
        FROM {TABLE} t
        LEFT JOIN public.identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
        ORDER BY t.id DESC
        LIMIT :lim
    """
    df = read_sql_df(sql, params={"lim": int(limit)})
    if df is None or df.empty:
        return pd.DataFrame()
    return df


# =========================
# Antarmuka
# =========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox("Pilih HMHI Cabang (Provinsi)", options=hmhi_list, key="thjk::hmhi_select")

    # ---------- Tabel 1: Penyandang Laki-laki ----------
    st.subheader("👨 Penyandang Laki-laki")
    df_lk = pd.DataFrame(
        0,
        index=["Hemofilia A laki-laki", "Hemofilia B laki-laki", "Total Laki-laki"],
        columns=[c for c, _ in SEVERITY_COLS] + [TOTAL_COL]
    )
    df_lk.index.name = "Baris"

    col_cfg = {c: st.column_config.NumberColumn(lbl, min_value=0, step=1) for c, lbl in SEVERITY_COLS}
    col_cfg[TOTAL_COL] = st.column_config.NumberColumn("Total", min_value=0, step=1, disabled=True)

    with st.form("thjk::form_lk"):
        ed_lk = st.data_editor(df_lk, key="thjk::editor_lk", column_config=col_cfg, use_container_width=True, num_rows="fixed")
        for row_label in ["Hemofilia A laki-laki", "Hemofilia B laki-laki"]:
            ed_lk.loc[row_label, TOTAL_COL] = sum(_to_nonneg_int(ed_lk.loc[row_label, c]) for c, _ in SEVERITY_COLS)
        for c, _ in SEVERITY_COLS:
            ed_lk.loc["Total Laki-laki", c] = _to_nonneg_int(ed_lk.loc["Hemofilia A laki-laki", c]) + _to_nonneg_int(ed_lk.loc["Hemofilia B laki-laki", c])
        ed_lk.loc["Total Laki-laki", TOTAL_COL] = sum(_to_nonneg_int(ed_lk.loc["Total Laki-laki", c]) for c, _ in SEVERITY_COLS)

        submit_lk = st.form_submit_button("💾 Simpan Data Laki-laki")

    if submit_lk:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                for label in ed_lk.index.tolist():
                    payload = {
                        "label": label,
                        "ringan": _to_nonneg_int(ed_lk.loc[label, "ringan"]),
                        "sedang": _to_nonneg_int(ed_lk.loc[label, "sedang"]),
                        "berat": _to_nonneg_int(ed_lk.loc[label, "berat"]),
                        "tidak_diketahui": _to_nonneg_int(ed_lk.loc[label, "tidak_diketahui"]),
                        "total": _to_nonneg_int(ed_lk.loc[label, "total"]),
                        "is_total_row": "1" if label.startswith("Total ") else "0",
                    }
                    insert_row(payload, kode_organisasi)
                st.success(f"Data laki-laki tersimpan untuk **{selected_hmhi}**.")

    st.divider()

    # ---------- Tabel 2: Penyandang Perempuan ----------
    st.subheader("👩 Penyandang Perempuan")

    FEMALE_ROWS = ["Hemofilia A perempuan", "Hemofilia B perempuan"]
    FEMALE_COLS = [("carrier", "Carrier (>40%)"), ("ringan", "Ringan (>5%)"), ("sedang", "Sedang (1-5%)"), ("berat",  "Berat (<1%)")]
    df_pr_new = pd.DataFrame(0, index=FEMALE_ROWS, columns=[c for c, _ in FEMALE_COLS])
    df_pr_new.index.name = "Jenis Hemofilia"
    col_cfg_pr = {c: st.column_config.NumberColumn(lbl, min_value=0, step=1) for c, lbl in FEMALE_COLS}

    with st.form("thjk::form_pr_new"):
        ed_pr_new = st.data_editor(df_pr_new, key="thjk::editor_pr_new", column_config=col_cfg_pr, use_container_width=True, num_rows="fixed")
        submit_pr_new = st.form_submit_button("💾 Simpan Data Penyandang Perempuan")

    if submit_pr_new:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            try:
                for jh in ed_pr_new.index.tolist():
                    carrier = _to_nonneg_int(ed_pr_new.loc[jh, "carrier"])
                    ringan  = _to_nonneg_int(ed_pr_new.loc[jh, "ringan"])
                    sedang  = _to_nonneg_int(ed_pr_new.loc[jh, "sedang"])
                    berat   = _to_nonneg_int(ed_pr_new.loc[jh, "berat"])
                    exec_sql(
                        text("""
                            INSERT INTO public.hemofilia_perempuan
                                (jenis_hemofilia, carrier, ringan, sedang, berat)
                            VALUES
                                (:jenis_hemofilia, :carrier, :ringan, :sedang, :berat)
                        """),
                        {
                            "jenis_hemofilia": jh,
                            "carrier": carrier,
                            "ringan": ringan,
                            "sedang": sedang,
                            "berat": berat,
                        }
                    )
                st.success("Data perempuan berhasil disimpan ke Postgres (public.hemofilia_perempuan).")
            except Exception as e:
                st.error(f"Gagal menyimpan data perempuan ke Postgres: {e}")

# =========================
# Data Tersimpan & Unggah Excel
# =========================
with tab_data:
    st.subheader("📄 Data Tersimpan (Laki-laki)")
    df = read_with_join(limit=500)

    st.caption("Gunakan template berikut saat mengunggah data (kolom harus sesuai).")
    template_rows = [
        {"HMHI cabang": "", "Baris": "Hemofilia A laki-laki", "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
        {"HMHI cabang": "", "Baris": "Hemofilia B laki-laki", "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
        {"HMHI cabang": "", "Baris": "Total Laki-laki",       "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
        {"HMHI cabang": "", "Baris": "Hemofilia A perempuan",  "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
        {"HMHI cabang": "", "Baris": "Hemofilia B perempuan",  "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
        {"HMHI cabang": "", "Baris": "Total Perempuan",        "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
    ]
    tmpl_df = pd.DataFrame(template_rows, columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button("📥 Unduh Template Excel (Tingkat Hemofilia & JK)", buf_tmpl.getvalue(), file_name="template_tingkat_hemofilia_jenis_kelamin.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    if df.empty:
        st.info("Belum ada data laki-laki.")
    else:
        order = ["hmhi_cabang", "kota_cakupan_cabang", "created_at", "label", "ringan", "sedang", "berat", "tidak_diketahui", "total"]
        order = [c for c in order if c in df.columns]
        view = df[order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "label": "Baris",
            "ringan": "Ringan (>5%)",
            "sedang": "Sedang (1-5%)",
            "berat": "Berat (<1%)",
            "tidak_diketahui": "Tidak diketahui",
            "total": "Total",
        })
        st.dataframe(view, use_container_width=True)

    st.divider()

    # (2) Template khusus Penyandang Perempuan
    st.subheader("📄 Data Tersimpan (Perempuan)")
    st.caption("Atau gunakan template khusus untuk input Penyandang Perempuan (Postgres: public.hemofilia_perempuan).")
    female_template_rows = [
        {"HMHI Cabang": "", "Jenis Hemofilia": "Hemofilia A perempuan", "Carrier (>40%)": 0, "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0},
        {"HMHI Cabang": "", "Jenis Hemofilia": "Hemofilia B perempuan", "Carrier (>40%)": 0, "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0},
    ]
    female_tmpl_df = pd.DataFrame(female_template_rows, columns=FEMALE_TEMPLATE_COLUMNS)

    hmhi_map_ref, _ = load_hmhi_to_kode()
    ref_rows = [{"HMHI Cabang": k, "kode_organisasi": v} for k, v in hmhi_map_ref.items()]
    ref_df = pd.DataFrame(ref_rows, columns=["HMHI Cabang", "kode_organisasi"])

    buf_tmpl_f = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl_f, engine="xlsxwriter") as w:
        female_tmpl_df.to_excel(w, index=False, sheet_name="PenyandangPerempuan")
        (ref_df if not ref_df.empty else pd.DataFrame(columns=["HMHI Cabang", "kode_organisasi"])) \
            .to_excel(w, index=False, sheet_name="ReferensiHMHI")
    st.download_button("📥 Unduh Template Excel (Penyandang Perempuan)", buf_tmpl_f.getvalue(), file_name="template_penyandang_perempuan.xlsx", mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    sql_female = """
        SELECT 
            hp.id, hp.kode_organisasi, hp.jenis_hemofilia,
            hp.carrier, hp.ringan, hp.sedang, hp.berat,
            hp.created_at,
            io.hmhi_cabang, io.kota_cakupan_cabang
        FROM public.hemofilia_perempuan hp
        LEFT JOIN public.identitas_organisasi io 
          ON io.kode_organisasi = hp.kode_organisasi
        ORDER BY hp.id DESC
        LIMIT :lim
    """
    df_female = read_sql_df(sql_female, params={"lim": 500})
    if df_female is None or df_female.empty:
        df_female = pd.DataFrame()

    if df_female.empty:
        st.info("Belum ada data penyandang perempuan.")
    else:
        order_f = ["hmhi_cabang", "kota_cakupan_cabang", "created_at", "jenis_hemofilia", "carrier", "ringan", "sedang", "berat"]
        order_f = [c for c in order_f if c in df_female.columns]
        view_f = df_female[order_f].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "jenis_hemofilia": "Jenis Hemofilia",
            "carrier": "Carrier (>40%)",
            "ringan": "Ringan (>5%)",
            "sedang": "Sedang (1-5%)",
            "berat": "Berat (<1%)",
        })
        st.dataframe(view_f, use_container_width=True)

    st.divider()
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader("Pilih file Excel (.xlsx) dengan header persis seperti template yang diunduh", type=["xlsx"], key="thjk::uploader")

    if up is not None:
        try:
            raw = pd.read_excel(up)
            raw.columns = [str(c).strip() for c in raw.columns]
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        cols = set(raw.columns)
        is_general = set(TEMPLATE_COLUMNS).issubset(cols)
        is_female  = set(FEMALE_TEMPLATE_COLUMNS).issubset(cols)

        if not is_general and not is_female:
            st.error("Header kolom tidak sesuai.")
            st.stop()

        # ===== Jalur 1: Template Umum =====
        if is_general:
            df_up = raw.rename(columns=ALIAS_TO_DB).copy()
            if st.button("🚀 Proses & Simpan (Template Umum)", type="primary", key="thjk::process_general"):
                hmhi_map, _ = load_hmhi_to_kode()
                results = []
                for i in range(len(df_up)):
                    try:
                        s = df_up.iloc[i]
                        hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                        if not hmhi:
                            raise ValueError("Kolom 'HMHI cabang' kosong.")
                        kode_organisasi = hmhi_map.get(hmhi)
                        if not kode_organisasi:
                            raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan.")
                        label = str((s.get("label") or "")).strip()
                        if not label:
                            raise ValueError("Kolom 'Baris' kosong.")
                        ringan = _to_nonneg_int(s.get("ringan"))
                        sedang = _to_nonneg_int(s.get("sedang"))
                        berat = _to_nonneg_int(s.get("berat"))
                        td = _to_nonneg_int(s.get("tidak_diketahui"))
                        total = _to_nonneg_int(s.get("total"))
                        if total == 0 and (ringan or sedang or berat or td):
                            total = ringan + sedang + berat + td
                        payload = {"label": label, "ringan": ringan, "sedang": sedang, "berat": berat, "tidak_diketahui": td, "total": total, "is_total_row": "1" if label.lower().startswith("total") else "0"}
                        insert_row(payload, kode_organisasi)
                        results.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"{hmhi} / {label}"})
                    except Exception as e:
                        results.append({"Baris Excel": i + 2, "Status": "GAGAL", "Keterangan": str(e)})
                st.dataframe(pd.DataFrame(results), use_container_width=True)

        # ===== Jalur 2: Template Penyandang Perempuan =====
        if is_female:
            df_fp = raw.rename(columns=FEMALE_ALIAS).copy()
            if st.button("🚀 Proses & Simpan (Penyandang Perempuan)", type="primary", key="thjk::process_female"):
                hmhi_map, _ = load_hmhi_to_kode()
                results_f = []
                for i in range(len(df_fp)):
                    try:
                        s = df_fp.iloc[i]
                        hmhi = str((s.get("hmhi_cabang") or "")).strip()
                        kode_organisasi = hmhi_map.get(hmhi, None)
                        jenis = str((s.get("jenis_hemofilia") or "")).strip()
                        carrier = _to_nonneg_int(s.get("carrier"))
                        ringan  = _to_nonneg_int(s.get("ringan"))
                        sedang  = _to_nonneg_int(s.get("sedang"))
                        berat   = _to_nonneg_int(s.get("berat"))
                        exec_sql(text("""INSERT INTO public.hemofilia_perempuan (kode_organisasi, jenis_hemofilia, carrier, ringan, sedang, berat) VALUES (:kode_organisasi, :jenis_hemofilia, :carrier, :ringan, :sedang, :berat)"""), {"kode_organisasi": kode_organisasi, "jenis_hemofilia": jenis, "carrier": carrier, "ringan": ringan, "sedang": sedang, "berat": berat})
                        results_f.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"{hmhi} / {jenis}"})
                    except Exception as e:
                        results_f.append({"Baris Excel": i + 2, "Status": "GAGAL", "Keterangan": str(e)})
                st.dataframe(pd.DataFrame(results_f), use_container_width=True)
