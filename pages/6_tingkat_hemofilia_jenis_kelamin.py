import io
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

# ===== Template unggah & alias =====
TEMPLATE_COLUMNS = [
    "HMHI cabang", "Baris",
    "Ringan (>5%)", "Sedang (1-5%)",
    "Berat (<1%)", "Tidak diketahui", "Total",
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

# 🆕 Template unggah khusus Perempuan
FEMALE_TEMPLATE_COLUMNS = [
    "HMHI Cabang", "Jenis Hemofilia",
    "Carrier (>40%)", "Ringan (>5%)",
    "Sedang (1-5%)", "Berat (<1%)",
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
    return mapping, sorted(mapping.keys())

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
        SELECT t.id, t.kode_organisasi, t.created_at, t.label,
               t.ringan, t.sedang, t.berat, t.tidak_diketahui,
               t.total, t.is_total_row,
               io.kota_cakupan_cabang, io.hmhi_cabang
        FROM {TABLE} t
        LEFT JOIN public.identitas_organisasi io
          ON io.kode_organisasi = t.kode_organisasi
        ORDER BY t.id DESC
        LIMIT :lim
    """
    return read_sql_df(sql, params={"lim": int(limit)}) or pd.DataFrame()

# =========================
# Antarmuka
# =========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

# =========================
# Tab Input
# =========================
with tab_input:
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    selected_hmhi = st.selectbox("Pilih HMHI Cabang (Provinsi)", options=hmhi_list, key="hmhi") if hmhi_list else None

    # ---------- 👨 Input laki-laki ----------
    st.subheader("👨 Penyandang Laki-laki")
    df_lk = pd.DataFrame(
        0,
        index=["Hemofilia A laki-laki", "Hemofilia B laki-laki", "Total Laki-laki"],
        columns=[c for c, _ in SEVERITY_COLS] + [TOTAL_COL]
    )
    df_lk.index.name = "Baris"
    col_cfg = {c: st.column_config.NumberColumn(lbl, min_value=0, step=1) for c, lbl in SEVERITY_COLS}
    col_cfg[TOTAL_COL] = st.column_config.NumberColumn("Total", disabled=True)

    with st.form("form_lk"):
        ed_lk = st.data_editor(df_lk, key="editor_lk", column_config=col_cfg, use_container_width=True, num_rows="fixed")
        for row_label in ["Hemofilia A laki-laki", "Hemofilia B laki-laki"]:
            ed_lk.loc[row_label, TOTAL_COL] = sum(_to_nonneg_int(ed_lk.loc[row_label, c]) for c, _ in SEVERITY_COLS)
        for c, _ in SEVERITY_COLS:
            ed_lk.loc["Total Laki-laki", c] = (
                _to_nonneg_int(ed_lk.loc["Hemofilia A laki-laki", c]) +
                _to_nonneg_int(ed_lk.loc["Hemofilia B laki-laki", c])
            )
        ed_lk.loc["Total Laki-laki", TOTAL_COL] = sum(_to_nonneg_int(ed_lk.loc["Total Laki-laki", c]) for c, _ in SEVERITY_COLS)
        submit_lk = st.form_submit_button("💾 Simpan Data Laki-laki")

    if submit_lk and selected_hmhi:
        kode_organisasi = hmhi_map.get(selected_hmhi)
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

    # ---------- 👩 Input perempuan ----------
    st.subheader("👩 Penyandang Perempuan")
    FEMALE_ROWS = ["Hemofilia A perempuan", "Hemofilia B perempuan"]
    FEMALE_COLS = [("carrier","Carrier (>40%)"),("ringan","Ringan (>5%)"),("sedang","Sedang (1-5%)"),("berat","Berat (<1%)")]
    df_pr = pd.DataFrame(0, index=FEMALE_ROWS, columns=[c for c,_ in FEMALE_COLS])
    df_pr.index.name = "Jenis Hemofilia"
    col_cfg_pr = {c: st.column_config.NumberColumn(lbl, min_value=0, step=1) for c,lbl in FEMALE_COLS}

    with st.form("form_pr"):
        ed_pr = st.data_editor(df_pr, key="editor_pr", column_config=col_cfg_pr, use_container_width=True, num_rows="fixed")
        submit_pr = st.form_submit_button("💾 Simpan Data Penyandang Perempuan")

    if submit_pr and selected_hmhi:
        for jh in ed_pr.index.tolist():
            exec_sql(
                text("""
                    INSERT INTO public.hemofilia_perempuan
                        (jenis_hemofilia, carrier, ringan, sedang, berat)
                    VALUES (:jenis_hemofilia, :carrier, :ringan, :sedang, :berat)
                """),
                {
                    "jenis_hemofilia": jh,
                    "carrier": _to_nonneg_int(ed_pr.loc[jh,"carrier"]),
                    "ringan":  _to_nonneg_int(ed_pr.loc[jh,"ringan"]),
                    "sedang":  _to_nonneg_int(ed_pr.loc[jh,"sedang"]),
                    "berat":   _to_nonneg_int(ed_pr.loc[jh,"berat"]),
                }
            )
        st.success("Data perempuan berhasil disimpan ke Postgres.")

# =========================
# Tab Data
# =========================
with tab_data:
    # ---- Laki-laki ----
    st.subheader("📄 Data Tersimpan (Laki-laki)")
    df = read_with_join(limit=500)
    if not df.empty:
        view = df.rename(columns={
            "hmhi_cabang":"HMHI cabang","kota_cakupan_cabang":"Kota/Provinsi Cakupan Cabang",
            "created_at":"Created At","label":"Baris",
            "ringan":"Ringan (>5%)","sedang":"Sedang (1-5%)","berat":"Berat (<1%)",
            "tidak_diketahui":"Tidak diketahui","total":"Total"
        })
        st.dataframe(view, use_container_width=True)
    else:
        st.info("Belum ada data laki-laki.")

    st.divider()
    # ---- Perempuan ----
    st.subheader("📄 Data Tersimpan (Perempuan)")
    sql_female = """
        SELECT hp.id, hp.kode_organisasi, hp.jenis_hemofilia,
               hp.carrier, hp.ringan, hp.sedang, hp.berat, hp.created_at,
               io.hmhi_cabang, io.kota_cakupan_cabang
        FROM public.hemofilia_perempuan hp
        LEFT JOIN public.identitas_organisasi io
          ON io.kode_organisasi = hp.kode_organisasi
        ORDER BY hp.id DESC LIMIT :lim
    """
    df_f = read_sql_df(sql_female, params={"lim":500}) or pd.DataFrame()
    if not df_f.empty:
        view_f = df_f.rename(columns={
            "hmhi_cabang":"HMHI cabang","kota_cakupan_cabang":"Kota/Provinsi Cakupan Cabang",
            "created_at":"Created At","jenis_hemofilia":"Jenis Hemofilia",
            "carrier":"Carrier (>40%)","ringan":"Ringan (>5%)",
            "sedang":"Sedang (1-5%)","berat":"Berat (<1%)"
        })
        st.dataframe(view_f, use_container_width=True)
    else:
        st.info("Belum ada data perempuan.")

    st.divider()
    # ---- Unggah Excel ----
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader("Pilih file Excel (.xlsx)...", type=["xlsx"], key="uploader")
    if up is not None:
        raw = pd.read_excel(up)
        raw.columns = [str(c).strip() for c in raw.columns]
        is_general = set(TEMPLATE_COLUMNS).issubset(raw.columns)
        is_female = set(FEMALE_TEMPLATE_COLUMNS).issubset(raw.columns)

        if is_general:
            df_up = raw.rename(columns=ALIAS_TO_DB).copy()
            if st.button("🚀 Proses & Simpan (Template Umum)", type="primary"):
                hmhi_map,_ = load_hmhi_to_kode(); results=[]
                for i,s in df_up.iterrows():
                    try:
                        hmhi = str(s.get("hmhi_cabang_info") or "").strip()
                        if not hmhi: raise ValueError("Kolom 'HMHI cabang' kosong.")
                        kode_organisasi = hmhi_map.get(hmhi)
                        if not kode_organisasi: raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan.")
                        label = str(s.get("label") or "").strip()
                        if not label: raise ValueError("Kolom 'Baris' kosong.")
                        ringan=_to_nonneg_int(s.get("ringan")); sedang=_to_nonneg_int(s.get("sedang"))
                        berat=_to_nonneg_int(s.get("berat")); td=_to_nonneg_int(s.get("tidak_diketahui"))
                        total=_to_nonneg_int(s.get("total")) or (ringan+sedang+berat+td)
                        payload={"label":label,"ringan":ringan,"sedang":sedang,"berat":berat,
                                 "tidak_diketahui":td,"total":total,
                                 "is_total_row":"1" if label.lower().startswith("total ") else "0"}
                        insert_row(payload, kode_organisasi)
                        results.append({"Baris Excel":i+2,"Status":"OK","Keterangan":f"Simpan → {hmhi}/{label}"})
                    except Exception as e:
                        results.append({"Baris Excel":i+2,"Status":"GAGAL","Keterangan":str(e)})
                st.dataframe(pd.DataFrame(results), use_container_width=True)

        if is_female:
            df_fp = raw.rename(columns=FEMALE_ALIAS).copy()
            if st.button("🚀 Proses & Simpan (Penyandang Perempuan)", type="primary"):
                hmhi_map,_=load_hmhi_to_kode(); results_f=[]
                for i,s in df_fp.iterrows():
                    try:
                        hmhi=str(s.get("hmhi_cabang") or "").strip()
                        if not hmhi: raise ValueError("Kolom 'HMHI Cabang' kosong.")
                        kode_organisasi=hmhi_map.get(hmhi)
                        if not kode_organisasi: raise ValueError(f"HMHI Cabang '{hmhi}' tidak ditemukan.")
                        jenis=str(s.get("jenis_hemofilia") or "").strip()
                        carrier=_to_nonneg_int(s.get("carrier")); ringan=_to_nonneg_int(s.get("ringan"))
                        sedang=_to_nonneg_int(s.get("sedang")); berat=_to_nonneg_int(s.get("berat"))
                        exec_sql(
                            text("""INSERT INTO public.hemofilia_perempuan
                                    (kode_organisasi, jenis_hemofilia, carrier, ringan, sedang, berat)
                                    VALUES (:kode_organisasi,:jenis_hemofilia,:carrier,:ringan,:sedang,:berat)"""),
                            {"kode_organisasi":kode_organisasi,"jenis_hemofilia":jenis,
                             "carrier":carrier,"ringan":ringan,"sedang":sedang,"berat":berat}
                        )
                        results_f.append({"Baris Excel":i+2,"Status":"OK","Keterangan":f"Simpan → {hmhi}/{jenis}"})
                    except Exception as e:
                        results_f.append({"Baris Excel":i+2,"Status":"GAGAL","Keterangan":str(e)})
                st.dataframe(pd.DataFrame(results_f), use_container_width=True)
