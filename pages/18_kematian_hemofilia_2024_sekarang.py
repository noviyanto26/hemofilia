import streamlit as st
import pandas as pd
from datetime import datetime
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Kematian Penyandang Hemofilia (2024–Sekarang)", page_icon="🕯️", layout="wide")
st.title("🕯️ Jumlah Kematian Penyandang Hemofilia (1 Januari 2024–Sekarang)")

# ======================== Koneksi DB (Supabase/Postgres) ========================
# Mengikuti referensi: gunakan helper dari db.py
from db import fetch_df as pg_fetch_df, exec_sql as pg_exec_sql  # memastikan url/engine diatur terpusat

TABLE = "public.kematian_hemofilia_2024kini"
TABLE_ORG = "public.identitas_organisasi"

# ======================== Helpers DB ========================
def insert_row(payload: dict, kode_organisasi: str):
    """
    Insert baris baru. created_at diisi NOW() (timestamp server).
    Kolom: id, kode_organisasi, created_at, penyebab_kematian, perdarahan, gangguan_hati, hiv, penyebab_lain, tahun_kematian
    """
    sql = f"""
        INSERT INTO {TABLE}
            (kode_organisasi, created_at,
             penyebab_kematian, perdarahan, gangguan_hati, hiv, penyebab_lain, tahun_kematian)
        VALUES
            (:kode_organisasi, NOW(),
             :penyebab_kematian, :perdarahan, :gangguan_hati, :hiv, :penyebab_lain, :tahun_kematian)
    """
    params = {
        "kode_organisasi": kode_organisasi,
        "penyebab_kematian": payload.get("penyebab_kematian"),
        "perdarahan": int(payload.get("perdarahan") or 0),
        "gangguan_hati": int(payload.get("gangguan_hati") or 0),
        "hiv": int(payload.get("hiv") or 0),
        "penyebab_lain": payload.get("penyebab_lain"),
        "tahun_kematian": int(payload.get("tahun_kematian") or 0),
    }
    pg_exec_sql(sql, params)

def read_with_label(limit=1000) -> pd.DataFrame:
    """
    Ambil data + join label organisasi (hmhi_cabang) untuk tampilan.
    """
    lim = int(limit)
    q = f"""
        SELECT
          t.id, t.kode_organisasi, t.created_at,
          t.penyebab_kematian, t.perdarahan, t.gangguan_hati, t.hiv, t.penyebab_lain, t.tahun_kematian,
          io.hmhi_cabang AS label_organisasi
        FROM {TABLE} t
        LEFT JOIN {TABLE_ORG} io ON io.kode_organisasi = t.kode_organisasi
        ORDER BY t.id DESC
        LIMIT {lim}
    """
    return pg_fetch_df(q)

def load_kode_organisasi_with_label():
    """
    Mapping kode_organisasi -> HMHI Cabang (tanpa kode). Jika duplikat label, diberi akhiran (pilihan n).
    """
    try:
        df = pg_fetch_df(f"""
            SELECT kode_organisasi, hmhi_cabang
            FROM {TABLE_ORG}
            ORDER BY id DESC
        """)
        if df.empty:
            return {}, []

        labels = df["hmhi_cabang"].fillna("-").astype(str).str.strip()
        counts = labels.value_counts()
        display, dup_index = [], {}
        for lab in labels:
            if counts[lab] == 1:
                display.append(lab)
            else:
                i = dup_index.get(lab, 1)
                display.append(f"{lab} (pilihan {i})")
                dup_index[lab] = i + 1

        mapping = {k: disp for k, disp in zip(df["kode_organisasi"], display)}
        return mapping, df["kode_organisasi"].tolist()
    except Exception:
        return {}, []

# ======================== Helpers umum ========================
def safe_int(val, default=0):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x): return default
        return int(x)
    except Exception:
        return default

# ======================== Daftar label Penyebab ========================
PENYEBAB_LABELS = [
    "Hemofilia A",
    "Hemofilia B",
    "Hemofilia tipe lain",
    "Terduga hemofilia",
    "vWD",
    "Kelainan pembekuan darah lain",
]

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # Pilih organisasi: tampilkan HMHI Cabang (tanpa kode)
    mapping, kode_list = load_kode_organisasi_with_label()
    if not kode_list:
        st.warning("Belum ada data Identitas Organisasi.")
        kode_organisasi = None
    else:
        kode_organisasi = st.selectbox(
            "Pilih Organisasi (HMHI Cabang)",
            options=kode_list,
            format_func=lambda x: mapping.get(x, "-"),
            key="kmh::kode_select"
        )

    st.subheader("Form Kematian Penyandang Hemofilia (2024–Sekarang)")

    # Dataframe default: 1 baris per label penyebab (label terkunci)
    df_default = pd.DataFrame({
        "penyebab_kematian": PENYEBAB_LABELS,
        "perdarahan": [0] * len(PENYEBAB_LABELS),
        "gangguan_hati": [0] * len(PENYEBAB_LABELS),
        "hiv": [0] * len(PENYEBAB_LABELS),
        "penyebab_lain": ["" for _ in PENYEBAB_LABELS],
        "tahun_kematian": [datetime.utcnow().year for _ in PENYEBAB_LABELS],
    })

    with st.form("kmh::form"):
        ed = st.data_editor(
            df_default,
            key="kmh::editor",
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            disabled=["penyebab_kematian"],
            column_config={
                "penyebab_kematian": st.column_config.TextColumn("Penyebab Kematian"),
                "perdarahan": st.column_config.NumberColumn("Perdarahan", min_value=0, step=1),
                "gangguan_hati": st.column_config.NumberColumn("Gangguan hati", min_value=0, step=1),
                "hiv": st.column_config.NumberColumn("HIV", min_value=0, step=1),
                "penyebab_lain": st.column_config.TextColumn("Penyebab lain (opsional)"),
                "tahun_kematian": st.column_config.NumberColumn("Tahun Kematian", min_value=2024, max_value=2100, step=1),
            },
        )
        submit = st.form_submit_button("💾 Simpan")

    # Simpan (skip baris kosong total)
    if submit:
        if not kode_organisasi:
            st.error("Pilih organisasi terlebih dahulu.")
        else:
            n_saved, skipped = 0, 0
            for _, row in ed.iterrows():
                sebab = str(row.get("penyebab_kematian") or "").strip()
                perd = safe_int(row.get("perdarahan", 0))
                hati = safe_int(row.get("gangguan_hati", 0))
                hivv = safe_int(row.get("hiv", 0))
                lain = str(row.get("penyebab_lain") or "").strip()
                thn  = safe_int(row.get("tahun_kematian", 0))

                if perd == 0 and hati == 0 and hivv == 0 and not lain:
                    skipped += 1
                    continue

                payload = {
                    "penyebab_kematian": sebab,
                    "perdarahan": perd,
                    "gangguan_hati": hati,
                    "hiv": hivv,
                    "penyebab_lain": lain,
                    "tahun_kematian": thn,
                }
                try:
                    insert_row(payload, kode_organisasi)
                    n_saved += 1
                except Exception as e:
                    st.error(f"Gagal menyimpan baris ({sebab}): {e}")

            if n_saved > 0:
                msg = f"{n_saved} baris tersimpan"
                if skipped > 0:
                    msg += f" ({skipped} baris kosong diabaikan)"
                st.success(f"{msg} untuk {mapping.get(kode_organisasi, '-')}.")
            else:
                st.info("Tidak ada baris valid untuk disimpan (semua baris kosong).")

with tab_data:
    st.subheader("📄 Data Tersimpan — Kematian Penyandang Hemofilia")

    # ====== Unduh Template Excel ======
    st.markdown("#### 📄 Unduh Template Excel")
    tmpl_buf = io.BytesIO()
    df_template = pd.DataFrame([
        {
            "penyebab_kematian": "Hemofilia A",
            "perdarahan": 0,
            "gangguan_hati": 0,
            "hiv": 0,
            "penyebab_lain": "",
            "tahun_kematian": datetime.utcnow().year,
            "kode_organisasi": "CAB001"
        },
        {
            "penyebab_kematian": "Hemofilia B",
            "perdarahan": 0,
            "gangguan_hati": 0,
            "hiv": 0,
            "penyebab_lain": "",
            "tahun_kematian": datetime.utcnow().year,
            "kode_organisasi": "CAB002"
        }
    ], columns=[
        "penyebab_kematian", "perdarahan", "gangguan_hati",
        "hiv", "penyebab_lain", "tahun_kematian", "kode_organisasi"
    ])
    with pd.ExcelWriter(tmpl_buf, engine="xlsxwriter") as w:
        df_template.to_excel(w, index=False, sheet_name="Template_Kematian_2024_Sekarang")
    st.download_button(
        "⬇️ Unduh Template Excel",
        tmpl_buf.getvalue(),
        file_name="template_kematian_hemofilia_2024_sekarang.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="kmh::download_template"
    )

    # ====== Unggah Excel (Impor Massal) ======
    st.markdown("### ⬆️ Unggah Excel (Impor Massal)")
    st.caption("File yang diunggah **harus** menyertakan semua kolom wajib.")
    up = st.file_uploader("Pilih file Excel (.xlsx)", type=["xlsx"], key="kmh::uploader")
    if up is not None:
        try:
            df_up = pd.read_excel(up)
            st.write("Pratinjau file diunggah:")
            st.dataframe(df_up.head(20), use_container_width=True)

            # Normalisasi nama kolom -> snake_case sederhana
            norm_map = {c: c.strip().lower().replace(" ", "_") for c in df_up.columns}
            df_norm = df_up.rename(columns=norm_map)

            required = [
                "penyebab_kematian", "perdarahan", "gangguan_hati",
                "hiv", "penyebab_lain", "tahun_kematian", "kode_organisasi"
            ]
            missing = [c for c in required if c not in df_norm.columns]
            if missing:
                st.error(f"Kolom wajib belum lengkap di file: {missing}")
            else:
                n_ok, n_skip = 0, 0
                for _, r in df_norm.iterrows():
                    sebab = str(r.get("penyebab_kematian") or "").strip()
                    perd  = safe_int(r.get("perdarahan", 0))
                    hati  = safe_int(r.get("gangguan_hati", 0))
                    hivv  = safe_int(r.get("hiv", 0))
                    lain  = str(r.get("penyebab_lain") or "").strip()
                    thn   = safe_int(r.get("tahun_kematian", 0))
                    korg  = str(r.get("kode_organisasi") or "").strip()

                    if not korg:
                        n_skip += 1
                        continue
                    # Baris benar-benar kosong → lewati
                    if perd == 0 and hati == 0 and hivv == 0 and not lain and not sebab:
                        n_skip += 1
                        continue

                    payload = {
                        "penyebab_kematian": sebab,
                        "perdarahan": perd,
                        "gangguan_hati": hati,
                        "hiv": hivv,
                        "penyebab_lain": lain,
                        "tahun_kematian": thn,
                    }
                    insert_row(payload, korg)
                    n_ok += 1

                if n_ok:
                    st.success(f"Impor selesai: {n_ok} baris masuk, {n_skip} baris dilewati.")
                else:
                    st.info("Tidak ada baris valid yang diimpor (cek kolom dan nilai).")
        except Exception as e:
            st.error(f"Gagal memproses file: {e}")

    # ====== Tampilkan data tersimpan ======
    df = read_with_label(limit=1000)
    if df.empty:
        st.info("Belum ada data tersimpan.")
    else:
        cols_order = [
            "label_organisasi", "created_at",
            "penyebab_kematian", "perdarahan", "gangguan_hati", "hiv",
            "penyebab_lain", "tahun_kematian"
        ]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "label_organisasi": "HMHI Cabang / Label Organisasi",
            "created_at": "Created At",
            "penyebab_kematian": "Penyebab Kematian",
            "perdarahan": "Perdarahan",
            "gangguan_hati": "Gangguan hati",
            "hiv": "HIV",
            "penyebab_lain": "Penyebab lain",
            "tahun_kematian": "Tahun Kematian",
        })

        # Hindari masalah serialisasi timezone saat ekspor
        if "Created At" in view.columns:
            view["Created At"] = view["Created At"].astype(str)

        st.dataframe(view, use_container_width=True)

        # Unduh data hasil gabung label
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="Kematian_2024_Sekarang")
        st.download_button(
            "⬇️ Unduh Data Excel",
            buf.getvalue(),
            file_name="kematian_hemofilia_2024_sekarang.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="kmh::download"
        )
