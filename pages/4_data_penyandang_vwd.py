import streamlit as st
import pandas as pd
from datetime import datetime
import io

# ===== Halaman =====
st.set_page_config(page_title="Data Penyandang vWD", page_icon="🩸", layout="wide")
st.title("🩸 Data Penyandang von Willebrand Disease (vWD) — per Kelompok Usia & Jenis Kelamin")

# ===== Target tabel di Supabase/Postgres =====
SUPABASE_TABLE = "public.vwd_usia_gender"
IDENTITAS_TABLE = "public.identitas_organisasi"

# Konektor ke Postgres (via db.py)
# Harus tersedia fungsi: fetch_df(sql, params=None), execute(sql, params=None), run_ddl(ddl)
# Kompatibilitas: beberapa repo memakai exec_sql, bukan execute
try:
    from db import fetch_df as pg_fetch_df, execute as pg_execute, run_ddl
except ImportError:
    from db import fetch_df as pg_fetch_df, exec_sql as pg_execute
    # Fallback run_ddl jika tidak ada di db.py
    try:
        from db import run_ddl
    except Exception:
        def run_ddl(ddl: str):
            for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
                pg_execute(stmt)


# ===== Konstanta UI/Data =====
AGE_GROUPS = ["0-4", "5-13", "14-18", "19-44", ">45", "Tidak ada data usia"]
TEMPLATE_AGE_ORDER = [">45", "19-44", "14-18", "5-13", "0-4"]

GENDER_COLS = [
    ("laki_laki", "Laki-Laki"),
    ("perempuan", "Perempuan"),
    ("jk_tidak_terdata", "Jenis Kelamin Tidak Terdata"),
]
TOTAL_LABEL = "Total"

TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Kelompok Usia",
    "Laki-Laki",
    "Perempuan",
    "Jenis Kelamin Tidak Terdata",
    "Total",
]

# ====== DDL: pastikan tabel ada (idempotent) ======
run_ddl(
    f"""
    CREATE TABLE IF NOT EXISTS {SUPABASE_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        kode_organisasi TEXT REFERENCES {IDENTITAS_TABLE}(kode_organisasi),
        created_at TIMESTAMP NOT NULL DEFAULT NOW(),
        kelompok_usia TEXT,
        laki_laki INTEGER,
        perempuan INTEGER,
        jk_tidak_terdata INTEGER,
        total INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_vwd_created_at ON {SUPABASE_TABLE}(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_vwd_kode ON {SUPABASE_TABLE}(kode_organisasi);
    """
)

# ===== Helpers =====
def safe_int(val):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return 0
        return int(x)
    except Exception:
        return 0


def load_hmhi_to_kode():
    """Ambil mapping hmhi_cabang -> kode_organisasi dari tabel identitas."""
    try:
        df = pg_fetch_df(
            f"""
            SELECT kode_organisasi, hmhi_cabang
            FROM {IDENTITAS_TABLE}
            WHERE COALESCE(hmhi_cabang,'') <> ''
            ORDER BY hmhi_cabang
            """
        )
        if df.empty:
            return {}, []
        mapping = {str(r.hmhi_cabang).strip(): str(r.kode_organisasi).strip() for _, r in df.iterrows() if str(r.hmhi_cabang).strip()}
        return mapping, sorted(mapping.keys())
    except Exception:
        return {}, []


def insert_row(payload: dict, kode_organisasi: str):
    sql = f"""
        INSERT INTO {SUPABASE_TABLE}
            (kode_organisasi, created_at, kelompok_usia, laki_laki, perempuan, jk_tidak_terdata, total)
        VALUES
            (:kode_organisasi, NOW(), :kelompok_usia, :laki_laki, :perempuan, :jk_tidak_terdata, :total)
    """
    params = {"kode_organisasi": kode_organisasi, **payload}
    pg_execute(sql, params)


def read_with_join(limit=300):
    sql = f"""
        SELECT
          v.id, v.kode_organisasi, v.created_at, v.kelompok_usia,
          v.laki_laki, v.perempuan, v.jk_tidak_terdata, v.total,
          io.kota_cakupan_cabang, io.hmhi_cabang
        FROM {SUPABASE_TABLE} v
        LEFT JOIN {IDENTITAS_TABLE} io ON io.kode_organisasi = v.kode_organisasi
        ORDER BY v.id DESC
        LIMIT :limit
    """
    return pg_fetch_df(sql, {"limit": int(limit)})

# ===== UI =====
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"]) 

with tab_input:
    st.caption("Isi jumlah Laki-Laki, Perempuan, dan Tidak Terdata untuk setiap kelompok usia. **Total** dihitung otomatis.")

    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom HMHI cabang). Harap isi dulu di halaman Identitas.")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="vwd::hmhi_select"
        )

    df_default = pd.DataFrame(0, index=AGE_GROUPS, columns=[k for k, _ in GENDER_COLS] + ["total"]) 
    df_default.index.name = "Kelompok Usia"

    col_config = {k: st.column_config.NumberColumn(label=lbl, min_value=0, step=1) for k, lbl in GENDER_COLS}
    col_config["total"] = st.column_config.NumberColumn(label=TOTAL_LABEL, min_value=0, step=1, disabled=True)

    with st.form("vwd_editor_form"):
        edited = st.data_editor(
            df_default,
            key="vwd::editor",
            column_config=col_config,
            use_container_width=True,
            num_rows="fixed",
        )
        # Hitung total per baris
        try:
            edited["total"] = (
                pd.to_numeric(edited["laki_laki"], errors="coerce").fillna(0).astype(int) +
                pd.to_numeric(edited["perempuan"], errors="coerce").fillna(0).astype(int) +
                pd.to_numeric(edited["jk_tidak_terdata"], errors="coerce").fillna(0).astype(int)
            )
        except Exception:
            edited["total"] = 0

        save = st.form_submit_button("💾 Simpan Semua Baris")

    if save:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                for usia, row in edited.iterrows():
                    payload = {
                        "kelompok_usia": str(usia),
                        "laki_laki": safe_int(row.get("laki_laki", 0)),
                        "perempuan": safe_int(row.get("perempuan", 0)),
                        "jk_tidak_terdata": safe_int(row.get("jk_tidak_terdata", 0)),
                        "total": safe_int(row.get("total", 0)),
                    }
                    insert_row(payload, kode_organisasi)
                st.success(f"Semua baris berhasil disimpan untuk **{selected_hmhi}**.")

with tab_data:
    st.subheader("📄 Data Tersimpan")
    df_x = read_with_join(limit=300)

    # ===== Unduh Template Excel =====
    st.caption("Gunakan template berikut saat mengunggah data (kolom dan urutan baris Kelompok Usia dianjurkan).")
    tmpl_records = []
    for usia in TEMPLATE_AGE_ORDER:
        row = {
            "HMHI cabang": "",
            "Kelompok Usia": usia,
            "Laki-Laki": 0,
            "Perempuan": 0,
            "Jenis Kelamin Tidak Terdata": 0,
            "Total": 0,
        }
        tmpl_records.append(row)
    tmpl_df = pd.DataFrame(tmpl_records, columns=TEMPLATE_COLUMNS)

    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_vwd.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="vwd::dl_template"
    )

    # ===== Tabel tampil =====
    if df_x.empty:
        st.info("Belum ada data.")
    else:
        order = [
            "hmhi_cabang",
            "kota_cakupan_cabang",
            "created_at",
            "kelompok_usia",
            "laki_laki",
            "perempuan",
            "jk_tidak_terdata",
            "total",
        ]
        order = [c for c in order if c in df_x.columns]

        view = df_x[order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "kelompok_usia": "Kelompok Usia",
            "laki_laki": "Laki-Laki",
            "perempuan": "Perempuan",
            "jk_tidak_terdata": "Jenis Kelamin Tidak Terdata",
            "total": "Total",
        })

        st.dataframe(view, use_container_width=True)

        # Unduh Excel (tampilan)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="DataVWD")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buffer.getvalue(),
            file_name="data_penyandang_vwd.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="vwd::download"
        )

    # ===== Unggah Excel =====
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan header persis seperti template",
        type=["xlsx"],
        key="vwd::uploader",
    )

    if up is not None:
        try:
            raw = pd.read_excel(up)
            raw.columns = [str(c).strip() for c in raw.columns]
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        # Validasi header
        missing = [c for c in TEMPLATE_COLUMNS if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Kolom yang belum ada: " + ", ".join(missing))
            st.stop()

        # alias -> key internal
        ALIAS_TO_DB = {
            "HMHI cabang": "hmhi_cabang_info",
            "Kelompok Usia": "kelompok_usia",
            "Laki-Laki": "laki_laki",
            "Perempuan": "perempuan",
            "Jenis Kelamin Tidak Terdata": "jk_tidak_terdata",
            "Total": "total",
        }
        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        # Pratinjau
        st.caption("Pratinjau 20 baris pertama dari file yang diunggah:")
        st.dataframe(raw.head(20), use_container_width=True)

        if st.button("🚀 Proses & Simpan", type="primary", key="vwd::process"):
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
                        raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                    laki = safe_int(s.get("laki_laki"))
                    pr = safe_int(s.get("perempuan"))
                    nd = safe_int(s.get("jk_tidak_terdata"))
                    total = safe_int(s.get("total"))
                    if total == 0 and (laki or pr or nd):
                        total = laki + pr + nd

                    kelompok = str((s.get("kelompok_usia") or "")).strip()
                    if not kelompok:
                        raise ValueError("Kolom 'Kelompok Usia' kosong.")

                    payload = {
                        "kelompok_usia": kelompok,
                        "laki_laki": max(laki, 0),
                        "perempuan": max(pr, 0),
                        "jk_tidak_terdata": max(nd, 0),
                        "total": max(total, 0),
                    }
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
                file_name="log_hasil_unggah_vwd.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="vwd::dl_log",
            )
