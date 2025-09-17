import streamlit as st
import pandas as pd
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Infeksi melalui Transfusi Darah", page_icon="🧬", layout="wide")
st.title("🧬 Jumlah Penyandang Hemofilia Terinfeksi Penyakit Menular Melalui Transfusi Darah")

# ======================== Konektor Postgres (Supabase) ========================
# Mengikuti pola referensi: gunakan helper dari db.py (sudah menangani st.secrets, engine, dll)
from db import fetch_df as pg_fetch_df, exec_sql as pg_exec_sql  # ← sama seperti file referensi

TABLE = "public.infeksi_transfusi_darah"
TABLE_ORG = "public.identitas_organisasi"

# ======================== Helper DB ========================
def insert_row_pg(payload: dict, kode_organisasi: str):
    """
    Insert 1 baris ke public.infeksi_transfusi_darah.
    created_at diisi NOW() di server Postgres.
    Skema kolom:
      id, kode_organisasi, created_at, kasus, jml_hepatitis_c, jml_hiv, penyakit_menular_lainnya
    """
    sql = f"""
        INSERT INTO {TABLE}
            (kode_organisasi, created_at, kasus, jml_hepatitis_c, jml_hiv, penyakit_menular_lainnya)
        VALUES
            (:kode_organisasi, NOW(), :kasus, :jml_hepatitis_c, :jml_hiv, :penyakit_menular_lainnya)
    """
    params = {
        "kode_organisasi": kode_organisasi,
        "kasus": payload.get("kasus"),
        "jml_hepatitis_c": int(payload.get("jml_hepatitis_c") or 0),
        "jml_hiv": int(payload.get("jml_hiv") or 0),
        "penyakit_menular_lainnya": payload.get("penyakit_menular_lainnya"),
    }
    pg_exec_sql(sql, params)

def read_with_label_pg(limit=1000):
    """
    Ambil data + label organisasi (hmhi_cabang dan/atau kota_cakupan_cabang).
    """
    lim = int(limit)
    sql = f"""
        SELECT
          t.id, t.kode_organisasi, t.created_at,
          t.kasus, t.jml_hepatitis_c, t.jml_hiv, t.penyakit_menular_lainnya,
          io.hmhi_cabang AS label_organisasi
        FROM {TABLE} t
        LEFT JOIN {TABLE_ORG} io ON io.kode_organisasi = t.kode_organisasi
        ORDER BY t.id DESC
        LIMIT :lim
    """
    return pg_fetch_df(sql, {"lim": lim})

def load_kode_organisasi_with_label_pg():
    """
    Ambil mapping kode_organisasi -> label (hmhi_cabang). Jika kosong, tampilkan '-'.
    """
    try:
        df = pg_fetch_df(f"""
            SELECT kode_organisasi, COALESCE(NULLIF(hmhi_cabang,''), '-') AS label
            FROM {TABLE_ORG}
            ORDER BY id DESC
        """)
        if df.empty:
            return {}, []
        mapping = {row["kode_organisasi"]: str(row["label"]) for _, row in df.iterrows()}
        return mapping, df["kode_organisasi"].tolist()
    except Exception:
        return {}, []

def safe_int(val, default=0):
    try:
        x = pd.to_numeric(val, errors="coerce")
        return default if pd.isna(x) else int(x)
    except Exception:
        return default

# ======================== LABEL TETAP UNTUK "Kasus" ========================
KASUS_LABELS = [
    "Kasus lama (sebelum 2024)",
    "Kasus baru (2024/2025)",
]

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    mapping, kode_list = load_kode_organisasi_with_label_pg()
    if not kode_list:
        st.warning("Belum ada data Identitas Organisasi (public.identitas_organisasi).")
        kode_organisasi = None
    else:
        kode_organisasi = st.selectbox(
            "Pilih Organisasi (HMHI Cabang)",
            options=kode_list,
            format_func=lambda x: mapping.get(x, "-"),
            key="itd::kode_select"
        )

    st.subheader("Form Infeksi melalui Transfusi Darah")

    df_default = pd.DataFrame({
        "kasus": KASUS_LABELS,
        "jml_hepatitis_c": [0, 0],
        "jml_hiv": [0, 0],
        "penyakit_menular_lainnya": ["", ""],
    })

    with st.form("itd::form"):
        ed = st.data_editor(
            df_default,
            key="itd::editor",
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            disabled=["kasus"],
            column_config={
                "kasus": st.column_config.TextColumn("Kasus"),
                "jml_hepatitis_c": st.column_config.NumberColumn("Jumlah Hepatitis C", min_value=0, step=1),
                "jml_hiv": st.column_config.NumberColumn("Jumlah HIV", min_value=0, step=1),
                "penyakit_menular_lainnya": st.column_config.TextColumn("Penyakit menular lainnya"),
            },
        )
        submit = st.form_submit_button("💾 Simpan")

    if submit:
        if not kode_organisasi:
            st.error("Pilih organisasi terlebih dahulu.")
        else:
            n_saved, skipped = 0, 0
            for _, row in ed.iterrows():
                kasus = str(row.get("kasus") or "").strip()
                hc = safe_int(row.get("jml_hepatitis_c", 0))
                hiv = safe_int(row.get("jml_hiv", 0))
                lain = str(row.get("penyakit_menular_lainnya") or "").strip()

                # lewati baris benar-benar kosong
                if hc == 0 and hiv == 0 and not lain:
                    skipped += 1
                    continue

                payload = {
                    "kasus": kasus,
                    "jml_hepatitis_c": hc,
                    "jml_hiv": hiv,
                    "penyakit_menular_lainnya": lain,
                }
                try:
                    insert_row_pg(payload, kode_organisasi)
                    n_saved += 1
                except Exception as e:
                    st.error(f"Gagal menyimpan baris ({kasus}): {e}")

            if n_saved > 0:
                msg = f"{n_saved} baris tersimpan"
                if skipped > 0:
                    msg += f" ({skipped} baris kosong diabaikan)"
                st.success(f"{msg} untuk {mapping.get(kode_organisasi, '-')}.")
            else:
                st.info("Tidak ada baris valid untuk disimpan.")

with tab_data:
    st.subheader("📄 Data Tersimpan — Infeksi melalui Transfusi Darah")

    # ====== Unduh Template Excel ======
    st.markdown("#### 📄 Unduh Template Excel")
    tmpl_buf = io.BytesIO()
    df_template = pd.DataFrame(
        [
            {"kasus": "Kasus lama (sebelum 2024)", "jml_hepatitis_c": 0, "jml_hiv": 0,
             "penyakit_menular_lainnya": "", "kode_organisasi": "CAB001"},
            {"kasus": "Kasus baru (2024/2025)", "jml_hepatitis_c": 0, "jml_hiv": 0,
             "penyakit_menular_lainnya": "", "kode_organisasi": "CAB002"},
        ],
        columns=["kasus", "jml_hepatitis_c", "jml_hiv", "penyakit_menular_lainnya", "kode_organisasi"]
    )
    with pd.ExcelWriter(tmpl_buf, engine="xlsxwriter") as w:
        df_template.to_excel(w, index=False, sheet_name="Template_Infeksi_Transfusi")
    st.download_button(
        "⬇️ Unduh Template Excel",
        tmpl_buf.getvalue(),
        file_name="template_infeksi_transfusi_darah.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="itd::download_template"
    )

    # ====== Unggah Excel ======
    st.markdown("### ⬆️ Unggah Excel (Impor Massal)")
    st.caption("File harus menyertakan semua kolom wajib.")
    up = st.file_uploader("Pilih file Excel (.xlsx)", type=["xlsx"], key="itd::uploader")

    def process_upload(df_up: pd.DataFrame):
        results, n_ok, n_skip = [], 0, 0
        for i, r in df_up.iterrows():
            try:
                kasus = str(r.get("kasus") or "").strip()
                hc = safe_int(r.get("jml_hepatitis_c", 0))
                hiv = safe_int(r.get("jml_hiv", 0))
                lain = str(r.get("penyakit_menular_lainnya") or "").strip()
                korg = str(r.get("kode_organisasi") or "").strip()

                if not korg:
                    n_skip += 1
                    results.append({"Baris Excel": i+2, "Status": "LEWAT", "Keterangan": "kode_organisasi kosong"})
                    continue
                if not kasus and hc == 0 and hiv == 0 and not lain:
                    n_skip += 1
                    results.append({"Baris Excel": i+2, "Status": "LEWAT", "Keterangan": "Baris kosong"})
                    continue

                insert_row_pg({
                    "kasus": kasus,
                    "jml_hepatitis_c": hc,
                    "jml_hiv": hiv,
                    "penyakit_menular_lainnya": lain,
                }, korg)

                results.append({"Baris Excel": i+2, "Status": "OK", "Keterangan": f"Simpan → {korg} / {kasus or '(tanpa kasus)'}"})
                n_ok += 1
            except Exception as e:
                results.append({"Baris Excel": i+2, "Status": "GAGAL", "Keterangan": str(e)})
        return pd.DataFrame(results), n_ok, n_skip

    if up is not None:
        try:
            raw = pd.read_excel(up)
            raw.columns = [str(c).strip() for c in raw.columns]
            st.dataframe(raw.head(20), use_container_width=True)

            # Normalisasi header → snake_case minimal
            norm_map = {c: c.strip().lower().replace(" ", "_") for c in raw.columns}
            df_norm = raw.rename(columns=norm_map)

            required = ["kasus", "jml_hepatitis_c", "jml_hiv", "penyakit_menular_lainnya", "kode_organisasi"]
            missing = [c for c in required if c not in df_norm.columns]
            if missing:
                st.error(f"Kolom wajib belum lengkap di file: {missing}")
            else:
                log_df, n_ok, n_skip = process_upload(df_norm)
                st.write("**Hasil unggah:**")
                st.dataframe(log_df, use_container_width=True)

                ok = (log_df["Status"] == "OK").sum()
                fail = (log_df["Status"] == "GAGAL").sum()
                skip = (log_df["Status"] == "LEWAT").sum()
                if ok: st.success(f"Berhasil menyimpan {ok} baris.")
                if fail: st.error(f"Gagal menyimpan {fail} baris.")
                if skip: st.info(f"Dilewati {skip} baris.")

                # Unduh log
                log_buf = io.BytesIO()
                with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                    log_df.to_excel(w, index=False, sheet_name="Hasil")
                st.download_button(
                    "📄 Unduh Log Hasil",
                    log_buf.getvalue(),
                    file_name="log_hasil_unggah_infeksi_transfusi_darah.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="itd::dl_log"
                )
        except Exception as e:
            st.error(f"Gagal memproses file: {e}")

    # ====== Tampilkan data ======
    try:
        df = read_with_label_pg(limit=1000)
    except Exception as e:
        st.error(f"Gagal membaca data dari Supabase: {e}")
        df = pd.DataFrame()

    if df.empty:
        st.info("Belum ada data tersimpan.")
    else:
        cols_order = ["label_organisasi", "created_at", "kasus", "jml_hepatitis_c", "jml_hiv", "penyakit_menular_lainnya"]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "label_organisasi": "HMHI Cabang",
            "created_at": "Created At",
            "kasus": "Kasus",
            "jml_hepatitis_c": "Jumlah Hepatitis C",
            "jml_hiv": "Jumlah HIV",
            "penyakit_menular_lainnya": "Penyakit menular lainnya",
        })

        # Hindari masalah tz saat ekspor → stringify waktu
        if "Created At" in view.columns:
            view["Created At"] = view["Created At"].astype(str)

        st.dataframe(view, use_container_width=True)

        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="Infeksi_Transfusi_Darah")
        st.download_button(
            "⬇️ Unduh Data Excel",
            buf.getvalue(),
            file_name="infeksi_transfusi_darah.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="itd::download"
        )
