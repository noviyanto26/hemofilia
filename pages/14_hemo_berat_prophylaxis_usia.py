import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="Hemofilia Berat - Prophylaxis per Usia", page_icon="📊", layout="wide")
st.title("📊 Persentase Penyandang Hemofilia Berat dengan Prophylaxis Berdasarkan Usia")

# ====== Target tabel di Postgres (Supabase) ======
SUPABASE_TABLE = "public.hemo_berat_prophylaxis_usia"
IDENT_TABLE    = "public.identitas_organisasi"

# Konektor Postgres yang sama seperti referensi (db.py)
from db import fetch_df as pg_fetch_df, exec_sql as pg_exec_sql

# ======================== Label statis & opsi lain ========================
JENIS_LABELS = [
    "Hemofilia A Berat",
    "Hemofilia B Berat",
    "Hemofilia tipe lain",
    "vWD",
]
YA_TIDAK_OPTIONS = ["Ya", "Tidak"]
ESTIMASI_REAL_OPTIONS = ["Estimasi", "Data real"]

# ======================== Template Unggah ========================
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Jenis",
    "0–18 tahun (%)",
    ">18 tahun (%)",
    "Frekuensi",
    "Produk yang digunakan",
    "Tidak ada data",
    "Dosis diterima (IU)/kedatangan",
    "Estimasi/Data real",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",                 # kolom bantu → mapping ke kode_organisasi
    "Jenis": "jenis",
    "0–18 tahun (%)": "persen_0_18",
    ">18 tahun (%)": "persen_gt_18",
    "Frekuensi": "frekuensi",
    "Produk yang digunakan": "produk",
    "Tidak ada data": "tidak_ada_data",
    "Dosis diterima (IU)/kedatangan": "dosis_per_kedatangan",
    "Estimasi/Data real": "estimasi_data_real",
}

# ======================== Helper umum ========================
def safe_float(val, default=0.0):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default

def load_hmhi_to_kode_pg():
    """
    Ambil mapping HMHI cabang -> kode_organisasi dari public.identitas_organisasi (Supabase).
    Return:
        mapping: dict {hmhi_cabang -> kode_organisasi}
        options: list[str] hmhi_cabang (urut alfabet)
    """
    try:
        df = pg_fetch_df(f"""
            SELECT hmhi_cabang, kode_organisasi
            FROM {IDENT_TABLE}
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

def insert_row_pg(payload: dict, kode_organisasi: str):
    """
    INSERT 1 baris ke public.hemo_berat_prophylaxis_usia.
    created_at diisi NOW() (timestamp server).
    """
    sql = f"""
    INSERT INTO {SUPABASE_TABLE} (
        kode_organisasi, created_at,
        jenis, persen_0_18, persen_gt_18, frekuensi,
        produk, tidak_ada_data, dosis_per_kedatangan, estimasi_data_real
    )
    VALUES (
        :kode_organisasi, NOW(),
        :jenis, :persen_0_18, :persen_gt_18, :frekuensi,
        :produk, :tidak_ada_data, :dosis_per_kedatangan, :estimasi_data_real
    )
    """
    params = {
        "kode_organisasi": kode_organisasi,
        "jenis": payload.get("jenis"),
        "persen_0_18": safe_float(payload.get("persen_0_18"), 0.0),
        "persen_gt_18": safe_float(payload.get("persen_gt_18"), 0.0),
        "frekuensi": payload.get("frekuensi"),
        "produk": payload.get("produk"),
        "tidak_ada_data": payload.get("tidak_ada_data"),
        "dosis_per_kedatangan": safe_float(payload.get("dosis_per_kedatangan"), 0.0),
        "estimasi_data_real": payload.get("estimasi_data_real"),
    }
    pg_exec_sql(sql, params)

def read_with_kota_pg(limit=1000):
    """
    Ambil data + join identitas_organisasi untuk hmhi_cabang & kota_cakupan_cabang.
    """
    lim = int(limit)
    q = f"""
    SELECT
      t.id, t.kode_organisasi, t.created_at,
      t.jenis, t.persen_0_18, t.persen_gt_18, t.frekuensi,
      t.produk, t.tidak_ada_data, t.dosis_per_kedatangan, t.estimasi_data_real,
      io.hmhi_cabang,
      io.kota_cakupan_cabang
    FROM {SUPABASE_TABLE} t
    LEFT JOIN {IDENT_TABLE} io
      ON io.kode_organisasi = t.kode_organisasi
    ORDER BY t.id DESC
    LIMIT {lim}
    """
    return pg_fetch_df(q)

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # ---- Pilihan dari HMHI Cabang (bukan Kode Organisasi) ----
    hmhi_map, hmhi_list = load_hmhi_to_kode_pg()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (HMHI cabang) di Supabase.")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="hbpu::hmhi_select"
        )

    st.subheader("Form Persentase Hemofilia Berat – Prophylaxis per Usia")

    # Data default: baris fixed sesuai label "Jenis", kolom "jenis" dikunci
    df_default = pd.DataFrame({
        "jenis": JENIS_LABELS,
        "persen_0_18": [0.0] * len(JENIS_LABELS),   # 0–100
        "persen_gt_18": [0.0] * len(JENIS_LABELS),  # 0–100
        "frekuensi": [""] * len(JENIS_LABELS),
        "produk": [""] * len(JENIS_LABELS),
        "tidak_ada_data": [""] * len(JENIS_LABELS),
        "dosis_per_kedatangan": [0.0] * len(JENIS_LABELS),
        "estimasi_data_real": [""] * len(JENIS_LABELS),
    })

    with st.form("hbpu::form"):
        ed = st.data_editor(
            df_default,
            key="hbpu::editor",
            use_container_width=True,
            num_rows="fixed",
            hide_index=True,
            disabled=["jenis"],
            column_config={
                "jenis": st.column_config.TextColumn("Jenis"),
                "persen_0_18": st.column_config.NumberColumn(
                    "0–18 tahun (%)", help="Masukkan persentase 0–100.",
                    min_value=0.0, max_value=100.0, step=1.0, format="%.0f"
                ),
                "persen_gt_18": st.column_config.NumberColumn(
                    ">18 tahun (%)", help="Masukkan persentase 0–100.",
                    min_value=0.0, max_value=100.0, step=1.0, format="%.0f"
                ),
                "frekuensi": st.column_config.TextColumn("Frekuensi"),
                "produk": st.column_config.TextColumn("Produk yang digunakan"),
                "tidak_ada_data": st.column_config.SelectboxColumn(
                    "Tidak ada data", options=[""] + YA_TIDAK_OPTIONS, required=False
                ),
                "dosis_per_kedatangan": st.column_config.NumberColumn(
                    "Dosis yang diterima (IU)/kedatangan",
                    min_value=0.0, step=50.0, format="%.0f"
                ),
                "estimasi_data_real": st.column_config.SelectboxColumn(
                    "Estimasi/Data real", options=[""] + ESTIMASI_REAL_OPTIONS, required=False
                ),
            },
        )
        submit = st.form_submit_button("💾 Simpan")

    if submit:
        # peringatan ringan bila persentase out-of-range (tetap disimpan)
        bad = []
        for i, row in ed.iterrows():
            p1 = safe_float(row.get("persen_0_18", 0))
            p2 = safe_float(row.get("persen_gt_18", 0))
            if p1 < 0 or p1 > 100 or p2 < 0 or p2 > 100:
                bad.append(i + 1)
        if bad:
            st.warning(f"Baris dengan persentase di luar 0–100: {bad}. Data tetap disimpan.")

        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                n_saved = 0
                for _, row in ed.iterrows():
                    jenis = str(row.get("jenis") or "").strip()
                    p0_18 = safe_float(row.get("persen_0_18", 0.0))
                    pgt18 = safe_float(row.get("persen_gt_18", 0.0))
                    frek = str(row.get("frekuensi") or "").strip()
                    prod = str(row.get("produk") or "").strip()
                    tdd  = str(row.get("tidak_ada_data") or "").strip()
                    dosis = safe_float(row.get("dosis_per_kedatangan", 0.0))
                    est  = str(row.get("estimasi_data_real") or "").strip()

                    # lewati baris benar-benar kosong
                    if (p0_18 == 0 and pgt18 == 0 and not frek and not prod and not tdd and dosis == 0 and not est and not jenis):
                        continue

                    payload = {
                        "jenis": jenis,
                        "persen_0_18": p0_18,
                        "persen_gt_18": pgt18,
                        "frekuensi": frek,
                        "produk": prod,
                        "tidak_ada_data": tdd,
                        "dosis_per_kedatangan": dosis,
                        "estimasi_data_real": est,
                    }
                    try:
                        insert_row_pg(payload, kode_organisasi)
                        n_saved += 1
                    except Exception as e:
                        st.error(f"Gagal menyimpan baris ({jenis}): {e}")

                if n_saved > 0:
                    st.success(f"{n_saved} baris tersimpan untuk **{selected_hmhi}**.")
                else:
                    st.info("Tidak ada baris valid untuk disimpan.")

with tab_data:
    st.subheader("📄 Data Tersimpan")
    try:
        df = read_with_kota_pg(limit=1000)
    except Exception as e:
        st.error(f"Gagal membaca data dari Supabase: {e}")
        df = pd.DataFrame()

    if df.empty:
        st.info("Belum ada data tersimpan.")
    else:
        cols_order = [
            "hmhi_cabang", "kota_cakupan_cabang", "created_at",
            "jenis", "persen_0_18", "persen_gt_18",
            "frekuensi", "produk", "tidak_ada_data",
            "dosis_per_kedatangan", "estimasi_data_real",
        ]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "jenis": "Jenis",
            "persen_0_18": "0–18 tahun (%)",
            "persen_gt_18": ">18 tahun (%)",
            "frekuensi": "Frekuensi",
            "produk": "Produk yang digunakan",
            "tidak_ada_data": "Tidak ada data",
            "dosis_per_kedatangan": "Dosis diterima (IU)/kedatangan",
            "estimasi_data_real": "Estimasi/Data real",
        })

        # Hindari error Excel datetime tz → stringify bila ada
        if "Created At" in view.columns:
            view["Created At"] = view["Created At"].astype(str)

        st.dataframe(view, use_container_width=True)

        # Unduh Excel (data tersimpan)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="HemoBerat_Prophylaxis_Usia")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buf.getvalue(),
            file_name="hemo_berat_prophylaxis_usia.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="hbpu::download"
        )

    # ======================== Template & Unggah ========================
    st.divider()
    st.markdown("### 📥 Template & Unggah Excel")

    # Unduh template kosong
    template_df = pd.DataFrame([{
        "HMHI cabang": "",
        "Jenis": "",
        "0–18 tahun (%)": 0,
        ">18 tahun (%)": 0,
        "Frekuensi": "",
        "Produk yang digunakan": "",
        "Tidak ada data": "",
        "Dosis diterima (IU)/kedatangan": 0,
        "Estimasi/Data real": "",
    }], columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        template_df.to_excel(w, index=False, sheet_name="Template_HBPU")
    st.download_button(
        "📄 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_hemo_berat_prophylaxis_usia.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="hbpu::dl_template"
    )

    up = st.file_uploader(
        "Unggah file Excel (.xlsx) sesuai template",
        type=["xlsx"],
        key="hbpu::uploader"
    )

    def process_upload(df_up: pd.DataFrame):
        """Validasi & simpan unggahan. Return (log_df, n_ok)."""
        hmhi_map, _ = load_hmhi_to_kode_pg()
        results = []
        n_ok = 0

        for i in range(len(df_up)):
            try:
                s = df_up.iloc[i]  # Series
                hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                if not hmhi:
                    raise ValueError("Kolom 'HMHI cabang' kosong.")
                kode_organisasi = hmhi_map.get(hmhi)
                if not kode_organisasi:
                    raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                jenis = str((s.get("jenis") or "")).strip()
                p0_18 = safe_float(s.get("persen_0_18", 0.0))
                pgt18 = safe_float(s.get("persen_gt_18", 0.0))
                frek  = str((s.get("frekuensi") or "")).strip()
                prod  = str((s.get("produk") or "")).strip()
                tdd   = str((s.get("tidak_ada_data") or "")).strip()
                dosis = safe_float(s.get("dosis_per_kedatangan", 0.0))
                est   = str((s.get("estimasi_data_real") or "")).strip()

                warn = []
                if p0_18 < 0 or p0_18 > 100: warn.append("0–18% di luar 0–100")
                if pgt18 < 0 or pgt18 > 100: warn.append(">18% di luar 0–100")

                # lewati baris benar-benar kosong
                if (p0_18 == 0 and pgt18 == 0 and not frek and not prod and not tdd and dosis == 0 and not est and not jenis):
                    results.append({"Baris Excel": i+2, "Status": "LEWAT", "Keterangan": "Baris kosong — dilewati"})
                    continue

                payload = {
                    "jenis": jenis,
                    "persen_0_18": p0_18,
                    "persen_gt_18": pgt18,
                    "frekuensi": frek,
                    "produk": prod,
                    "tidak_ada_data": tdd,
                    "dosis_per_kedatangan": dosis,
                    "estimasi_data_real": est,
                }
                insert_row_pg(payload, kode_organisasi)

                info = f"Simpan → {hmhi} / {jenis or '(tanpa jenis)'}"
                if warn: info += " | Peringatan: " + "; ".join(warn)
                results.append({"Baris Excel": i+2, "Status": "OK", "Keterangan": info})
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
        minimal = ["HMHI cabang", "Jenis"]
        missing = [c for c in minimal if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Minimal harus ada: " + ", ".join(minimal))
            st.stop()

        # Pastikan semua kolom template ada
        for c in TEMPLATE_COLUMNS:
            if c not in raw.columns:
                if c in ("0–18 tahun (%)", ">18 tahun (%)", "Dosis diterima (IU)/kedatangan"):
                    raw[c] = 0
                else:
                    raw[c] = ""

        st.caption("Pratinjau 20 baris pertama:")
        st.dataframe(raw[TEMPLATE_COLUMNS].head(20), use_container_width=True)

        # Ubah header → nama kolom internal
        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        if st.button("🚀 Proses & Simpan Unggahan", type="primary", key="hbpu::process"):
            log_df, n_ok = process_upload(df_up)
            st.write("**Hasil unggah:**")
            st.dataframe(log_df, use_container_width=True)

            ok = (log_df["Status"] == "OK").sum()
            fail = (log_df["Status"] == "GAGAL").sum()
            skip = (log_df["Status"] == "LEWAT").sum()
            if ok: st.success(f"Berhasil menyimpan {ok} baris.")
            if fail: st.error(f"Gagal menyimpan {fail} baris.")
            if skip: st.info(f"Dilewati {skip} baris kosong.")

            # Unduh log hasil
            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                log_df.to_excel(w, index=False, sheet_name="Hasil")
            st.download_button(
                "📄 Unduh Log Hasil",
                log_buf.getvalue(),
                file_name="log_hasil_unggah_hemo_berat_prophylaxis_usia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="hbpu::dl_log"
            )
