import streamlit as st
import pandas as pd
from datetime import datetime
import io

# =============== UI CONFIG ===============
st.set_page_config(page_title="Rumah Sakit Penangan Hemofilia", page_icon="🏥", layout="wide")
st.title("🏥 Rumah Sakit yang Menangani Hemofilia")

# =============== KONSTAN DB ===============
TABLE_RS_PENANGAN = "public.rs_penangan_hemofilia"
TABLE_ORG         = "public.identitas_organisasi"
TABLE_RS_MASTER   = "public.rumah_sakit"

TIPE_RS_OPTIONS = [
    "RSU Tipe A", "RSU Tipe B", "RSU Tipe C", "RSU Tipe D",
    "RS Swasta Tipe A", "RS Swasta Tipe B", "RS Swasta Tipe C", "RS Swasta Tipe D",
]
YA_TIDAK_OPTIONS = ["Ya", "Tidak"]

TEMPLATE_COLUMNS = [
    "HMHI cabang",            # dipetakan ke kode_organisasi via identitas_organisasi
    "Kode RS",                # pilih dari master RS (kode_rs)
    "Nama Rumah Sakit",       # opsional; fallback dari master jika kosong & Kode RS ada
    "Tipe RS",                # opsional; boleh override master
    "Terdapat Dokter Hematologi",     # Ya/Tidak
    "Terdapat Tim Terpadu Hemofilia", # Ya/Tidak
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Kode RS": "kode_rs",
    "Nama Rumah Sakit": "nama_rumah_sakit",
    "Tipe RS": "tipe_rs",
    "Terdapat Dokter Hematologi": "dokter_hematologi",
    "Terdapat Tim Terpadu Hemofilia": "tim_terpadu",
}

# =============== KONEKTOR DB (ikuti pola Identitas Organisasi) ===============
# Menggunakan helper dari db.py seperti pada halaman Identitas Organisasi
from db import fetch_df as pg_fetch_df, exec_sql as pg_exec_sql  # :contentReference[oaicite:1]{index=1}

# =============== HELPERS ===============
def load_hmhi_to_kode() -> tuple[dict, list]:
    """Map hmhi_cabang -> kode_organisasi dari identitas_organisasi."""
    df = pg_fetch_df(f"""
        SELECT kode_organisasi, hmhi_cabang
        FROM {TABLE_ORG}
        WHERE hmhi_cabang IS NOT NULL AND hmhi_cabang <> ''
        ORDER BY id DESC
    """)
    if df.empty:
        return {}, []
    mapping = {str(r["hmhi_cabang"]).strip(): str(r["kode_organisasi"]).strip() for _, r in df.iterrows()}
    return mapping, sorted(mapping.keys())

def load_rs_master() -> pd.DataFrame:
    """Master RS dari public.rumah_sakit (kode_rs, nama_rs, kota, provinsi, tipe_rs, kelas_rs, kontak)."""
    return pg_fetch_df(f"""
        SELECT kode_rs, nama_rs, kota, provinsi, tipe_rs, kelas_rs, kontak
        FROM {TABLE_RS_MASTER}
        ORDER BY nama_rs
    """)

def insert_row(payload: dict, kode_organisasi: str) -> None:
    """
    Insert ke public.rs_penangan_hemofilia (created_at default NOW() di DB):
      kolom: id, kode_organisasi, kode_rs, created_at, nama_rumah_sakit, tipe_rs, dokter_hematologi, tim_terpadu
    """
    sql = f"""
        INSERT INTO {TABLE_RS_PENANGAN}
            (kode_organisasi, kode_rs, nama_rumah_sakit, tipe_rs, dokter_hematologi, tim_terpadu)
        VALUES
            (:kode_organisasi, :kode_rs, :nama_rumah_sakit, :tipe_rs, :dokter_hematologi, :tim_terpadu)
    """
    params = {
        "kode_organisasi": kode_organisasi,
        "kode_rs": payload.get("kode_rs") or None,
        "nama_rumah_sakit": payload.get("nama_rumah_sakit") or None,
        "tipe_rs": payload.get("tipe_rs") or None,
        "dokter_hematologi": payload.get("dokter_hematologi") or None,
        "tim_terpadu": payload.get("tim_terpadu") or None,
    }
    pg_exec_sql(sql, params)

def read_rs_penangan_with_join(limit: int = 500) -> pd.DataFrame:
    """
    Ambil data tersimpan + join identitas_organisasi & rumah_sakit.
    Nama/Tipe RS diambil dari entri penangan jika ada; kalau NULL, fallback ke master RS.
    """
    return pg_fetch_df(f"""
        SELECT
          t.id, t.created_at,
          t.kode_organisasi, io.hmhi_cabang, io.kota_cakupan_cabang,
          t.kode_rs,
          COALESCE(t.nama_rumah_sakit, rs.nama_rs) AS nama_rumah_sakit,
          COALESCE(t.tipe_rs, rs.tipe_rs)          AS tipe_rs,
          t.dokter_hematologi, t.tim_terpadu,
          rs.kota, rs.provinsi, rs.kelas_rs, rs.kontak
        FROM {TABLE_RS_PENANGAN} t
        LEFT JOIN {TABLE_ORG} io ON io.kode_organisasi = t.kode_organisasi
        LEFT JOIN {TABLE_RS_MASTER} rs ON rs.kode_rs = t.kode_rs
        ORDER BY t.id DESC
        LIMIT {int(limit)}
    """)

# =============== UI ===============
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

# ---------- TAB INPUT ----------
with tab_input:
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox("Pilih HMHI Cabang (Provinsi)", options=hmhi_list, key="rs::hmhi_select")

    df_rs = load_rs_master()
    if df_rs.empty:
        st.error("Master rumah_sakit kosong. Tambahkan data ke public.rumah_sakit (kode_rs, nama_rs, kota, provinsi, dst.).")
    else:
        # Opsi label ramah: "KODE — Nama (Kota, Provinsi)"
        rs_map_by_label = {}
        rs_options = [""]
        for _, r in df_rs.iterrows():
            kode_rs = (str(r["kode_rs"]).strip() if pd.notna(r["kode_rs"]) else "")
            nama    = (str(r["nama_rs"]).strip() if pd.notna(r["nama_rs"]) else "")
            kota    = (str(r["kota"]).strip() if pd.notna(r["kota"]) else "")
            prov    = (str(r["provinsi"]).strip() if pd.notna(r["provinsi"]) else "")
            label = f"{kode_rs} — {nama}" + (f" ({kota}, {prov})" if (kota or prov) else "")
            rs_map_by_label[label] = {"kode_rs": kode_rs, "nama_rs": nama, "tipe_rs": r.get("tipe_rs")}
            rs_options.append(label)

        # Template editor
        df_default = pd.DataFrame({
            "rs_label": ["", "", "", "", ""],           # pilih dari master
            "override_nama": ["", "", "", "", ""],      # opsional
            "override_tipe": ["", "", "", "", ""],      # opsional
            "dokter_hematologi": ["", "", "", "", ""],  # Ya/Tidak
            "tim_terpadu": ["", "", "", "", ""],        # Ya/Tidak
        })

        with st.form("rs::form"):
            edited = st.data_editor(
                df_default,
                key="rs::editor",
                column_config={
                    "rs_label": st.column_config.SelectboxColumn(
                        "Pilih RS (Kode — Nama — Lokasi)", options=rs_options, required=False
                    ),
                    "override_nama": st.column_config.TextColumn("Override Nama RS (opsional)"),
                    "override_tipe": st.column_config.SelectboxColumn("Override Tipe RS (opsional)", options=[""] + TIPE_RS_OPTIONS),
                    "dokter_hematologi": st.column_config.SelectboxColumn("Terdapat Dokter Hematologi", options=[""] + YA_TIDAK_OPTIONS),
                    "tim_terpadu": st.column_config.SelectboxColumn("Terdapat Tim Terpadu Hemofilia", options=[""] + YA_TIDAK_OPTIONS),
                },
                use_container_width=True,
                num_rows="dynamic",
                hide_index=True,
            )

            # Pratinjau
            if not edited.empty:
                prv = []
                for _, r in edited.iterrows():
                    lbl   = (r.get("rs_label") or "").strip()
                    base  = rs_map_by_label.get(lbl, {})
                    nama  = r.get("override_nama") or base.get("nama_rs") or ""
                    tipe  = r.get("override_tipe") or base.get("tipe_rs") or ""
                    prv.append({
                        "Kode RS": base.get("kode_rs", ""),
                        "Nama RS (hasil)": nama,
                        "Tipe RS (hasil)": tipe,
                        "Dokter Hematologi": r.get("dokter_hematologi", ""),
                        "Tim Terpadu": r.get("tim_terpadu", ""),
                    })
                st.caption("Pratinjau baris yang akan disimpan:")
                st.dataframe(pd.DataFrame(prv), use_container_width=True)

            submitted = st.form_submit_button("💾 Simpan")

        if submitted:
            if not selected_hmhi:
                st.error("Pilih HMHI cabang terlebih dahulu.")
            else:
                kode_organisasi = hmhi_map.get(selected_hmhi)
                if not kode_organisasi:
                    st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
                else:
                    n_saved = 0
                    for _, r in edited.iterrows():
                        lbl = (r.get("rs_label") or "").strip()
                        if not lbl:
                            continue
                        base = rs_map_by_label.get(lbl, {})
                        kode_rs = base.get("kode_rs", "")
                        if not kode_rs:
                            continue  # wajib pilih RS dari master

                        nama_final = (r.get("override_nama") or base.get("nama_rs") or "").strip()
                        tipe_final = (r.get("override_tipe") or base.get("tipe_rs") or "").strip()
                        dokter = (r.get("dokter_hematologi") or "").strip()
                        tim    = (r.get("tim_terpadu") or "").strip()

                        # Validasi
                        if dokter and dokter not in YA_TIDAK_OPTIONS:
                            st.warning(f"Baris di-skip: 'Terdapat Dokter Hematologi' harus Ya/Tidak. Nilai: {dokter}")
                            continue
                        if tim and tim not in YA_TIDAK_OPTIONS:
                            st.warning(f"Baris di-skip: 'Terdapat Tim Terpadu Hemofilia' harus Ya/Tidak. Nilai: {tim}")
                            continue
                        if not nama_final:
                            st.warning("Baris di-skip: Nama RS kosong (master & override kosong).")
                            continue

                        payload = {
                            "kode_rs": kode_rs,
                            "nama_rumah_sakit": nama_final,
                            "tipe_rs": tipe_final if tipe_final in TIPE_RS_OPTIONS else None,
                            "dokter_hematologi": dokter if dokter in YA_TIDAK_OPTIONS else None,
                            "tim_terpadu": tim if tim in YA_TIDAK_OPTIONS else None,
                        }
                        insert_row(payload, kode_organisasi)
                        n_saved += 1

                    if n_saved:
                        st.success(f"{n_saved} baris tersimpan untuk **{selected_hmhi}**.")
                    else:
                        st.info("Tidak ada baris valid untuk disimpan.")

# ---------- TAB DATA ----------
with tab_data:
    st.subheader("📄 Data Tersimpan")
    df = read_rs_penangan_with_join(limit=500)

    if df.empty:
        st.info("Belum ada data.")
    else:
        cols_order = [
            "hmhi_cabang", "kota_cakupan_cabang", "created_at",
            "kode_rs", "nama_rumah_sakit", "tipe_rs",
            "dokter_hematologi", "tim_terpadu",
            "kota", "provinsi", "kelas_rs", "kontak"
        ]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "kode_rs": "Kode RS",
            "nama_rumah_sakit": "Nama Rumah Sakit",
            "tipe_rs": "Tipe RS",
            "dokter_hematologi": "Terdapat Dokter Hematologi",
            "tim_terpadu": "Terdapat Tim Terpadu Hemofilia",
            "kota": "Kota",
            "provinsi": "Provinsi",
            "kelas_rs": "Kelas RS",
            "kontak": "Kontak",
        })
        st.dataframe(view, use_container_width=True)

        # Unduh Excel
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="RS_Penangan_Hemofilia")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buf.getvalue(),
            file_name="rs_penangan_hemofilia.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="rs::download"
        )

    st.divider()
    st.markdown("### 📥 Template & Unggah Excel")

    # Template kosong
    tmpl_df = pd.DataFrame([{
        "HMHI cabang": "",
        "Kode RS": "",
        "Nama Rumah Sakit": "",
        "Tipe RS": "",
        "Terdapat Dokter Hematologi": "",
        "Terdapat Tim Terpadu Hemofilia": "",
    }], columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template_RS")
    st.download_button(
        "📄 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_rs_penangan_hemofilia.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="rs::dl_template"
    )

    # Upload
    up = st.file_uploader("Unggah file Excel (.xlsx) sesuai template", type=["xlsx"], key="rs::uploader")

    def process_upload(df_up: pd.DataFrame):
        hmhi_map, _ = load_hmhi_to_kode()
        rs_master = load_rs_master().copy()
        rs_master["kode_rs_str"] = rs_master["kode_rs"].astype(str).str.strip()

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

                kode_rs = str((s.get("kode_rs") or "")).strip()
                nama    = str((s.get("nama_rumah_sakit") or "")).strip()
                tipe    = str((s.get("tipe_rs") or "")).strip()
                dokter  = str((s.get("dokter_hematologi") or "")).strip()
                tim     = str((s.get("tim_terpadu") or "")).strip()

                if kode_rs:
                    m = rs_master[rs_master["kode_rs_str"].str.casefold() == kode_rs.casefold()]
                    if m.empty:
                        raise ValueError(f"Kode RS '{kode_rs}' tidak ada di master.")
                    if not nama:
                        nama = str(m.iloc[0]["nama_rs"] or "").strip()
                    if not tipe:
                        tipe = str(m.iloc[0]["tipe_rs"] or "").strip()

                if dokter and dokter not in YA_TIDAK_OPTIONS:
                    raise ValueError("Kolom 'Terdapat Dokter Hematologi' harus 'Ya' atau 'Tidak'")
                if tim and tim not in YA_TIDAK_OPTIONS:
                    raise ValueError("Kolom 'Terdapat Tim Terpadu Hemofilia' harus 'Ya' atau 'Tidak'")

                if not nama:
                    raise ValueError("Nama Rumah Sakit kosong (isi manual atau pilih Kode RS).")

                payload = {
                    "kode_rs": kode_rs or None,
                    "nama_rumah_sakit": nama,
                    "tipe_rs": tipe if tipe in TIPE_RS_OPTIONS else None,
                    "dokter_hematologi": dokter if dokter in YA_TIDAK_OPTIONS else None,
                    "tim_terpadu": tim if tim in YA_TIDAK_OPTIONS else None,
                }
                insert_row(payload, kode_organisasi)
                results.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {kode_rs or '-'} / {nama}"})
            except Exception as e:
                results.append({"Baris Excel": i + 2, "Status": "GAGAL", "Keterangan": str(e)})

        return pd.DataFrame(results)

    if up is not None:
        try:
            raw = pd.read_excel(up)
            raw.columns = [str(c).strip() for c in raw.columns]
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        missing = [c for c in ["HMHI cabang"] if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Minimal harus ada: " + ", ".join(missing))
            st.stop()

        for c in TEMPLATE_COLUMNS:
            if c not in raw.columns:
                raw[c] = ""

        st.caption("Pratinjau 20 baris pertama:")
        st.dataframe(raw[TEMPLATE_COLUMNS].head(20), use_container_width=True)

        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        if st.button("🚀 Proses & Simpan Unggahan", type="primary", key="rs::process"):
            log_df = process_upload(df_up)
            st.write("**Hasil unggah:**")
            st.dataframe(log_df, use_container_width=True)

            ok = (log_df["Status"] == "OK").sum()
            fail = (log_df["Status"] == "GAGAL").sum()
            if ok:
                st.success(f"Berhasil menyimpan {ok} baris.")
            if fail:
                st.error(f"Gagal menyimpan {fail} baris.")

            # Unduh log
            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                log_df.to_excel(w, index=False, sheet_name="Hasil")
            st.download_button(
                "📄 Unduh Log Hasil",
                log_buf.getvalue(),
                file_name="log_hasil_unggah_rs_penangan_hemofilia.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="rs::dl_log"
            )
