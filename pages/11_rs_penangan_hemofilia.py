import io
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# =========================
# Konfigurasi & Konstanta
# =========================
st.set_page_config(page_title="Rumah Sakit Penangan Hemofilia", page_icon="🏥", layout="wide")
st.title("🏥 Rumah Sakit yang Menangani Hemofilia")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = str((BASE_DIR / "hemofilia.db").resolve())

TABLE = "rs_penangan_hemofilia"  # skema mengikuti Postgres

# ===== Template unggah (sesuai FK & kolom Postgres) =====
TEMPLATE_COLUMNS = [
    "HMHI cabang",  # ke kode_organisasi
    "Kode RS",      # FK ke rumah_sakit.kode_rs
    "Layanan",
    "Catatan",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Kode RS": "kode_rs",
    "Layanan": "layanan",
    "Catatan": "catatan",
}

# ======================== Util DB ========================
def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def _table_exists(conn, name: str) -> bool:
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
    return cur.fetchone() is not None

def ensure_identitas_schema():
    """Pastikan tabel identitas_organisasi ada (minimal kolom yang dipakai UI)."""
    with connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS identitas_organisasi (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT UNIQUE,
                hmhi_cabang TEXT,
                kota_cakupan_cabang TEXT
            )
        """)
        conn.commit()

def ensure_rumah_sakit_schema():
    """Pastikan master rumah_sakit selaras dengan Postgres."""
    with connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS rumah_sakit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_rs TEXT UNIQUE,
                nama_rs TEXT NOT NULL,
                provinsi TEXT,
                kota TEXT,
                tipe_rs TEXT,
                kelas_rs TEXT,
                kontak TEXT
            )
        """)
        conn.commit()

def init_rs_penangan_schema():
    """Skema utama mengikuti Postgres: rs_penangan_hemofilia."""
    with connect() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT,
                kode_rs TEXT,
                d_at TEXT NOT NULL,     -- gunakan ISO UTC, ekuivalen now() at time zone UTC
                layanan TEXT,
                catatan TEXT,
                FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi),
                FOREIGN KEY (kode_rs) REFERENCES rumah_sakit(kode_rs)
            )
        """)
        conn.commit()

# ======================== Helpers ========================
def load_hmhi_to_kode():
    """Map hmhi_cabang -> kode_organisasi dari identitas_organisasi."""
    with connect() as conn:
        df = pd.read_sql_query(
            "SELECT kode_organisasi, hmhi_cabang FROM identitas_organisasi ORDER BY id DESC",
            conn
        )
    if df.empty:
        return {}, []
    mapping = {}
    for _, r in df.iterrows():
        hmhi = str(r["hmhi_cabang"] or "").strip()
        ko = str(r["kode_organisasi"] or "").strip()
        if hmhi:
            mapping[hmhi] = ko
    return mapping, sorted(mapping.keys())

def load_rs_master():
    """Ambil master RS + buat label pilihan."""
    with connect() as conn:
        try:
            df = pd.read_sql_query(
                "SELECT kode_rs, nama_rs, kota, provinsi, tipe_rs, kelas_rs FROM rumah_sakit ORDER BY nama_rs",
                conn
            )
        except Exception:
            df = pd.DataFrame(columns=["kode_rs", "nama_rs", "kota", "provinsi", "tipe_rs", "kelas_rs"])
    df["kode_rs"] = df["kode_rs"].astype(str).str.strip()
    df["nama_rs"] = df["nama_rs"].astype(str).str.strip()
    df["kota"] = df["kota"].astype(str).str.strip()
    df["provinsi"] = df["provinsi"].astype(str).str.strip()

    # label tampilan: "KODE — NAMA RS (Kota, Provinsi)"
    df["label"] = df.apply(
        lambda r: f"{r['kode_rs']} — {r['nama_rs']}" +
                  (f" ({r['kota']}, {r['provinsi']})" if r["kota"] or r["provinsi"] else ""),
        axis=1
    )
    label_to_kode = {row["label"]: row["kode_rs"] for _, row in df.iterrows()}
    kode_to_label = {row["kode_rs"]: row["label"] for _, row in df.iterrows()}
    return df, label_to_kode, kode_to_label

def insert_row(kode_organisasi: str, kode_rs: str, layanan: str, catatan: str):
    with connect() as conn:
        conn.execute(
            f"INSERT INTO {TABLE} (kode_organisasi, kode_rs, d_at, layanan, catatan) VALUES (?, ?, ?, ?, ?)",
            [kode_organisasi, kode_rs, datetime.utcnow().isoformat(), layanan, catatan]
        )
        conn.commit()

def read_with_join(limit=500):
    """Join untuk tampilan kaya informasi."""
    with connect() as conn:
        return pd.read_sql_query(
            f"""
            SELECT
              t.id, t.d_at, t.layanan, t.catatan,
              t.kode_organisasi, io.hmhi_cabang, io.kota_cakupan_cabang,
              t.kode_rs, rs.nama_rs, rs.kota, rs.provinsi, rs.tipe_rs, rs.kelas_rs, rs.kontak
            FROM {TABLE} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            LEFT JOIN rumah_sakit rs ON rs.kode_rs = t.kode_rs
            ORDER BY t.id DESC
            LIMIT ?
            """,
            conn, params=[limit]
        )

# ======================== Startup ========================
ensure_identitas_schema()
ensure_rumah_sakit_schema()
init_rs_penangan_schema()

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input (Editor Tabel)", "📄 Data & Excel"])

# ---------- TAB INPUT ----------
with tab_input:
    st.caption("Pilih HMHI cabang, lalu isi beberapa baris RS (berdasarkan **Kode RS** dari master).")
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    df_rs, label_to_kode, kode_to_label = load_rs_master()

    if not hmhi_list:
        st.warning("Belum ada data **identitas_organisasi**. Isi dulu HMHI cabang & kode_organisasi.")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox("Pilih HMHI Cabang (Provinsi)", options=hmhi_list, key="rs::hmhi")

    if df_rs.empty:
        st.error("Master **rumah_sakit** kosong. Tambahkan data RS (kode_rs, nama_rs, dst.) terlebih dahulu.")
        st.stop()

    # Editor: pilih via label (agar user bisa melihat nama & lokasi), tapi yang disimpan adalah KODE RS.
    default_rows = 5
    df_default = pd.DataFrame({
        "rs_label": [""] * default_rows,  # tampil di UI
        "layanan": [""] * default_rows,
        "catatan": [""] * default_rows,
    })

    option_labels = [""] + sorted(label_to_kode.keys())

    with st.form("rs::form_editor"):
        edited = st.data_editor(
            df_default,
            key="rs::editor",
            column_config={
                "rs_label": st.column_config.SelectboxColumn(
                    "Rumah Sakit (Kode — Nama RS (Kota, Provinsi))",
                    options=option_labels, required=False
                ),
                "layanan": st.column_config.TextColumn("Layanan", help="Teks bebas, contoh: 'Klinik hemofilia; IGD 24 jam'"),
                "catatan": st.column_config.TextColumn("Catatan", help="Opsional"),
            },
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
        )

        # Pratinjau hasil simpan (pecah label → kode_rs & nama rs)
        if not edited.empty:
            preview = []
            for _, r in edited.iterrows():
                lbl = str(r.get("rs_label") or "").strip()
                kode_rs = label_to_kode.get(lbl, "")
                nama_rs = ""
                if kode_rs:
                    row = df_rs[df_rs["kode_rs"].str.casefold() == kode_rs.casefold()]
                    if not row.empty:
                        nama_rs = row.iloc[0]["nama_rs"]
                preview.append({
                    "Kode RS": kode_rs,
                    "Nama RS": nama_rs,
                    "Layanan": r.get("layanan", ""),
                    "Catatan": r.get("catatan", ""),
                })
            st.caption("Pratinjau baris yang akan disimpan:")
            st.dataframe(pd.DataFrame(preview), use_container_width=True)

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
                    lbl = str(r.get("rs_label") or "").strip()
                    if not lbl:
                        continue
                    kode_rs = label_to_kode.get(lbl)
                    if not kode_rs:
                        st.warning(f"Lewati baris tanpa Kode RS valid: '{lbl}'")
                        continue
                    layanan = str(r.get("layanan") or "").strip()
                    catatan = str(r.get("catatan") or "").strip()
                    insert_row(kode_organisasi, kode_rs, layanan, catatan)
                    n_saved += 1
                if n_saved:
                    st.success(f"{n_saved} baris tersimpan untuk **{selected_hmhi}**.")
                else:
                    st.info("Tidak ada baris valid untuk disimpan.")

# ---------- TAB DATA ----------
with tab_data:
    st.subheader("📄 Data Tersimpan")
    df = read_with_join(limit=500)

    if df.empty:
        st.info("Belum ada data.")
    else:
        view = df.rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "d_at": "Waktu Input (UTC)",
            "kode_rs": "Kode RS",
            "nama_rs": "Nama RS",
            "kota": "Kota",
            "provinsi": "Provinsi",
            "tipe_rs": "Tipe RS",
            "kelas_rs": "Kelas RS",
            "kontak": "Kontak",
            "layanan": "Layanan",
            "catatan": "Catatan",
        })
        order = [
            "HMHI cabang", "Kota/Provinsi Cakupan Cabang", "Waktu Input (UTC)",
            "Kode RS", "Nama RS", "Kota", "Provinsi", "Tipe RS", "Kelas RS", "Kontak",
            "Layanan", "Catatan",
        ]
        order = [c for c in order if c in view.columns]
        st.dataframe(view[order], use_container_width=True)

        # Unduh Excel (data tersimpan)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view[order].to_excel(w, index=False, sheet_name="RS_Penangan_Hemofilia")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buf.getvalue(),
            file_name="rs_penangan_hemofilia.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="rs::download"
        )

    st.divider()
    st.markdown("### 📥 Template & Unggah Excel")

    # Template kosong untuk unggah
    tmpl_df = pd.DataFrame([{
        "HMHI cabang": "",
        "Kode RS": (df_rs.iloc[0]["kode_rs"] if not df_rs.empty else ""),
        "Layanan": "",
        "Catatan": "",
    }], columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📄 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_rs_penangan_hemofilia.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="rs::dl_template"
    )

    # Unggah
    up = st.file_uploader("Unggah file Excel (.xlsx) sesuai template", type=["xlsx"], key="rs::uploader")

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
                    raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                kode_rs = str((s.get("kode_rs") or "")).strip()
                if not kode_rs:
                    raise ValueError("Kolom 'Kode RS' kosong.")
                # validasi kode_rs ada di master
                if df_rs[df_rs["kode_rs"].str.casefold() == kode_rs.casefold()].empty:
                    raise ValueError(f"Kode RS '{kode_rs}' tidak ada di master rumah_sakit.")

                layanan = str((s.get("layanan") or "")).strip()
                catatan = str((s.get("catatan") or "")).strip()

                insert_row(kode_organisasi, kode_rs, layanan, catatan)
                results.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {kode_rs}"})
                n_ok += 1
            except Exception as e:
                results.append({"Baris Excel": i + 2, "Status": "GAGAL", "Keterangan": str(e)})

        return pd.DataFrame(results), n_ok

    if up is not None:
        try:
            raw = pd.read_excel(up)
            raw.columns = [str(c).strip() for c in raw.columns]
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        # Validasi header minimal
        missing = [c for c in ["HMHI cabang", "Kode RS"] if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Minimal harus ada: " + ", ".join(missing))
            st.stop()

        # Tambahkan kolom opsional agar aman
        for c in TEMPLATE_COLUMNS:
            if c not in raw.columns:
                raw[c] = ""

        st.caption("Pratinjau 20 baris pertama:")
        st.dataframe(raw[TEMPLATE_COLUMNS].head(20), use_container_width=True)

        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        if st.button("🚀 Proses & Simpan Unggahan", type="primary", key="rs::process"):
            log_df, n_ok = process_upload(df_up)
            st.write("**Hasil unggah:**")
            st.dataframe(log_df, use_container_width=True)

            ok = (log_df["Status"] == "OK").sum()
            fail = (log_df["Status"] == "GAGAL").sum()
            if ok:
                st.success(f"Berhasil menyimpan {ok} baris.")
            if fail:
                st.error(f"Gagal menyimpan {fail} baris.")

            # Unduh log hasil
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
