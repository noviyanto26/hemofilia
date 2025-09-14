import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

st.set_page_config(page_title="Pasien Nonfaktor", page_icon="🩸", layout="wide")
st.title("🩸 Pasien Pengguna Nonfaktor — Dengan & Tanpa Inhibitor")

DB_PATH = "hemofilia.db"
TABLE_INHIB = "pasien_nonfaktor_inhibitor"
TABLE_TANPA = "pasien_nonfaktor_tanpa_inhibitor"

# ===== Template unggah & alias =====
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Jenis Penanganan",
    "Ketersediaan",        # "", "Tersedia", "Tidak tersedia"
    "Jumlah Pengguna",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Jenis Penanganan": "jenis_penanganan",
    "Ketersediaan": "ketersediaan",
    "Jumlah Pengguna": "jumlah_pengguna",
}

# ======================== Util DB umum ========================
def connect():
    return sqlite3.connect(DB_PATH)

def _has_column(conn, table, col):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any((row[1] == col) for row in cur.fetchall())

def _table_exists(conn, table):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def migrate_if_needed(table_name: str):
    """Pastikan skema final (termasuk kode_organisasi & kolom form) tersedia."""
    with connect() as conn:
        if not _table_exists(conn, table_name):
            return
        needed = ["kode_organisasi", "jenis_penanganan", "ketersediaan", "jumlah_pengguna"]
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({table_name})")
        have = [r[1] for r in cur.fetchall()]
        if all(col in have for col in needed):
            return

        st.warning(f"Migrasi skema: menyesuaikan tabel {table_name} …")
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT,
                    created_at TEXT NOT NULL,
                    jenis_penanganan TEXT,
                    ketersediaan TEXT,
                    jumlah_pengguna INTEGER,
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
            # Salin kolom yang ada saja (jika tabel lama ada)
            cur.execute(f"PRAGMA table_info({table_name})")
            old_cols = [r[1] for r in cur.fetchall() if r[1] != "id"]
            if old_cols:
                cols_csv = ", ".join(old_cols)
                cur.execute(f"INSERT INTO {table_name}_new ({cols_csv}) SELECT {cols_csv} FROM {table_name}")
            cur.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_backup")
            cur.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
            conn.commit()
            st.success(f"Migrasi {table_name} selesai. Tabel lama disimpan sebagai _backup.")
        except Exception as e:
            conn.rollback()
            st.error(f"Migrasi {table_name} gagal: {e}")
        finally:
            cur.execute("PRAGMA foreign_keys=ON")

def init_db(table_name: str):
    with connect() as conn:
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT,
                created_at TEXT NOT NULL,
                jenis_penanganan TEXT,
                ketersediaan TEXT,
                jumlah_pengguna INTEGER,
                FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
            )
        """)
        conn.commit()

# ======================== Helpers umum ========================
def load_hmhi_to_kode():
    """
    Ambil pilihan dari identitas_organisasi.hmhi_cabang dan petakan ke kode_organisasi.
    Return:
        mapping: dict {hmhi_cabang -> kode_organisasi}
        options: list hmhi_cabang (urut alfabet)
    """
    with connect() as conn:
        try:
            df = pd.read_sql_query(
                "SELECT kode_organisasi, hmhi_cabang FROM identitas_organisasi ORDER BY id DESC",
                conn
            )
            if df.empty:
                return {}, []
            mapping = {}
            for _, row in df.iterrows():
                hmhi_val = (str(row["hmhi_cabang"]).strip() if pd.notna(row["hmhi_cabang"]) else "")
                kode_val = (str(row["kode_organisasi"]).strip() if pd.notna(row["kode_organisasi"]) else "")
                if hmhi_val:
                    mapping[hmhi_val] = kode_val
            options = sorted(mapping.keys())
            return mapping, options
        except Exception:
            return {}, []

def safe_int(val):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return 0
        return int(x)
    except Exception:
        return 0

def insert_row(table_name: str, payload: dict, kode_organisasi: str):
    with connect() as conn:
        c = conn.cursor()
        cols = ", ".join(payload.keys())
        placeholders = ", ".join(["?"] * len(payload))
        vals = [payload[k] for k in payload]
        c.execute(
            f"INSERT INTO {table_name} (kode_organisasi, created_at, {cols}) VALUES (?, ?, {placeholders})",
            [kode_organisasi, datetime.utcnow().isoformat()] + vals
        )
        conn.commit()

def read_with_kota(table_name: str, limit=500):
    with connect() as conn:
        if not _has_column(conn, table_name, "kode_organisasi"):
            st.error(f"Kolom 'kode_organisasi' belum tersedia di {table_name}. Coba refresh setelah migrasi.")
            return pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT ?", conn, params=[limit])

        return pd.read_sql_query(
            f"""
            SELECT
              t.id, t.kode_organisasi, t.created_at,
              t.jenis_penanganan, t.ketersediaan, t.jumlah_pengguna,
              io.hmhi_cabang, io.kota_cakupan_cabang
            FROM {table_name} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            ORDER BY t.id DESC
            LIMIT ?
            """, conn, params=[limit]
        )

# ======================== Startup ========================
for t in (TABLE_INHIB, TABLE_TANPA):
    migrate_if_needed(t)
    init_db(t)

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # -- Sumber pilihan dari HMHI cabang → mapped ke kode_organisasi
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="nf::hmhi_select"
        )

    # --- Form 1: Dengan Inhibitor ---
    st.subheader("1) Pasien Pengguna Nonfaktor Dengan Inhibitor")
    df_inhib_default = pd.DataFrame({
        "jenis_penanganan": ["", "", "", "", ""],
        "ketersediaan": ["", "", "", "", ""],
        "jumlah_pengguna": [0, 0, 0, 0, 0],
    })
    with st.form("nf::form_inhib"):
        ed_inhib = st.data_editor(
            df_inhib_default,
            key="nf::editor_inhib",
            column_config={
                "jenis_penanganan": st.column_config.TextColumn("Jenis Penanganan", help="Contoh: Emicizumab profilaksis, rFVIIa, aPCC, dsb."),
                "ketersediaan": st.column_config.SelectboxColumn(
                    "Ketersediaan", options=["", "Tersedia", "Tidak tersedia"], required=False
                ),
                "jumlah_pengguna": st.column_config.NumberColumn("Jumlah Pengguna", min_value=0, step=1),
            },
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
        )
        sub_inhib = st.form_submit_button("💾 Simpan (Dengan Inhibitor)")

    if sub_inhib:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                n_saved = 0
                for _, row in ed_inhib.iterrows():
                    jenis = str(row.get("jenis_penanganan") or "").strip()
                    ket = str(row.get("ketersediaan") or "").strip()
                    jml = safe_int(row.get("jumlah_pengguna", 0))
                    if not jenis and jml <= 0 and not ket:
                        continue
                    payload = {
                        "jenis_penanganan": jenis,
                        "ketersediaan": ket,
                        "jumlah_pengguna": jml,
                    }
                    insert_row(TABLE_INHIB, payload, kode_organisasi)
                    n_saved += 1
                if n_saved > 0:
                    st.success(f"{n_saved} baris tersimpan (Dengan Inhibitor) untuk **{selected_hmhi}**.")
                else:
                    st.info("Tidak ada baris valid untuk disimpan (Dengan Inhibitor).")

    st.divider()

    # --- Form 2: Tanpa Inhibitor ---
    st.subheader("2) Pasien Pengguna Nonfaktor Tanpa Inhibitor")
    df_tanpa_default = pd.DataFrame({
        "jenis_penanganan": ["", "", "", "", ""],
        "ketersediaan": ["", "", "", "", ""],
        "jumlah_pengguna": [0, 0, 0, 0, 0],
    })
    with st.form("nf::form_tanpa"):
        ed_tanpa = st.data_editor(
            df_tanpa_default,
            key="nf::editor_tanpa",
            column_config={
                "jenis_penanganan": st.column_config.TextColumn("Jenis Penanganan", help="Contoh: Emicizumab profilaksis, rFVIIa, aPCC, dsb."),
                "ketersediaan": st.column_config.SelectboxColumn(
                    "Ketersediaan", options=["", "Tersedia", "Tidak tersedia"], required=False
                ),
                "jumlah_pengguna": st.column_config.NumberColumn("Jumlah Pengguna", min_value=0, step=1),
            },
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
        )
        sub_tanpa = st.form_submit_button("💾 Simpan (Tanpa Inhibitor)")

    if sub_tanpa:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                n_saved = 0
                for _, row in ed_tanpa.iterrows():
                    jenis = str(row.get("jenis_penanganan") or "").strip()
                    ket = str(row.get("ketersediaan") or "").strip()
                    jml = safe_int(row.get("jumlah_pengguna", 0))
                    if not jenis and jml <= 0 and not ket:
                        continue
                    payload = {
                        "jenis_penanganan": jenis,
                        "ketersediaan": ket,
                        "jumlah_pengguna": jml,
                    }
                    insert_row(TABLE_TANPA, payload, kode_organisasi)
                    n_saved += 1
                if n_saved > 0:
                    st.success(f"{n_saved} baris tersimpan (Tanpa Inhibitor) untuk **{selected_hmhi}**.")
                else:
                    st.info("Tidak ada baris valid untuk disimpan (Tanpa Inhibitor).")

# ======================== Data Tersimpan & Unggah Excel ========================
with tab_data:
    # ---------- Template Excel ----------
    st.caption("Gunakan template berikut saat mengunggah data (kolom harus persis sama):")
    tmpl_rows = [
        {"HMHI cabang": "", "Jenis Penanganan": "Emicizumab profilaksis", "Ketersediaan": "Tersedia", "Jumlah Pengguna": 0},
        {"HMHI cabang": "", "Jenis Penanganan": "rFVIIa", "Ketersediaan": "", "Jumlah Pengguna": 0},
    ]
    tmpl_df = pd.DataFrame(tmpl_rows, columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_pasien_nonfaktor.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="nf::dl_template"
    )

    # ---------- Data Tersimpan: Dengan Inhibitor ----------
    st.subheader("📄 Data Tersimpan — Dengan Inhibitor")
    df_inhib = read_with_kota(TABLE_INHIB, limit=500)
    if df_inhib.empty:
        st.info("Belum ada data (Dengan Inhibitor).")
    else:
        cols_order = ["hmhi_cabang", "kota_cakupan_cabang", "created_at", "jenis_penanganan", "ketersediaan", "jumlah_pengguna"]
        cols_order = [c for c in cols_order if c in df_inhib.columns]
        view_inhib = df_inhib[cols_order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "jenis_penanganan": "Jenis Penanganan",
            "ketersediaan": "Ketersediaan",
            "jumlah_pengguna": "Jumlah Pengguna",
        })
        st.dataframe(view_inhib, use_container_width=True)

        buf1 = io.BytesIO()
        with pd.ExcelWriter(buf1, engine="xlsxwriter") as w:
            view_inhib.to_excel(w, index=False, sheet_name="Nonfaktor_Inhibitor")
        st.download_button(
            "⬇️ Unduh Excel (Dengan Inhibitor)",
            buf1.getvalue(),
            file_name="pasien_nonfaktor_dengan_inhibitor.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="nf::download_inhib"
        )

    st.divider()

    # ---------- Data Tersimpan: Tanpa Inhibitor ----------
    st.subheader("📄 Data Tersimpan — Tanpa Inhibitor")
    df_tanpa = read_with_kota(TABLE_TANPA, limit=500)
    if df_tanpa.empty:
        st.info("Belum ada data (Tanpa Inhibitor).")
    else:
        cols_order = ["hmhi_cabang", "kota_cakupan_cabang", "created_at", "jenis_penanganan", "ketersediaan", "jumlah_pengguna"]
        cols_order = [c for c in cols_order if c in df_tanpa.columns]
        view_tanpa = df_tanpa[cols_order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "jenis_penanganan": "Jenis Penanganan",
            "ketersediaan": "Ketersediaan",
            "jumlah_pengguna": "Jumlah Pengguna",
        })
        st.dataframe(view_tanpa, use_container_width=True)

        buf2 = io.BytesIO()
        with pd.ExcelWriter(buf2, engine="xlsxwriter") as w:
            view_tanpa.to_excel(w, index=False, sheet_name="Nonfaktor_TanpaInhibitor")
        st.download_button(
            "⬇️ Unduh Excel (Tanpa Inhibitor)",
            buf2.getvalue(),
            file_name="pasien_nonfaktor_tanpa_inhibitor.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="nf::download_tanpa"
        )

    st.divider()
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan header persis seperti template",
        type=["xlsx"],
        key="nf::uploader"
    )

    def process_upload(df_up: pd.DataFrame, target_table: str):
        """Proses & simpan DataFrame unggah ke tabel target."""
        hmhi_map, _ = load_hmhi_to_kode()
        results = []
        n_ok = 0

        for i in range(len(df_up)):
            try:
                s = df_up.iloc[i]  # Series, agar .get tersedia
                hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                if not hmhi:
                    raise ValueError("Kolom 'HMHI cabang' kosong.")
                kode_organisasi = hmhi_map.get(hmhi)
                if not kode_organisasi:
                    raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                jenis = str((s.get("jenis_penanganan") or "")).strip()
                ket = str((s.get("ketersediaan") or "")).strip()
                jml = safe_int(s.get("jumlah_pengguna", 0))

                if not jenis and jml <= 0 and not ket:
                    # baris kosong → lewati tapi tidak error
                    results.append({"Baris Excel": i + 2, "Status": "LEWATI", "Keterangan": "Baris kosong / tidak ada data"})
                    continue

                payload = {
                    "jenis_penanganan": jenis,
                    "ketersediaan": ket,
                    "jumlah_pengguna": max(jml, 0),
                }
                insert_row(target_table, payload, kode_organisasi)
                results.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi}"})
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

        # Validasi header
        missing = [c for c in TEMPLATE_COLUMNS if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Kolom yang belum ada: " + ", ".join(missing))
            st.stop()

        # Pratinjau
        st.caption("Pratinjau 20 baris pertama dari file yang diunggah:")
        st.dataframe(raw.head(20), use_container_width=True)

        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        # Pilih target tabel lewat radio
        target = st.radio(
            "Simpan data ke tabel:",
            options=("Dengan Inhibitor", "Tanpa Inhibitor"),
            horizontal=True,
            key="nf::target_table"
        )

        if st.button("🚀 Proses & Simpan", type="primary", key="nf::process"):
            target_table = TABLE_INHIB if target == "Dengan Inhibitor" else TABLE_TANPA
            res_df, n_ok = process_upload(df_up, target_table)
            st.write("**Hasil unggah:**")
            st.dataframe(res_df, use_container_width=True)

            ok = (res_df["Status"] == "OK").sum()
            fail = (res_df["Status"] == "GAGAL").sum()
            skip = (res_df["Status"] == "LEWATI").sum()
            if ok:
                st.success(f"Berhasil menyimpan {ok} baris.")
            if skip:
                st.info(f"Dilewati {skip} baris kosong.")
            if fail:
                st.error(f"Gagal menyimpan {fail} baris.")

            # Unduh log hasil
            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                res_df.to_excel(w, index=False, sheet_name="Hasil")
            st.download_button(
                "📄 Unduh Log Hasil",
                log_buf.getvalue(),
                file_name=f"log_hasil_unggah_pasien_nonfaktor_{'inhib' if target_table==TABLE_INHIB else 'tanpa'}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="nf::dl_log"
            )
