import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

st.set_page_config(page_title="Rumah Sakit Penangan Hemofilia", page_icon="🏥", layout="wide")
st.title("🏥 Rumah Sakit yang Menangani Hemofilia")

DB_PATH = "hemofilia.db"
TABLE = "rs_penangan_hemofilia"

TIPE_RS_OPTIONS = [
    "RSU Tipe A", "RSU Tipe B", "RSU Tipe C", "RSU Tipe D",
    "RS Swasta Tipe A", "RS Swasta Tipe B", "RS Swasta Tipe C", "RS Swasta Tipe D",
]
YA_TIDAK_OPTIONS = ["Ya", "Tidak"]

# ===== Template unggah =====
TEMPLATE_COLUMNS = [
    "HMHI cabang",                      # wajib → dipetakan ke kode_organisasi
    "Nama Rumah Sakit",                 # wajib
    "Tipe RS",                          # opsional, harus salah satu dari TIPE_RS_OPTIONS
    "Terdapat Dokter Hematologi",       # opsional, Ya/Tidak
    "Terdapat Tim Terpadu Hemofilia",   # opsional, Ya/Tidak
    "Lokasi",                           # opsional (bila kosong → fallback master rumah_sakit)
    "Propinsi",                         # opsional (bila kosong → fallback master rumah_sakit)
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Nama Rumah Sakit": "nama_rumah_sakit",
    "Tipe RS": "tipe_rs",
    "Terdapat Dokter Hematologi": "dokter_hematologi",
    "Terdapat Tim Terpadu Hemofilia": "tim_terpadu",
    "Lokasi": "lokasi",
    "Propinsi": "propinsi",
}

# ======================== Util DB ========================
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

def ensure_rumah_sakit_schema():
    """
    Pastikan tabel `rumah_sakit` tersedia dan memiliki kolom:
      - Nama (TEXT, UNIQUE)
      - Lokasi (TEXT)
      - Propinsi (TEXT)
    """
    with connect() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS rumah_sakit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                Nama TEXT UNIQUE,
                Lokasi TEXT,
                Propinsi TEXT
            )
        """)
        cur.execute("PRAGMA table_info(rumah_sakit)")
        cols = [r[1] for r in cur.fetchall()]
        if "Lokasi" not in cols:
            cur.execute("ALTER TABLE rumah_sakit ADD COLUMN Lokasi TEXT")
        if "Propinsi" not in cols:
            cur.execute("ALTER TABLE rumah_sakit ADD COLUMN Propinsi TEXT")
        conn.commit()

def migrate_if_needed():
    """Migrasi tabel rs_penangan_hemofilia agar memiliki kolom lokasi & propinsi selain kolom wajib lain."""
    with connect() as conn:
        if not _table_exists(conn, TABLE):
            return

        required_cols = ["kode_organisasi", "nama_rumah_sakit", "tipe_rs",
                         "dokter_hematologi", "tim_terpadu", "lokasi", "propinsi"]
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({TABLE})")
        have = [r[1] for r in cur.fetchall()]
        if all(col in have for col in required_cols):
            return

        st.warning("Migrasi skema: menambahkan kolom lokasi & propinsi pada rs_penangan_hemofilia…")
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT,
                    created_at TEXT NOT NULL,
                    nama_rumah_sakit TEXT,
                    tipe_rs TEXT,
                    dokter_hematologi TEXT,
                    tim_terpadu TEXT,
                    lokasi TEXT,
                    propinsi TEXT,
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
            cur.execute(f"PRAGMA table_info({TABLE})")
            old_cols = [r[1] for r in cur.fetchall() if r[1] != "id"]
            if old_cols:
                cols_csv = ", ".join(old_cols)
                cur.execute(f"INSERT INTO {TABLE}_new ({cols_csv}) SELECT {cols_csv} FROM {TABLE}")
            cur.execute(f"ALTER TABLE {TABLE} RENAME TO {TABLE}_backup")
            cur.execute(f"ALTER TABLE {TABLE}_new RENAME TO {TABLE}")
            conn.commit()
            st.success("Migrasi selesai. Tabel lama disimpan sebagai _backup.")
        except Exception as e:
            conn.rollback()
            st.error(f"Migrasi gagal: {e}")
        finally:
            cur.execute("PRAGMA foreign_keys=ON")

def init_db():
    with connect() as conn:
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT,
                created_at TEXT NOT NULL,
                nama_rumah_sakit TEXT,
                tipe_rs TEXT,
                dokter_hematologi TEXT,
                tim_terpadu TEXT,
                lokasi TEXT,
                propinsi TEXT,
                FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
            )
        """)
        conn.commit()

# ======================== Helpers ========================
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

def load_rs_master():
    with connect() as conn:
        try:
            df = pd.read_sql_query("SELECT Nama, Lokasi, Propinsi FROM rumah_sakit ORDER BY Nama", conn)
            return df
        except Exception:
            return pd.DataFrame(columns=["Nama", "Lokasi", "Propinsi"])

def insert_row(payload: dict, kode_organisasi: str):
    with connect() as conn:
        c = conn.cursor()
        cols = ", ".join(payload.keys())
        placeholders = ", ".join(["?"] * len(payload))
        vals = [payload[k] for k in payload]
        c.execute(
            f"INSERT INTO {TABLE} (kode_organisasi, created_at, {cols}) VALUES (?, ?, {placeholders})",
            [kode_organisasi, datetime.utcnow().isoformat()] + vals
        )
        conn.commit()

def read_with_kota(limit=500):
    """
    Gabungkan:
      - kota_cakupan_cabang dari identitas_organisasi
      - Lokasi & Propinsi dari t.lokasi/propinsi; fallback ke master rumah_sakit bila kosong.
    """
    with connect() as conn:
        if not _has_column(conn, TABLE, "kode_organisasi"):
            st.error("Kolom 'kode_organisasi' belum tersedia. Coba refresh setelah migrasi.")
            return pd.read_sql_query(f"SELECT * FROM {TABLE} ORDER BY id DESC LIMIT ?", conn, params=[limit])

        query = f"""
            SELECT
              t.id, t.kode_organisasi, t.created_at,
              t.nama_rumah_sakit, t.tipe_rs, t.dokter_hematologi, t.tim_terpadu,
              io.hmhi_cabang, io.kota_cakupan_cabang,
              COALESCE(NULLIF(t.lokasi, ''), rs.Lokasi)     AS lokasi_final,
              COALESCE(NULLIF(t.propinsi, ''), rs.Propinsi) AS propinsi_final
            FROM {TABLE} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            LEFT JOIN rumah_sakit rs ON rs.Nama = t.nama_rumah_sakit
            ORDER BY t.id DESC
            LIMIT ?
        """
        return pd.read_sql_query(query, conn, params=[limit])

# ======================== Startup ========================
ensure_rumah_sakit_schema()
migrate_if_needed()
init_db()

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

# ---------- TAB INPUT ----------
with tab_input:
    # Sumber pilihan: HMHI cabang → kode_organisasi
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="rs::hmhi_select"
        )

    # Master RS → untuk bantu memilih "Nama - Lokasi - Propinsi"
    df_rs = load_rs_master()
    if df_rs.empty:
        st.error("Tidak bisa memuat daftar Rumah Sakit dari tabel 'rumah_sakit'. Pastikan kolom 'Nama', 'Lokasi', 'Propinsi' ada.")
    else:
        # Buat label: "Nama - Lokasi - Propinsi" -> parts
        def to_str(x): return "" if pd.isna(x) else str(x)
        label_to_parts = {}
        for _, r in df_rs.iterrows():
            nama = to_str(r["Nama"]).strip()
            lokasi = to_str(r["Lokasi"]).strip()
            prop  = to_str(r["Propinsi"]).strip()
            if not nama:
                continue
            label = f"{nama} - {lokasi or '-'} - {prop or '-'}"
            label_to_parts[label] = {"nama": nama, "lokasi": lokasi, "propinsi": prop}

        option_labels = [""] + sorted(label_to_parts.keys())

        # Template awal 5 baris
        df_default = pd.DataFrame({
            "nama_rumah_sakit": ["", "", "", "", ""],  # akan berisi LABEL gabungan
            "tipe_rs": ["", "", "", "", ""],
            "dokter_hematologi": ["", "", "", "", ""],
            "tim_terpadu": ["", "", "", "", ""],
        })

        with st.form("rs::form"):
            edited = st.data_editor(
                df_default,
                key="rs::editor",
                column_config={
                    # opsi berlabel gabungan
                    "nama_rumah_sakit": st.column_config.SelectboxColumn(
                        "Nama Rumah Sakit (Nama - Lokasi - Propinsi)",
                        options=option_labels,
                        required=False
                    ),
                    "tipe_rs": st.column_config.SelectboxColumn(
                        "Tipe RS", options=[""] + TIPE_RS_OPTIONS, required=False
                    ),
                    "dokter_hematologi": st.column_config.SelectboxColumn(
                        "Terdapat Dokter Hematologi", options=[""] + YA_TIDAK_OPTIONS, required=False
                    ),
                    "tim_terpadu": st.column_config.SelectboxColumn(
                        "Terdapat Tim Terpadu Hemofilia", options=[""] + YA_TIDAK_OPTIONS, required=False
                    ),
                },
                use_container_width=True,
                num_rows="dynamic",
                hide_index=True,
            )

            # Pratinjau tabel (pecah label -> Nama, Lokasi, Propinsi)
            if not edited.empty:
                preview = []
                for _, row in edited.iterrows():
                    label = (row.get("nama_rumah_sakit") or "").strip()
                    parts = label_to_parts.get(label, {"nama": "", "lokasi": "", "propinsi": ""})
                    preview.append({
                        "Nama Rumah Sakit": parts["nama"],
                        "Lokasi": parts["lokasi"],
                        "Propinsi": parts["propinsi"],
                        "Tipe RS": row.get("tipe_rs", ""),
                        "Terdapat Dokter Hematologi": row.get("dokter_hematologi", ""),
                        "Terdapat Tim Terpadu Hemofilia": row.get("tim_terpadu", ""),
                    })
                st.caption("Pratinjau baris yang akan disimpan (label: Nama - Lokasi - Propinsi):")
                st.dataframe(pd.DataFrame(preview), use_container_width=True)

            submitted = st.form_submit_button("💾 Simpan")

        st.info(
            "Tim terpadu hemofilia adalah tim medis dan paramedis dari berbagai disiplin ilmu yang "
            "terintegrasi di rumah sakit dan memiliki SK penugasan dari rumah sakit terkait"
        )

        if submitted:
            if not selected_hmhi:
                st.error("Pilih HMHI cabang terlebih dahulu.")
            else:
                kode_organisasi = hmhi_map.get(selected_hmhi)
                if not kode_organisasi:
                    st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
                else:
                    n_saved = 0
                    for _, row in edited.iterrows():
                        label = (row.get("nama_rumah_sakit") or "").strip()
                        if not label:
                            continue  # baris kosong
                        parts = label_to_parts.get(label, {"nama": "", "lokasi": "", "propinsi": ""})
                        if not parts["nama"]:
                            continue

                        tipe = str(row.get("tipe_rs") or "").strip()
                        dok  = str(row.get("dokter_hematologi") or "").strip()
                        tim  = str(row.get("tim_terpadu") or "").strip()

                        payload = {
                            "nama_rumah_sakit": parts["nama"],  # simpan nama murni
                            "tipe_rs": tipe if tipe in TIPE_RS_OPTIONS else "",
                            "dokter_hematologi": dok if dok in YA_TIDAK_OPTIONS else "",
                            "tim_terpadu": tim if tim in YA_TIDAK_OPTIONS else "",
                            "lokasi": parts["lokasi"],          # simpan juga lokasi
                            "propinsi": parts["propinsi"],      # simpan juga propinsi
                        }
                        insert_row(payload, kode_organisasi)
                        n_saved += 1

                    if n_saved > 0:
                        st.success(f"{n_saved} baris tersimpan untuk **{selected_hmhi}**.")
                    else:
                        st.info("Tidak ada baris valid untuk disimpan.")

# ---------- TAB DATA (Tersimpan + Unggah Excel) ----------
with tab_data:
    st.subheader("📄 Data Tersimpan")
    df = read_with_kota(limit=500)

    if df.empty:
        st.info("Belum ada data.")
    else:
        cols_order = [
            "hmhi_cabang", "kota_cakupan_cabang", "created_at",
            "nama_rumah_sakit", "lokasi_final", "propinsi_final",
            "tipe_rs", "dokter_hematologi", "tim_terpadu"
        ]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "nama_rumah_sakit": "Nama Rumah Sakit",
            "lokasi_final": "Lokasi",
            "propinsi_final": "Propinsi",
            "tipe_rs": "Tipe RS",
            "dokter_hematologi": "Terdapat Dokter Hematologi",
            "tim_terpadu": "Terdapat Tim Terpadu Hemofilia",
        })

        st.dataframe(view, use_container_width=True)

        # Unduh Excel dari data tersimpan
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="RS_Penangan_Hemofilia")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buffer.getvalue(),
            file_name="rs_penangan_hemofilia.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="rs::download"
        )

    st.divider()
    st.markdown("### 📥 Template & Unggah Excel")

    # ——— Unduh Template Kosong
    tmpl_df = pd.DataFrame([{
        "HMHI cabang": "",
        "Nama Rumah Sakit": "RS Contoh",
        "Tipe RS": "",
        "Terdapat Dokter Hematologi": "",
        "Terdapat Tim Terpadu Hemofilia": "",
        "Lokasi": "",
        "Propinsi": "",
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

    # ——— Unggah File
    up = st.file_uploader(
        "Unggah file Excel (.xlsx) sesuai template",
        type=["xlsx"],
        key="rs::uploader"
    )

    def process_upload(df_up: pd.DataFrame):
        """Validasi & simpan unggahan. Kembalikan (log_df, n_ok)."""
        hmhi_map, _ = load_hmhi_to_kode()
        rs_master = load_rs_master()
        rs_master["Nama_str"] = rs_master["Nama"].astype(str).str.strip()

        results = []
        n_ok = 0

        for i in range(len(df_up)):
            try:
                s = df_up.iloc[i]  # Series → punya .get
                hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                if not hmhi:
                    raise ValueError("Kolom 'HMHI cabang' kosong.")
                kode_organisasi = hmhi_map.get(hmhi)
                if not kode_organisasi:
                    raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                nama_rs = str((s.get("nama_rumah_sakit") or "")).strip()
                if not nama_rs:
                    raise ValueError("Kolom 'Nama Rumah Sakit' kosong.")

                tipe = str((s.get("tipe_rs") or "")).strip()
                if tipe and tipe not in TIPE_RS_OPTIONS:
                    raise ValueError(f"Tipe RS tidak valid: '{tipe}'")

                dok = str((s.get("dokter_hematologi") or "")).strip()
                if dok and dok not in YA_TIDAK_OPTIONS:
                    raise ValueError("Kolom 'Terdapat Dokter Hematologi' harus 'Ya' atau 'Tidak'")

                tim = str((s.get("tim_terpadu") or "")).strip()
                if tim and tim not in YA_TIDAK_OPTIONS:
                    raise ValueError("Kolom 'Terdapat Tim Terpadu Hemofilia' harus 'Ya' atau 'Tidak'")

                lokasi = str((s.get("lokasi") or "")).strip()
                prop   = str((s.get("propinsi") or "")).strip()

                # Fallback lokasi/propinsi dari master bila kosong
                if not lokasi or not prop:
                    m = rs_master[rs_master["Nama_str"].str.casefold() == nama_rs.casefold()]
                    if not m.empty:
                        if not lokasi:
                            lokasi = str(m.iloc[0]["Lokasi"] or "").strip()
                        if not prop:
                            prop = str(m.iloc[0]["Propinsi"] or "").strip()

                payload = {
                    "nama_rumah_sakit": nama_rs,
                    "tipe_rs": tipe,
                    "dokter_hematologi": dok,
                    "tim_terpadu": tim,
                    "lokasi": lokasi,
                    "propinsi": prop,
                }
                insert_row(payload, kode_organisasi)
                results.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {nama_rs}"})
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
        missing = [c for c in ["HMHI cabang", "Nama Rumah Sakit"] if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Minimal harus ada: " + ", ".join(missing))
            st.stop()

        # Tambahkan kolom opsional yang belum ada agar aman
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
            st.success(f"Berhasil menyimpan {ok} baris.") if ok else None
            st.error(f"Gagal menyimpan {fail} baris.") if fail else None

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
