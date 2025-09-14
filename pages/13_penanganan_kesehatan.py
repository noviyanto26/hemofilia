import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Penanganan Kesehatan", page_icon="🏥", layout="wide")
st.title("🏥 Penanganan Kesehatan")

DB_PATH = "hemofilia.db"
TABLE = "penanganan_kesehatan"

# ======================== Util DB umum ========================
def connect():
    return sqlite3.connect(DB_PATH)

def _table_exists(conn, table):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def _has_column(conn, table, col):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any((row[1] == col) for row in cur.fetchall())

def migrate_if_needed(table_name: str):
    """
    Pastikan skema final tersedia.
    Skema final:
      id, kode_organisasi, created_at,
      jenis_hemofilia, jenis_penanganan, layanan_rawat,
      dosis_per_orang_per_kedatangan, frekuensi
    """
    with connect() as conn:
        cur = conn.cursor()
        if not _table_exists(conn, table_name):
            return

        needed = [
            "kode_organisasi",
            "jenis_hemofilia",
            "jenis_penanganan",
            "layanan_rawat",
            "dosis_per_orang_per_kedatangan",
            "frekuensi",
        ]
        cur.execute(f"PRAGMA table_info({table_name})")
        have = [r[1] for r in cur.fetchall()]
        if all(col in have for col in needed):
            return  # sudah sesuai

        st.warning(f"Migrasi skema: menyesuaikan tabel {table_name} …")
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT,
                    created_at TEXT NOT NULL,
                    jenis_hemofilia TEXT,
                    jenis_penanganan TEXT,
                    layanan_rawat TEXT,
                    dosis_per_orang_per_kedatangan REAL,
                    frekuensi TEXT,
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
            # Salin kolom yang ada dari tabel lama
            cur.execute(f"PRAGMA table_info({table_name})")
            old_cols = [r[1] for r in cur.fetchall() if r[1] != "id"]
            if old_cols:
                cols_csv = ", ".join(old_cols)
                cur.execute(f"INSERT INTO {table_name}_new ({cols_csv}) SELECT {cols_csv} FROM {table_name}")
            # Ganti nama
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
                jenis_hemofilia TEXT,
                jenis_penanganan TEXT,
                layanan_rawat TEXT,
                dosis_per_orang_per_kedatangan REAL,
                frekuensi TEXT,
                FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
            )
        """)
        conn.commit()

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

def read_with_kota(table_name: str, limit=1000):
    with connect() as conn:
        if not _has_column(conn, table_name, "kode_organisasi"):
            st.error(f"Kolom 'kode_organisasi' belum tersedia di {table_name}. Coba refresh setelah migrasi.")
            return pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT ?", conn, params=[limit])

        return pd.read_sql_query(
            f"""
            SELECT
              t.id, t.kode_organisasi, t.created_at,
              t.jenis_hemofilia, t.jenis_penanganan, t.layanan_rawat,
              t.dosis_per_orang_per_kedatangan, t.frekuensi,
              io.hmhi_cabang,
              io.kota_cakupan_cabang
            FROM {table_name} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            ORDER BY t.id DESC
            LIMIT ?
            """, conn, params=[limit]
        )

# ======================== Helpers UI/Input ========================
def load_hmhi_to_kode():
    """
    Ambil HMHI cabang → kode_organisasi dari identitas_organisasi.
    Return:
        mapping: dict {hmhi_cabang -> kode_organisasi}
        options: list hmhi_cabang (urut alfabet)
    """
    with connect() as conn:
        try:
            df = pd.read_sql_query(
                "SELECT hmhi_cabang, kode_organisasi FROM identitas_organisasi ORDER BY id DESC",
                conn
            )
            if df.empty:
                return {}, []
            mapping = {}
            for _, row in df.iterrows():
                hmhi = (str(row["hmhi_cabang"]).strip() if pd.notna(row["hmhi_cabang"]) else "")
                kode = (str(row["kode_organisasi"]).strip() if pd.notna(row["kode_organisasi"]) else "")
                if hmhi:
                    mapping[hmhi] = kode
            return mapping, sorted(mapping.keys())
        except Exception:
            return {}, []

def safe_float(val, default=0.0):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return default
        return float(x)
    except Exception:
        return default

# ======================== Startup ========================
migrate_if_needed(TABLE)
init_db(TABLE)

# ======================== Label statis & opsi lain ========================
JENIS_HEMO_LABELS = [
    "Hemofilia A",
    "Hemofilia B",
    "Hemofilia tipe lain",
    "vWD",
    "Terduga Hemofilia",
    "Kelainan Pembekuan Darah Lain",
]
JENIS_PENANGANAN_OPTIONS = ["Prophylaxis", "On Demand"]
LAYANAN_RAWAT_OPTIONS = ["Rawat Jalan", "Rawat Inap"]

# ======================== Template Unggah ========================
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Jenis Hemofilia",
    "Jenis Penanganan",
    "Layanan Rawat",
    "Dosis/orang/kedatangan (IU)",
    "Frekuensi",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",                   # untuk mapping ke kode_organisasi
    "Jenis Hemofilia": "jenis_hemofilia",
    "Jenis Penanganan": "jenis_penanganan",
    "Layanan Rawat": "layanan_rawat",
    "Dosis/orang/kedatangan (IU)": "dosis_per_orang_per_kedatangan",
    "Frekuensi": "frekuensi",
}

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # ---- Pilihan dari HMHI Cabang (bukan Kode Organisasi) ----
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="pk::hmhi_select"
        )

    st.subheader("Form Penanganan Kesehatan")

    # Template awal: baris fixed sesuai label statis; kolom 'jenis_hemofilia' dikunci
    df_default = pd.DataFrame({
        "jenis_hemofilia": JENIS_HEMO_LABELS,
        "jenis_penanganan": [""] * len(JENIS_HEMO_LABELS),
        "layanan_rawat": [""] * len(JENIS_HEMO_LABELS),
        "dosis_per_orang_per_kedatangan": [0.0] * len(JENIS_HEMO_LABELS),
        "frekuensi": [""] * len(JENIS_HEMO_LABELS),
    })

    with st.form("pk::form"):
        ed = st.data_editor(
            df_default,
            key="pk::editor",
            use_container_width=True,
            num_rows="fixed",            # baris fixed mengikuti label statis
            hide_index=True,
            disabled=["jenis_hemofilia"],# kunci kolom label statis
            column_config={
                "jenis_hemofilia": st.column_config.TextColumn("Jenis Hemofilia"),
                "jenis_penanganan": st.column_config.SelectboxColumn(
                    "Jenis Penanganan",
                    options=[""] + JENIS_PENANGANAN_OPTIONS,
                    required=False
                ),
                "layanan_rawat": st.column_config.SelectboxColumn(
                    "Layanan Rawat",
                    options=[""] + LAYANAN_RAWAT_OPTIONS,
                    required=False
                ),
                "dosis_per_orang_per_kedatangan": st.column_config.NumberColumn(
                    "Dosis/orang/kedatangan (IU)",
                    help="Masukkan angka (contoh: 500).",
                    min_value=0.0, step=50.0, format="%.0f"
                ),
                "frekuensi": st.column_config.TextColumn(
                    "Frekuensi",
                    help="Contoh: 2x/minggu, 3x/bulan, harian, dsb."
                ),
            },
        )
        submit = st.form_submit_button("💾 Simpan")

    # --------- Catatan di bawah form (Kotak Info) ---------
    st.info(
        "**Keterangan:**\n"
        "- **Prophylaxis**: penanganan pencegahan perdarahan dengan cara memberikan *replacement therapy* "
        "secara teratur tanpa melihat ada atau tidaknya perdarahan.\n"
        "- **On Demand**: penanganan dengan memberikan *replacement therapy* hanya bila ada perdarahan."
    )

    if submit:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                n_saved = 0
                for _, row in ed.iterrows():
                    jenis_hemo = str(row.get("jenis_hemofilia") or "").strip()
                    jenis_pen = str(row.get("jenis_penanganan") or "").strip()
                    layanan = str(row.get("layanan_rawat") or "").strip()
                    dosis = safe_float(row.get("dosis_per_orang_per_kedatangan", 0.0))
                    frek = str(row.get("frekuensi") or "").strip()

                    # Lewati baris benar-benar kosong (semua input selain label masih default)
                    if not jenis_pen and not layanan and dosis == 0.0 and not frek:
                        continue

                    payload = {
                        "jenis_hemofilia": jenis_hemo,
                        "jenis_penanganan": jenis_pen,
                        "layanan_rawat": layanan,
                        "dosis_per_orang_per_kedatangan": dosis,
                        "frekuensi": frek,
                    }
                    insert_row(TABLE, payload, kode_organisasi)
                    n_saved += 1

                if n_saved > 0:
                    st.success(f"{n_saved} baris tersimpan untuk **{selected_hmhi}**.")
                else:
                    st.info("Tidak ada baris valid untuk disimpan.")

with tab_data:
    st.subheader("📄 Data Tersimpan — Penanganan Kesehatan")
    df = read_with_kota(TABLE, limit=1000)
    if df.empty:
        st.info("Belum ada data tersimpan.")
    else:
        cols_order = [
            "hmhi_cabang", "kota_cakupan_cabang", "created_at",
            "jenis_hemofilia", "jenis_penanganan", "layanan_rawat",
            "dosis_per_orang_per_kedatangan", "frekuensi"
        ]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "jenis_hemofilia": "Jenis Hemofilia",
            "jenis_penanganan": "Jenis Penanganan",
            "layanan_rawat": "Layanan Rawat",
            "dosis_per_orang_per_kedatangan": "Dosis/orang/kedatangan (IU)",
            "frekuensi": "Frekuensi",
        })

        st.dataframe(view, use_container_width=True)

        # Unduh Excel (data tersimpan)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="PenangananKesehatan")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buf.getvalue(),
            file_name="penanganan_kesehatan.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="pk::download"
        )

    st.divider()
    st.markdown("### 📥 Template & Unggah Excel")

    # --- Unduh Template Kosong
    template_df = pd.DataFrame([{
        "HMHI cabang": "",
        "Jenis Hemofilia": "",
        "Jenis Penanganan": "",
        "Layanan Rawat": "",
        "Dosis/orang/kedatangan (IU)": 0,
        "Frekuensi": "",
    }], columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        template_df.to_excel(w, index=False, sheet_name="Template_PK")
    st.download_button(
        "📄 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_penanganan_kesehatan.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="pk::dl_template"
    )

    # --- Unggah File
    up = st.file_uploader(
        "Unggah file Excel (.xlsx) sesuai template",
        type=["xlsx"],
        key="pk::uploader"
    )

    def process_upload(df_up: pd.DataFrame):
        """Validasi & simpan unggahan. Kembalikan (log_df, n_ok)."""
        hmhi_map, _ = load_hmhi_to_kode()
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

                jenis_hemo = str((s.get("jenis_hemofilia") or "")).strip()
                jenis_pen  = str((s.get("jenis_penanganan") or "")).strip()
                layanan    = str((s.get("layanan_rawat") or "")).strip()
                dosis_val  = safe_float(s.get("dosis_per_orang_per_kedatangan", 0.0))
                frek       = str((s.get("frekuensi") or "")).strip()

                # jika benar-benar kosong (selain label)
                if not jenis_hemo and not jenis_pen and not layanan and dosis_val == 0.0 and not frek:
                    results.append({"Baris Excel": i+2, "Status": "LEWAT", "Keterangan": "Baris kosong — dilewati"})
                    continue

                payload = {
                    "jenis_hemofilia": jenis_hemo,
                    "jenis_penanganan": jenis_pen,
                    "layanan_rawat": layanan,
                    "dosis_per_orang_per_kedatangan": dosis_val,
                    "frekuensi": frek,
                }
                insert_row(TABLE, payload, kode_organisasi)
                results.append({"Baris Excel": i+2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {jenis_hemo or '(tanpa jenis)'}"})
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
        minimal = ["HMHI cabang", "Jenis Hemofilia"]
        missing = [c for c in minimal if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Minimal harus ada: " + ", ".join(minimal))
            st.stop()

        # Pastikan semua kolom template tersedia (tambahkan jika tidak ada)
        for c in TEMPLATE_COLUMNS:
            if c not in raw.columns:
                raw[c] = "" if c != "Dosis/orang/kedatangan (IU)" else 0

        st.caption("Pratinjau 20 baris pertama:")
        st.dataframe(raw[TEMPLATE_COLUMNS].head(20), use_container_width=True)

        # Ubah header ke nama kolom database internal
        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        if st.button("🚀 Proses & Simpan Unggahan", type="primary", key="pk::process"):
            log_df, n_ok = process_upload(df_up)
            st.write("**Hasil unggah:**")
            st.dataframe(log_df, use_container_width=True)

            ok = (log_df["Status"] == "OK").sum()
            fail = (log_df["Status"] == "GAGAL").sum()
            skip = (log_df["Status"] == "LEWAT").sum()
            if ok:
                st.success(f"Berhasil menyimpan {ok} baris.")
            if fail:
                st.error(f"Gagal menyimpan {fail} baris.")
            if skip:
                st.info(f"Dilewati {skip} baris kosong.")

            # Unduh log hasil
            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                log_df.to_excel(w, index=False, sheet_name="Hasil")
            st.download_button(
                "📄 Unduh Log Hasil",
                log_buf.getvalue(),
                file_name="log_hasil_unggah_penanganan_kesehatan.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="pk::dl_log"
            )
