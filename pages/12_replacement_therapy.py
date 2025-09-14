import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Replacement Therapy", page_icon="🧪", layout="wide")
st.title("🧪 Ketersediaan Produk Replacement Therapy")

DB_PATH = "hemofilia.db"
TABLE = "ketersediaan_produk_replacement"

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
      id, kode_organisasi, created_at, produk, ketersediaan, digunakan, merk,
      jumlah_pengguna, jumlah_iu_per_kemasan, harga
    """
    with connect() as conn:
        cur = conn.cursor()
        if not _table_exists(conn, table_name):
            return

        needed = [
            "kode_organisasi",
            "produk",
            "ketersediaan",
            "digunakan",
            "merk",
            "jumlah_pengguna",
            "jumlah_iu_per_kemasan",
            "harga",
        ]
        cur.execute(f"PRAGMA table_info({table_name})")
        have = [r[1] for r in cur.fetchall()]
        if all(col in have for col in needed):
            return  # sudah sesuai

        st.warning(f"Migrasi skema: menyesuaikan tabel {table_name} …")
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            # Buat tabel baru dengan skema lengkap
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT,
                    created_at TEXT NOT NULL,
                    produk TEXT,
                    ketersediaan TEXT,
                    digunakan TEXT,
                    merk TEXT,
                    jumlah_pengguna INTEGER,
                    jumlah_iu_per_kemasan INTEGER,
                    harga REAL,
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
                produk TEXT,
                ketersediaan TEXT,
                digunakan TEXT,
                merk TEXT,
                jumlah_pengguna INTEGER,
                jumlah_iu_per_kemasan INTEGER,
                harga REAL,
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
        # Jika belum ada foreign-key kolom, tetap tampilkan apa adanya
        if not _has_column(conn, table_name, "kode_organisasi"):
            st.error(f"Kolom 'kode_organisasi' belum tersedia di {table_name}. Coba refresh setelah migrasi.")
            return pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT ?", conn, params=[limit])

        return pd.read_sql_query(
            f"""
            SELECT
              t.id, t.kode_organisasi, t.created_at,
              t.produk, t.ketersediaan, t.digunakan, t.merk,
              t.jumlah_pengguna, t.jumlah_iu_per_kemasan, t.harga,
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

def safe_int(val, default=0):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return default
        return int(x)
    except Exception:
        return default

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

# ======================== Konstanta UI ========================
PRODUK_OPTIONS = [
    "Plasma (FFP)",
    "Cryoprecipitate",
    "Konsentrat (plasma derived)",
    "Konsentrat (rekombinan)",
    "Konsentrat (prolonged half life)",
    "Prothrombin Complex",
    "DDAVP",
    "Emicizumab (Hemlibra)",
    "Konsentrat Bypassing Agent",
]
KETERSEDIAAN_OPTIONS = ["Tersedia", "Tidak Tersedia"]
YA_TIDAK_OPTIONS = ["Ya", "Tidak"]

# ======================== Template Unggah ========================
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Produk",
    "Ketersediaan",
    "Digunakan",
    "Merk",
    "Jumlah Pengguna",
    "Jumlah iu/vial per kemasan",
    "Harga",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",            # hanya untuk mapping ke kode_organisasi
    "Produk": "produk",
    "Ketersediaan": "ketersediaan",
    "Digunakan": "digunakan",
    "Merk": "merk",
    "Jumlah Pengguna": "jumlah_pengguna",
    "Jumlah iu/vial per kemasan": "jumlah_iu_per_kemasan",
    "Harga": "harga",
}

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # --- Pilihan dari HMHI Cabang ---
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="rt::hmhi_select"
        )

    st.subheader("Form Ketersediaan Produk Replacement Therapy")

    # Template data awal (dinamis)
    df_default = pd.DataFrame({
        "produk": ["", "", "", "", ""],
        "ketersediaan": ["", "", "", "", ""],
        "digunakan": ["", "", "", "", ""],
        "merk": ["", "", "", "", ""],
        "jumlah_pengguna": [0, 0, 0, 0, 0],
        "jumlah_iu_per_kemasan": [0, 0, 0, 0, 0],
        "harga": [0.0, 0.0, 0.0, 0.0, 0.0],
    })

    with st.form("rt::form"):
        ed = st.data_editor(
            df_default,
            key="rt::editor",
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
            column_config={
                "produk": st.column_config.SelectboxColumn(
                    "Produk",
                    options=[""] + PRODUK_OPTIONS,
                    required=False
                ),
                "ketersediaan": st.column_config.SelectboxColumn(
                    "Ketersediaan",
                    options=[""] + KETERSEDIAAN_OPTIONS,
                    required=False
                ),
                "digunakan": st.column_config.SelectboxColumn(
                    "Digunakan",
                    options=[""] + YA_TIDAK_OPTIONS,
                    required=False
                ),
                "merk": st.column_config.TextColumn("Merk", help="Contoh: merek dagang/brand produk"),
                "jumlah_pengguna": st.column_config.NumberColumn(
                    "Jumlah Pengguna", min_value=0, step=1
                ),
                "jumlah_iu_per_kemasan": st.column_config.NumberColumn(
                    "Jumlah iu/vial dalam 1 kemasan", min_value=0, step=1
                ),
                "harga": st.column_config.NumberColumn(
                    "Harga", help="Nilai numerik (contoh: 1250000)", min_value=0.0, step=1000.0, format="%.0f"
                ),
            },
        )
        submit = st.form_submit_button("💾 Simpan")

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
                    produk = str(row.get("produk") or "").strip()
                    ketersediaan = str(row.get("ketersediaan") or "").strip()
                    digunakan = str(row.get("digunakan") or "").strip()
                    merk = str(row.get("merk") or "").strip()
                    jml_pengguna = safe_int(row.get("jumlah_pengguna", 0))
                    jml_iu = safe_int(row.get("jumlah_iu_per_kemasan", 0))
                    harga = safe_float(row.get("harga", 0.0))

                    # baris kosong dilewati
                    is_all_empty = (not produk and not ketersediaan and not digunakan and not merk and jml_pengguna == 0 and jml_iu == 0 and harga == 0.0)
                    if is_all_empty:
                        continue

                    payload = {
                        "produk": produk,
                        "ketersediaan": ketersediaan,
                        "digunakan": digunakan,
                        "merk": merk,
                        "jumlah_pengguna": jml_pengguna,
                        "jumlah_iu_per_kemasan": jml_iu,
                        "harga": harga,
                    }
                    insert_row(TABLE, payload, kode_organisasi)
                    n_saved += 1

                if n_saved > 0:
                    st.success(f"{n_saved} baris tersimpan untuk **{selected_hmhi}**.")
                else:
                    st.info("Tidak ada baris valid untuk disimpan.")

with tab_data:
    st.subheader("📄 Data Tersimpan — Replacement Therapy")
    df = read_with_kota(TABLE, limit=1000)
    if df.empty:
        st.info("Belum ada data tersimpan.")
    else:
        # Susun tampilan kolom
        cols_order = [
            "hmhi_cabang", "kota_cakupan_cabang", "created_at",
            "produk", "ketersediaan", "digunakan", "merk",
            "jumlah_pengguna", "jumlah_iu_per_kemasan", "harga"
        ]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "produk": "Produk",
            "ketersediaan": "Ketersediaan",
            "digunakan": "Digunakan",
            "merk": "Merk",
            "jumlah_pengguna": "Jumlah Pengguna",
            "jumlah_iu_per_kemasan": "Jumlah iu/vial per kemasan",
            "harga": "Harga",
        })

        # Format kolom Harga ke tampilan ribuan (tanpa mengubah data asli)
        view_fmt = view.copy()
        if "Harga" in view_fmt.columns:
            def _fmt_harga(x):
                try:
                    xi = int(float(x))
                    return f"{xi:,}".replace(",", ".")
                except Exception:
                    return x
            view_fmt["Harga"] = view_fmt["Harga"].apply(_fmt_harga)

        st.dataframe(view_fmt, use_container_width=True)

        # Unduh Excel (pakai data asli, bukan string terformat)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="ReplacementTherapy")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buf.getvalue(),
            file_name="ketersediaan_produk_replacement_therapy.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="rt::download"
        )

    st.divider()
    st.markdown("### 📥 Template & Unggah Excel")

    # --- Unduh Template Kosong
    template_df = pd.DataFrame([{
        "HMHI cabang": "",
        "Produk": "",
        "Ketersediaan": "",
        "Digunakan": "",
        "Merk": "",
        "Jumlah Pengguna": 0,
        "Jumlah iu/vial per kemasan": 0,
        "Harga": 0,
    }], columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        template_df.to_excel(w, index=False, sheet_name="Template_RT")
    st.download_button(
        "📄 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_replacement_therapy.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="rt::dl_template"
    )

    # --- Unggah File
    up = st.file_uploader(
        "Unggah file Excel (.xlsx) sesuai template",
        type=["xlsx"],
        key="rt::uploader"
    )

    def process_upload(df_up: pd.DataFrame):
        """Validasi & simpan unggahan. Kembalikan (log_df, n_ok)."""
        hmhi_map, _ = load_hmhi_to_kode()
        results = []
        n_ok = 0

        for i in range(len(df_up)):
            try:
                s = df_up.iloc[i]  # Series -> punya .get
                hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                if not hmhi:
                    raise ValueError("Kolom 'HMHI cabang' kosong.")
                kode_organisasi = hmhi_map.get(hmhi)
                if not kode_organisasi:
                    raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                produk = str((s.get("produk") or "")).strip()
                if produk and produk not in PRODUK_OPTIONS:
                    raise ValueError(f"Produk tidak valid: '{produk}'")

                ketersediaan = str((s.get("ketersediaan") or "")).strip()
                if ketersediaan and ketersediaan not in KETERSEDIAAN_OPTIONS:
                    raise ValueError("Nilai 'Ketersediaan' harus 'Tersedia' atau 'Tidak Tersedia'")

                digunakan = str((s.get("digunakan") or "")).strip()
                if digunakan and digunakan not in YA_TIDAK_OPTIONS:
                    raise ValueError("Nilai 'Digunakan' harus 'Ya' atau 'Tidak'")

                merk = str((s.get("merk") or "")).strip()
                jml_pengguna = safe_int(s.get("jumlah_pengguna", 0))
                jml_iu       = safe_int(s.get("jumlah_iu_per_kemasan", 0))
                harga_val    = safe_float(s.get("harga", 0.0))

                # jika benar-benar kosong, lewati
                is_all_empty = (not produk and not ketersediaan and not digunakan and not merk and jml_pengguna == 0 and jml_iu == 0 and harga_val == 0.0)
                if is_all_empty:
                    results.append({"Baris Excel": i+2, "Status": "LEWAT", "Keterangan": "Baris kosong — dilewati"})
                    continue

                payload = {
                    "produk": produk,
                    "ketersediaan": ketersediaan,
                    "digunakan": digunakan,
                    "merk": merk,
                    "jumlah_pengguna": jml_pengguna,
                    "jumlah_iu_per_kemasan": jml_iu,
                    "harga": harga_val,
                }
                insert_row(TABLE, payload, kode_organisasi)
                results.append({"Baris Excel": i+2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {produk or '(tanpa produk)'}"})
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
        minimal = ["HMHI cabang"]
        missing = [c for c in minimal if c not in raw.columns]
        if missing:
            st.error("Header kolom tidak sesuai. Minimal harus ada: " + ", ".join(minimal))
            st.stop()

        # Pastikan semua kolom template tersedia (tambahkan jika tidak ada)
        for c in TEMPLATE_COLUMNS:
            if c not in raw.columns:
                raw[c] = ""

        st.caption("Pratinjau 20 baris pertama:")
        st.dataframe(raw[TEMPLATE_COLUMNS].head(20), use_container_width=True)

        # Ubah header ke nama kolom database
        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        if st.button("🚀 Proses & Simpan Unggahan", type="primary", key="rt::process"):
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
                file_name="log_hasil_unggah_replacement_therapy.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="rt::dl_log"
            )
