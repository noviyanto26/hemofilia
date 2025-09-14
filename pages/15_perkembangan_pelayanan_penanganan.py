import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Perkembangan Pelayanan Penanganan Hemofilia", page_icon="📈", layout="wide")
st.title("📈 Perkembangan Pelayanan Penanganan Hemofilia")

DB_PATH = "hemofilia.db"
TABLE = "perkembangan_pelayanan_penanganan"

# ======================== Util DB ========================
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

def _get_first_existing_col(conn, table, candidates):
    """Kembalikan nama kolom pertama yang ada dari kandidat; None jika tak ada."""
    cur = conn.cursor()
    try:
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        for c in candidates:
            if c in cols:
                return c
    except Exception:
        pass
    return None

def ensure_rumah_sakit_schema():
    """
    Pastikan tabel `rumah_sakit` tersedia dan memiliki:
      Nama (TEXT UNIQUE), Lokasi (TEXT), Propinsi (TEXT)
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

def migrate_if_needed(table_name: str):
    """
    Skema final:
      id, kode_organisasi, created_at,
      jenis, jumlah_terapi_gen, tahun,
      nama_rumah_sakit, lokasi, propinsi
    """
    with connect() as conn:
        if not _table_exists(conn, table_name):
            return
        cur = conn.cursor()
        need = [
            "kode_organisasi", "jenis", "jumlah_terapi_gen", "tahun",
            "nama_rumah_sakit", "lokasi", "propinsi"
        ]
        cur.execute(f"PRAGMA table_info({table_name})")
        have = [r[1] for r in cur.fetchall()]
        if all(c in have for c in need):
            return

        st.warning(f"Migrasi skema: menyesuaikan tabel {table_name} …")
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT,
                    created_at TEXT NOT NULL,
                    jenis TEXT,
                    jumlah_terapi_gen INTEGER,
                    tahun INTEGER,
                    nama_rumah_sakit TEXT,
                    lokasi TEXT,
                    propinsi TEXT,
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
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
                jenis TEXT,
                jumlah_terapi_gen INTEGER,
                tahun INTEGER,
                nama_rumah_sakit TEXT,
                lokasi TEXT,
                propinsi TEXT,
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

        # Ambil label organisasi: prioritaskan HMHI Cabang
        label_col = _get_first_existing_col(conn, "identitas_organisasi",
                                            ["HMHI_cabang", "hmhi_cabang", "HMHI cabang", "kota_cakupan_cabang"])
        select_label = f"io.[{label_col}]" if label_col and " " in label_col else f"io.{label_col}" if label_col else "NULL"
        return pd.read_sql_query(
            f"""
            SELECT
              t.id, t.kode_organisasi, t.created_at,
              t.jenis, t.jumlah_terapi_gen, t.tahun,
              t.nama_rumah_sakit, t.lokasi, t.propinsi,
              {select_label} AS label_organisasi
            FROM {table_name} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            ORDER BY t.id DESC
            LIMIT ?
            """, conn, params=[limit]
        )

# ======================== Helpers UI/Input ========================
def load_kode_organisasi_with_label():
    """Mapping kode_organisasi -> 'HMHI Cabang' (tanpa kode).
       Duplikat diberi suffix '(pilihan n)'. """
    with connect() as conn:
        try:
            label_col = _get_first_existing_col(
                conn, "identitas_organisasi",
                ["HMHI_cabang", "hmhi_cabang", "HMHI cabang", "kota_cakupan_cabang"]
            )

            if label_col is None:
                df = pd.read_sql_query(
                    "SELECT kode_organisasi FROM identitas_organisasi ORDER BY id DESC", conn
                )
                if df.empty:
                    return {}, []
                mapping = {row["kode_organisasi"]: "-" for _, row in df.iterrows()}
                return mapping, df["kode_organisasi"].tolist()

            col_ref = f"[{label_col}]" if " " in label_col else label_col
            df = pd.read_sql_query(
                f"SELECT kode_organisasi, {col_ref} AS label FROM identitas_organisasi ORDER BY id DESC",
                conn
            )
            if df.empty:
                return {}, []

            labels = df["label"].fillna("-").astype(str).str.strip()
            counts = labels.value_counts()
            display = []
            dup_index = {}
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

def load_rs_names():
    """Ambil daftar RS dari tabel rumah_sakit (Nama, Lokasi, Propinsi)."""
    with connect() as conn:
        try:
            df = pd.read_sql_query("SELECT Nama, Lokasi, Propinsi FROM rumah_sakit ORDER BY Nama", conn)
            names = sorted(set(df["Nama"].dropna().astype(str).tolist()))
            return names, df
        except Exception:
            return [], pd.DataFrame(columns=["Nama", "Lokasi", "Propinsi"])

def safe_int(val, default=0):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return default
        return int(x)
    except Exception:
        return default

# ======================== Startup ========================
ensure_rumah_sakit_schema()
migrate_if_needed(TABLE)
init_db(TABLE)

# ======================== Label statis untuk "Jenis" ========================
JENIS_LABELS = [
    "Hemofilia A Berat",
    "Hemofilia B Berat",
    "Hemofilia tipe lain",
    "vWD",
]

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    mapping, kode_list = load_kode_organisasi_with_label()
    if not kode_list:
        st.warning("Belum ada data Identitas Organisasi.")
        kode_organisasi = None
    else:
        # ✅ Hanya menampilkan HMHI Cabang (tanpa kode)
        kode_organisasi = st.selectbox(
            "Pilih Organisasi (HMHI Cabang)",
            options=kode_list,
            format_func=lambda x: mapping.get(x, "-"),
            key="pph::kode_select"
        )

    # Master RS
    rs_names, df_rs = load_rs_names()
    if not rs_names:
        st.error("Tidak bisa memuat daftar Rumah Sakit dari tabel 'rumah_sakit'. Pastikan kolom 'Nama', 'Lokasi', 'Propinsi' ada.")
    else:
        # Siapkan label "Nama - Lokasi - Propinsi"
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

        st.subheader("Form Perkembangan Pelayanan Penanganan Hemofilia")

        # Data awal: 1 baris per label Jenis
        df_default = pd.DataFrame({
            "jenis": JENIS_LABELS,
            "jumlah_terapi_gen": [0] * len(JENIS_LABELS),
            "tahun": [datetime.utcnow().year] * len(JENIS_LABELS),
            "nama_rumah_sakit": [""] * len(JENIS_LABELS),
        })

        with st.form("pph::form"):
            ed = st.data_editor(
                df_default,
                key="pph::editor",
                use_container_width=True,
                hide_index=True,
                num_rows="fixed",
                disabled=["jenis"],
                column_config={
                    "jenis": st.column_config.TextColumn("Jenis"),
                    "jumlah_terapi_gen": st.column_config.NumberColumn(
                        "Jumlah Penyandang yang telah menjalani Terapi Gen",
                        min_value=0, step=1
                    ),
                    "tahun": st.column_config.NumberColumn(
                        "Tahun",
                        help="Contoh: 2024",
                        min_value=1900, max_value=2100, step=1
                    ),
                    "nama_rumah_sakit": st.column_config.SelectboxColumn(
                        "Nama Rumah Sakit (Nama - Lokasi - Propinsi)",
                        options=option_labels,
                        required=False
                    ),
                },
            )

            # Pratinjau data yang akan disimpan (pecah label RS)
            if not ed.empty:
                preview = []
                for _, row in ed.iterrows():
                    label = (row.get("nama_rumah_sakit") or "").strip()
                    parts = label_to_parts.get(label, {"nama": "", "lokasi": "", "propinsi": ""})
                    preview.append({
                        "Jenis": row.get("jenis", ""),
                        "Jumlah Terapi Gen": row.get("jumlah_terapi_gen", 0),
                        "Tahun": row.get("tahun", ""),
                        "Nama Rumah Sakit": parts["nama"],
                        "Lokasi": parts["lokasi"],
                        "Propinsi": parts["propinsi"],
                    })
                st.caption("Pratinjau baris yang akan disimpan:")
                st.dataframe(pd.DataFrame(preview), use_container_width=True)

            submitted = st.form_submit_button("💾 Simpan")

        if submitted:
            if not kode_organisasi:
                st.error("Pilih organisasi terlebih dahulu.")
            else:
                n_saved, skipped = 0, 0
                for _, row in ed.iterrows():
                    jenis = str(row.get("jenis") or "").strip()
                    jml  = safe_int(row.get("jumlah_terapi_gen", 0))
                    th   = safe_int(row.get("tahun", 0))
                    label = (row.get("nama_rumah_sakit") or "").strip()
                    parts = label_to_parts.get(label, {"nama": "", "lokasi": "", "propinsi": ""})

                    # Lewati baris benar-benar kosong
                    if jml == 0 and th == 0 and not parts["nama"]:
                        skipped += 1
                        continue

                    payload = {
                        "jenis": jenis,
                        "jumlah_terapi_gen": jml,
                        "tahun": th,
                        "nama_rumah_sakit": parts["nama"],
                        "lokasi": parts["lokasi"],
                        "propinsi": parts["propinsi"],
                    }
                    insert_row(TABLE, payload, kode_organisasi)
                    n_saved += 1

                if n_saved > 0:
                    msg = f"{n_saved} baris tersimpan"
                    if skipped > 0:
                        msg += f" ({skipped} baris kosong diabaikan)"
                    st.success(f"{msg} untuk {mapping.get(kode_organisasi, '-')}.")
                else:
                    st.info("Tidak ada baris valid untuk disimpan (semua baris kosong).")

with tab_data:
    st.subheader("📄 Data Tersimpan")
    # ====== Unggah Excel untuk impor massal ======
    st.markdown("### ⬆️ Unggah Excel (Impor Massal)")
    st.caption(
        "Format kolom yang didukung (WAJIB sertakan `kode_organisasi` di file): "
        "`jenis, jumlah_terapi_gen, tahun, nama_rumah_sakit, lokasi, propinsi, kode_organisasi`. "
        "Atau gunakan kolom gabungan `nama_rumah_sakit` berformat `Nama - Lokasi - Propinsi` (akan dipecah otomatis)."
    )

    # ⚠️ Tidak ada selectbox HMHI Cabang di tab Data
    up = st.file_uploader("Pilih file Excel (.xlsx)", type=["xlsx"], key="pph::uploader")
    if up is not None:
        try:
            df_up = pd.read_excel(up)
            st.write("Pratinjau file diunggah:")
            st.dataframe(df_up.head(20), use_container_width=True)

            # Normalisasi nama kolom
            norm_map = {c: c.strip().lower().replace(" ", "_") for c in df_up.columns}
            df_norm = df_up.rename(columns=norm_map)

            # Pecah kolom gabungan jika ada (nama_rumah_sakit: 'Nama - Lokasi - Propinsi')
            if "nama_rumah_sakit" in df_norm.columns and \
               ("lokasi" not in df_norm.columns or "propinsi" not in df_norm.columns):
                parts = df_norm["nama_rumah_sakit"].astype(str).str.split(" - ", n=2, expand=True)
                if parts.shape[1] >= 1:
                    df_norm["nama_rumah_sakit"] = parts[0].str.strip()
                if parts.shape[1] >= 2 and "lokasi" not in df_norm.columns:
                    df_norm["lokasi"] = parts[1].str.strip()
                if parts.shape[1] >= 3 and "propinsi" not in df_norm.columns:
                    df_norm["propinsi"] = parts[2].str.strip()

            # Wajib kolom ini:
            required = ["jenis", "jumlah_terapi_gen", "tahun", "nama_rumah_sakit", "kode_organisasi"]
            missing = [c for c in required if c not in df_norm.columns]
            if missing:
                st.error(f"Kolom wajib belum lengkap di file: {missing}")
            else:
                n_ok, n_skip = 0, 0
                for _, r in df_norm.iterrows():
                    jns = str(r.get("jenis") or "").strip()
                    jml = safe_int(r.get("jumlah_terapi_gen", 0))
                    thn = safe_int(r.get("tahun", 0))
                    nm  = str(r.get("nama_rumah_sakit") or "").strip()
                    lok = str(r.get("lokasi") or "").strip()
                    prop= str(r.get("propinsi") or r.get("provinsi") or "").strip()
                    korg = str(r.get("kode_organisasi") or "").strip()

                    # Wajib punya kode_organisasi di setiap baris
                    if not korg:
                        n_skip += 1
                        continue

                    # Abaikan baris kosong total
                    if not jns and jml == 0 and thn == 0 and not nm:
                        n_skip += 1
                        continue

                    payload = {
                        "jenis": jns,
                        "jumlah_terapi_gen": jml,
                        "tahun": thn,
                        "nama_rumah_sakit": nm,
                        "lokasi": lok,
                        "propinsi": prop,
                    }
                    insert_row(TABLE, payload, korg)
                    n_ok += 1

                if n_ok:
                    msg = f"Impor selesai: {n_ok} baris masuk"
                    if n_skip:
                        msg += f", {n_skip} baris dilewati"
                    st.success(msg)
                else:
                    st.info("Tidak ada baris valid yang diimpor (cek kolom dan nilai).")
        except Exception as e:
            st.error(f"Gagal memproses file: {e}")

    # ====== Tampilkan data tersimpan ======
    df = read_with_kota(TABLE, limit=1000)

    if df.empty:
        st.info("Belum ada data.")
    else:
        cols_order = [
            "label_organisasi", "created_at",
            "jenis", "jumlah_terapi_gen", "tahun",
            "nama_rumah_sakit", "lokasi", "propinsi",
        ]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "label_organisasi": "HMHI Cabang / Label Organisasi",
            "created_at": "Created At",
            "jenis": "Jenis",
            "jumlah_terapi_gen": "Jumlah Terapi Gen",
            "tahun": "Tahun",
            "nama_rumah_sakit": "Nama Rumah Sakit",
            "lokasi": "Lokasi",
            "propinsi": "Propinsi",
        })

        st.dataframe(view, use_container_width=True)

        # Unduh Excel
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="Perkembangan_Penanganan")
        st.download_button(
            "⬇️ Unduh Excel",
            buf.getvalue(),
            file_name="perkembangan_pelayanan_penanganan.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="pph::download"
        )
