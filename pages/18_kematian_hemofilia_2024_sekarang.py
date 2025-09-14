import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Kematian Penyandang Hemofilia (2024–Sekarang)", page_icon="🕯️", layout="wide")
st.title("🕯️ Jumlah Kematian Penyandang Hemofilia (1 Januari 2024–Sekarang)")

DB_PATH = "hemofilia.db"
TABLE = "kematian_hemofilia_2024kini"

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

def migrate_if_needed(table_name: str):
    """
    Skema final:
      id, kode_organisasi, created_at,
      penyebab_kematian, perdarahan, gangguan_hati, hiv, penyebab_lain, tahun_kematian
    """
    with connect() as conn:
        cur = conn.cursor()
        if not _table_exists(conn, table_name):
            return
        needed = [
            "kode_organisasi", "penyebab_kematian", "perdarahan",
            "gangguan_hati", "hiv", "penyebab_lain", "tahun_kematian",
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
                    penyebab_kematian TEXT,
                    perdarahan INTEGER,
                    gangguan_hati INTEGER,
                    hiv INTEGER,
                    penyebab_lain TEXT,
                    tahun_kematian INTEGER,
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
            # Salin data lama (kolom yang ada)
            cur.execute(f"PRAGMA table_info({table_name})")
            old_cols = [r[1] for r in cur.fetchall() if r[1] != "id"]
            if old_cols:
                cols_csv = ", ".join(old_cols)
                cur.execute(f"INSERT INTO {table_name}_new ({cols_csv}) SELECT {cols_csv} FROM {table_name}")

            # Tukar tabel
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
                penyebab_kematian TEXT,
                perdarahan INTEGER,
                gangguan_hati INTEGER,
                hiv INTEGER,
                penyebab_lain TEXT,
                tahun_kematian INTEGER,
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

def read_with_label(table_name: str, limit=1000):
    """Ambil data + label organisasi (prioritas HMHI Cabang, fallback kota_cakupan_cabang)."""
    with connect() as conn:
        if not _has_column(conn, table_name, "kode_organisasi"):
            st.error(f"Kolom 'kode_organisasi' belum tersedia di {table_name}. Coba refresh setelah migrasi.")
            return pd.read_sql_query(f"SELECT * FROM {table_name} ORDER BY id DESC LIMIT ?", conn, params=[limit])

        label_col = _get_first_existing_col(
            conn, "identitas_organisasi",
            ["HMHI_cabang", "hmhi_cabang", "HMHI cabang", "kota_cakupan_cabang"]
        )
        select_label = f"io.[{label_col}]" if label_col and " " in label_col else f"io.{label_col}" if label_col else "NULL"

        return pd.read_sql_query(
            f"""
            SELECT
              t.id, t.kode_organisasi, t.created_at,
              t.penyebab_kematian, t.perdarahan, t.gangguan_hati, t.hiv, t.penyebab_lain, t.tahun_kematian,
              {select_label} AS label_organisasi
            FROM {table_name} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            ORDER BY t.id DESC
            LIMIT ?
            """, conn, params=[limit]
        )

# ======================== Helpers UI/Input ========================
def load_kode_organisasi_with_label():
    """Mapping kode_organisasi -> 'HMHI Cabang' (tanpa kode). Duplikat → '(pilihan n)'."""
    with connect() as conn:
        try:
            label_col = _get_first_existing_col(
                conn, "identitas_organisasi",
                ["HMHI_cabang", "hmhi_cabang", "HMHI cabang", "kota_cakupan_cabang"]
            )
            if label_col is None:
                df = pd.read_sql_query("SELECT kode_organisasi FROM identitas_organisasi ORDER BY id DESC", conn)
                if df.empty:
                    return {}, []
                return {row["kode_organisasi"]: "-" for _, row in df.iterrows()}, df["kode_organisasi"].tolist()

            col_ref = f"[{label_col}]" if " " in label_col else label_col
            df = pd.read_sql_query(
                f"SELECT kode_organisasi, {col_ref} AS label FROM identitas_organisasi ORDER BY id DESC", conn
            )
            if df.empty:
                return {}, []

            labels = df["label"].fillna("-").astype(str).str.strip()
            counts = labels.value_counts()
            display, dup_index = [], {}
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

def safe_int(val, default=0):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return default
        return int(x)
    except Exception:
        return default

# ======================== Startup ========================
migrate_if_needed(TABLE)
init_db(TABLE)

# ======================== Daftar label Penyebab ========================
PENYEBAB_LABELS = [
    "Hemofilia A",
    "Hemofilia B",
    "Hemofilia tipe lain",
    "Terduga hemofilia",
    "vWD",
    "Kelainan pembekuan darah lain",
]

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # Pilih organisasi: tampilkan HMHI Cabang (tanpa kode)
    mapping, kode_list = load_kode_organisasi_with_label()
    if not kode_list:
        st.warning("Belum ada data Identitas Organisasi.")
        kode_organisasi = None
    else:
        kode_organisasi = st.selectbox(
            "Pilih Organisasi (HMHI Cabang)",
            options=kode_list,
            format_func=lambda x: mapping.get(x, "-"),
            key="kmh::kode_select"
        )

    st.subheader("Form Kematian Penyandang Hemofilia (2024–Sekarang)")

    # Dataframe default: 1 baris per label penyebab (label terkunci)
    df_default = pd.DataFrame({
        "penyebab_kematian": PENYEBAB_LABELS,
        "perdarahan": [0] * len(PENYEBAB_LABELS),
        "gangguan_hati": [0] * len(PENYEBAB_LABELS),
        "hiv": [0] * len(PENYEBAB_LABELS),
        "penyebab_lain": ["" for _ in PENYEBAB_LABELS],
        "tahun_kematian": [datetime.utcnow().year for _ in PENYEBAB_LABELS],
    })

    with st.form("kmh::form"):
        ed = st.data_editor(
            df_default,
            key="kmh::editor",
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            disabled=["penyebab_kematian"],
            column_config={
                "penyebab_kematian": st.column_config.TextColumn("Penyebab Kematian"),
                "perdarahan": st.column_config.NumberColumn("Perdarahan", min_value=0, step=1),
                "gangguan_hati": st.column_config.NumberColumn("Gangguan hati", min_value=0, step=1),
                "hiv": st.column_config.NumberColumn("HIV", min_value=0, step=1),
                "penyebab_lain": st.column_config.TextColumn("Penyebab lain (opsional)"),
                "tahun_kematian": st.column_config.NumberColumn("Tahun Kematian", min_value=2024, max_value=2100, step=1),
            },
        )
        submit = st.form_submit_button("💾 Simpan")

    # Simpan (skip baris kosong)
    if submit:
        if not kode_organisasi:
            st.error("Pilih organisasi terlebih dahulu.")
        else:
            n_saved, skipped = 0, 0
            for _, row in ed.iterrows():
                sebab = str(row.get("penyebab_kematian") or "").strip()
                perd = safe_int(row.get("perdarahan", 0))
                hati = safe_int(row.get("gangguan_hati", 0))
                hivv = safe_int(row.get("hiv", 0))
                lain = str(row.get("penyebab_lain") or "").strip()
                thn  = safe_int(row.get("tahun_kematian", 0))
                if perd == 0 and hati == 0 and hivv == 0 and not lain:
                    skipped += 1
                    continue
                payload = {
                    "penyebab_kematian": sebab,
                    "perdarahan": perd,
                    "gangguan_hati": hati,
                    "hiv": hivv,
                    "penyebab_lain": lain,
                    "tahun_kematian": thn,
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
    st.subheader("📄 Data Tersimpan — Kematian Penyandang Hemofilia")

    # ====== Unduh Template Excel ======
    st.markdown("#### 📄 Unduh Template Excel")
    tmpl_buf = io.BytesIO()
    df_template = pd.DataFrame([
        {
            "penyebab_kematian": "Hemofilia A",
            "perdarahan": 0,
            "gangguan_hati": 0,
            "hiv": 0,
            "penyebab_lain": "",
            "tahun_kematian": datetime.utcnow().year,
            "kode_organisasi": "CAB001"
        },
        {
            "penyebab_kematian": "Hemofilia B",
            "perdarahan": 0,
            "gangguan_hati": 0,
            "hiv": 0,
            "penyebab_lain": "",
            "tahun_kematian": datetime.utcnow().year,
            "kode_organisasi": "CAB002"
        }
    ], columns=[
        "penyebab_kematian", "perdarahan", "gangguan_hati",
        "hiv", "penyebab_lain", "tahun_kematian", "kode_organisasi"
    ])
    with pd.ExcelWriter(tmpl_buf, engine="xlsxwriter") as w:
        df_template.to_excel(w, index=False, sheet_name="Template_Kematian_2024_Sekarang")
    st.download_button(
        "⬇️ Unduh Template Excel",
        tmpl_buf.getvalue(),
        file_name="template_kematian_hemofilia_2024_sekarang.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="kmh::download_template"
    )

    # ====== Unggah Excel (Impor Massal) ======
    st.markdown("### ⬆️ Unggah Excel (Impor Massal)")
    st.caption("File yang diunggah **harus** menyertakan semua kolom wajib.")
    up = st.file_uploader("Pilih file Excel (.xlsx)", type=["xlsx"], key="kmh::uploader")
    if up is not None:
        try:
            df_up = pd.read_excel(up)
            st.write("Pratinjau file diunggah:")
            st.dataframe(df_up.head(20), use_container_width=True)

            # Normalisasi nama kolom -> snake_case sederhana
            norm_map = {c: c.strip().lower().replace(" ", "_") for c in df_up.columns}
            df_norm = df_up.rename(columns=norm_map)

            required = [
                "penyebab_kematian", "perdarahan", "gangguan_hati",
                "hiv", "penyebab_lain", "tahun_kematian", "kode_organisasi"
            ]
            missing = [c for c in required if c not in df_norm.columns]
            if missing:
                st.error(f"Kolom wajib belum lengkap di file: {missing}")
            else:
                n_ok, n_skip = 0, 0
                for _, r in df_norm.iterrows():
                    sebab = str(r.get("penyebab_kematian") or "").strip()
                    perd  = safe_int(r.get("perdarahan", 0))
                    hati  = safe_int(r.get("gangguan_hati", 0))
                    hivv  = safe_int(r.get("hiv", 0))
                    lain  = str(r.get("penyebab_lain") or "").strip()
                    thn   = safe_int(r.get("tahun_kematian", 0))
                    korg  = str(r.get("kode_organisasi") or "").strip()

                    if not korg:
                        n_skip += 1
                        continue
                    # Jika baris kosong total (angka 0, teks kosong) → lewati
                    if perd == 0 and hati == 0 and hivv == 0 and not lain and not sebab:
                        n_skip += 1
                        continue

                    payload = {
                        "penyebab_kematian": sebab,
                        "perdarahan": perd,
                        "gangguan_hati": hati,
                        "hiv": hivv,
                        "penyebab_lain": lain,
                        "tahun_kematian": thn,
                    }
                    insert_row(TABLE, payload, korg)
                    n_ok += 1

                if n_ok:
                    st.success(f"Impor selesai: {n_ok} baris masuk, {n_skip} baris dilewati.")
                else:
                    st.info("Tidak ada baris valid yang diimpor (cek kolom dan nilai).")
        except Exception as e:
            st.error(f"Gagal memproses file: {e}")

    # ====== Tampilkan data tersimpan ======
    df = read_with_label(TABLE, limit=1000)
    if df.empty:
        st.info("Belum ada data tersimpan.")
    else:
        cols_order = [
            "label_organisasi", "created_at",
            "penyebab_kematian", "perdarahan", "gangguan_hati", "hiv",
            "penyebab_lain", "tahun_kematian"
        ]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "label_organisasi": "HMHI Cabang / Label Organisasi",
            "created_at": "Created At",
            "penyebab_kematian": "Penyebab Kematian",
            "perdarahan": "Perdarahan",
            "gangguan_hati": "Gangguan hati",
            "hiv": "HIV",
            "penyebab_lain": "Penyebab lain",
            "tahun_kematian": "Tahun Kematian",
        })
        st.dataframe(view, use_container_width=True)

        # Unduh data hasil gabung label
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="Kematian_2024_Sekarang")
        st.download_button(
            "⬇️ Unduh Data Excel",
            buf.getvalue(),
            file_name="kematian_hemofilia_2024_sekarang.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="kmh::download"
        )
