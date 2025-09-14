import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Infeksi melalui Transfusi Darah", page_icon="🧬", layout="wide")
st.title("🧬 Jumlah Penyandang Hemofilia Terinfeksi Penyakit Menular Melalui Transfusi Darah")

DB_PATH = "hemofilia.db"
TABLE = "infeksi_transfusi_darah"

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
    with connect() as conn:
        cur = conn.cursor()
        if not _table_exists(conn, table_name):
            return

        needed = ["kode_organisasi", "kasus", "jml_hepatitis_c", "jml_hiv", "penyakit_menular_lainnya"]
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
                    kasus TEXT,
                    jml_hepatitis_c INTEGER,
                    jml_hiv INTEGER,
                    penyakit_menular_lainnya TEXT,
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
            # Salin data lama
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
                kasus TEXT,
                jml_hepatitis_c INTEGER,
                jml_hiv INTEGER,
                penyakit_menular_lainnya TEXT,
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
            st.error(f"Kolom 'kode_organisasi' belum tersedia di {table_name}.")
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
              t.kasus, t.jml_hepatitis_c, t.jml_hiv, t.penyakit_menular_lainnya,
              {select_label} AS label_organisasi
            FROM {table_name} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            ORDER BY t.id DESC
            LIMIT ?
            """, conn, params=[limit]
        )

# ======================== Helpers UI/Input ========================
def load_kode_organisasi_with_label():
    """Mapping kode_organisasi -> HMHI Cabang (tanpa kode)."""
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
            df = pd.read_sql_query(f"SELECT kode_organisasi, {col_ref} AS label FROM identitas_organisasi ORDER BY id DESC", conn)
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

# ======================== LABEL TETAP UNTUK "Kasus" ========================
KASUS_LABELS = [
    "Kasus lama (sebelum 2024)",
    "Kasus baru (2024/2025)",
]

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    mapping, kode_list = load_kode_organisasi_with_label()
    if not kode_list:
        st.warning("Belum ada data Identitas Organisasi.")
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

                if hc == 0 and hiv == 0 and not lain:
                    skipped += 1
                    continue

                payload = {
                    "kasus": kasus,
                    "jml_hepatitis_c": hc,
                    "jml_hiv": hiv,
                    "penyakit_menular_lainnya": lain,
                }
                insert_row(TABLE, payload, kode_organisasi)
                n_saved += 1

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
    df_template = pd.DataFrame([
        {
            "kasus": "Kasus lama (sebelum 2024)",
            "jml_hepatitis_c": 0,
            "jml_hiv": 0,
            "penyakit_menular_lainnya": "",
            "kode_organisasi": "CAB001"
        },
        {
            "kasus": "Kasus baru (2024/2025)",
            "jml_hepatitis_c": 0,
            "jml_hiv": 0,
            "penyakit_menular_lainnya": "",
            "kode_organisasi": "CAB002"
        }
    ], columns=["kasus", "jml_hepatitis_c", "jml_hiv", "penyakit_menular_lainnya", "kode_organisasi"])
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
    if up is not None:
        try:
            df_up = pd.read_excel(up)
            st.dataframe(df_up.head(20), use_container_width=True)

            norm_map = {c: c.strip().lower().replace(" ", "_") for c in df_up.columns}
            df_norm = df_up.rename(columns=norm_map)

            required = ["kasus", "jml_hepatitis_c", "jml_hiv", "penyakit_menular_lainnya", "kode_organisasi"]
            missing = [c for c in required if c not in df_norm.columns]
            if missing:
                st.error(f"Kolom wajib belum lengkap di file: {missing}")
            else:
                n_ok, n_skip = 0, 0
                for _, r in df_norm.iterrows():
                    kasus = str(r.get("kasus") or "").strip()
                    hc = safe_int(r.get("jml_hepatitis_c", 0))
                    hiv = safe_int(r.get("jml_hiv", 0))
                    lain = str(r.get("penyakit_menular_lainnya") or "").strip()
                    korg = str(r.get("kode_organisasi") or "").strip()

                    if not korg:
                        n_skip += 1
                        continue
                    if not kasus and hc == 0 and hiv == 0 and not lain:
                        n_skip += 1
                        continue

                    payload = {
                        "kasus": kasus,
                        "jml_hepatitis_c": hc,
                        "jml_hiv": hiv,
                        "penyakit_menular_lainnya": lain,
                    }
                    insert_row(TABLE, payload, korg)
                    n_ok += 1

                if n_ok:
                    st.success(f"Impor selesai: {n_ok} baris masuk, {n_skip} dilewati.")
                else:
                    st.info("Tidak ada baris valid yang diimpor.")
        except Exception as e:
            st.error(f"Gagal memproses file: {e}")

    # ====== Tampilkan data ======
    df = read_with_label(TABLE, limit=1000)
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
