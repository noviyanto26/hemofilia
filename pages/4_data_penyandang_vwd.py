import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

st.set_page_config(page_title="Data Penyandang vWD", page_icon="🩸", layout="wide")
st.title("🩸 Data Penyandang von Willebrand Disease (vWD) — per Kelompok Usia & Jenis Kelamin")

DB_PATH = "hemofilia.db"
TABLE = "vwd_usia_gender"

# Urutan kelompok usia (tampilan input)
AGE_GROUPS = ["0-4", "5-13", "14-18", "19-44", ">45", "Tidak ada data usia"]
# Urutan baris di template unggah
TEMPLATE_AGE_ORDER = [">45", "19-44", "14-18", "5-13", "0-4"]

GENDER_COLS = [
    ("laki_laki", "Laki-Laki"),
    ("perempuan", "Perempuan"),
    ("jk_tidak_terdata", "Jenis Kelamin Tidak Terdata"),
]
TOTAL_LABEL = "Total"

# ===== Kolom template unggah & pemetaan alias =====
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Kelompok Usia",
    "Laki-Laki",
    "Perempuan",
    "Jenis Kelamin Tidak Terdata",
    "Total",  # boleh diisi atau dikosongkan (akan dihitung otomatis)
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Kelompok Usia": "kelompok_usia",
    "Laki-Laki": "laki_laki",
    "Perempuan": "perempuan",
    "Jenis Kelamin Tidak Terdata": "jk_tidak_terdata",
    "Total": "total",
}

# ---------- Util DB ----------
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

# ---------- Migrasi kolom kode_organisasi jika belum ada ----------
def migrate_add_kode_organisasi_if_needed():
    with connect() as conn:
        if not _table_exists(conn, TABLE):
            return
        if _has_column(conn, TABLE, "kode_organisasi"):
            return

        st.warning("Migrasi skema vWD: menambahkan kolom kode_organisasi...")
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            cols_sql = "kelompok_usia TEXT, " + ", ".join([f"{k} INTEGER" for k, _ in GENDER_COLS] + ["total INTEGER"])
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT,
                    created_at TEXT NOT NULL,
                    {cols_sql},
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
            select_cols = "created_at, kelompok_usia, " + ", ".join([k for k, _ in GENDER_COLS]) + ", total"
            cur.execute(f"""
                INSERT INTO {TABLE}_new (id, kode_organisasi, created_at, kelompok_usia, {", ".join([k for k, _ in GENDER_COLS])}, total)
                SELECT id, NULL as kode_organisasi, {select_cols}
                FROM {TABLE}
                ORDER BY id
            """)
            cur.execute(f"ALTER TABLE {TABLE} RENAME TO {TABLE}_backup")
            cur.execute(f"ALTER TABLE {TABLE}_new RENAME TO {TABLE}")
            conn.commit()
            st.success("Migrasi selesai. Tabel lama disimpan sebagai _backup.")
        except Exception as e:
            conn.rollback()
            st.error(f"Migrasi vWD gagal: {e}")
        finally:
            cur.execute("PRAGMA foreign_keys=ON")

# ---------- Init tabel (skema final) ----------
def init_db():
    cols_sql = "kelompok_usia TEXT, " + ", ".join([f"{k} INTEGER" for k, _ in GENDER_COLS] + ["total INTEGER"])
    with connect() as conn:
        c = conn.cursor()
        c.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT,
                created_at TEXT NOT NULL,
                {cols_sql},
                FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
            )
        """)
        conn.commit()

# ---------- Helpers ----------
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
                hmhi_val = str(row["hmhi_cabang"]).strip() if pd.notna(row["hmhi_cabang"]) else ""
                kode_val = str(row["kode_organisasi"]).strip() if pd.notna(row["kode_organisasi"]) else ""
                if hmhi_val:
                    mapping[hmhi_val] = kode_val
            options = sorted(mapping.keys())
            return mapping, options
        except Exception:
            return {}, []

def insert_row(payload: dict, kode_organisasi: str):
    with connect() as conn:
        c = conn.cursor()
        keys = list(payload.keys())
        cols = ", ".join(keys)
        placeholders = ", ".join(["?"] * len(keys))
        vals = [payload[k] for k in keys]
        c.execute(
            f"INSERT INTO {TABLE} (kode_organisasi, created_at, {cols}) VALUES (?, ?, {placeholders})",
            [kode_organisasi, datetime.utcnow().isoformat()] + vals
        )
        conn.commit()

def read_with_join(limit=300):
    with connect() as conn:
        if not _has_column(conn, TABLE, "kode_organisasi"):
            st.error("Kolom 'kode_organisasi' belum tersedia pada tabel vWD. Coba refresh setelah migrasi.")
            return pd.read_sql_query(f"SELECT * FROM {TABLE} ORDER BY id DESC LIMIT ?", conn, params=[limit])

        return pd.read_sql_query(
            f"""
            SELECT
              v.id, v.kode_organisasi, v.created_at, v.kelompok_usia,
              v.laki_laki, v.perempuan, v.jk_tidak_terdata, v.total,
              io.kota_cakupan_cabang, io.hmhi_cabang
            FROM {TABLE} v
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = v.kode_organisasi
            ORDER BY v.id DESC
            LIMIT ?
            """,
            conn, params=[limit]
        )

def safe_int(val):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return 0
        return int(x)
    except Exception:
        return 0

# ---------- Startup ----------
migrate_add_kode_organisasi_if_needed()
init_db()

# ---------- UI ----------
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    st.caption("Isi jumlah Laki-Laki, Perempuan, dan Tidak Terdata untuk setiap kelompok usia. **Total** dihitung otomatis.")

    # 🔁 Sumber pilihan: hmhi_cabang → kode_organisasi
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom HMHI cabang). Harap isi dulu.")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="vwd::hmhi_select"
        )

    df_default = pd.DataFrame(0, index=AGE_GROUPS, columns=[k for k, _ in GENDER_COLS] + ["total"])
    df_default.index.name = "Kelompok Usia"

    col_config = {k: st.column_config.NumberColumn(label=lbl, min_value=0, step=1) for k, lbl in GENDER_COLS}
    col_config["total"] = st.column_config.NumberColumn(label=TOTAL_LABEL, min_value=0, step=1, disabled=True)

    with st.form("vwd_editor_form"):
        edited = st.data_editor(
            df_default,
            key="vwd::editor",
            column_config=col_config,
            use_container_width=True,
            num_rows="fixed",
        )
        # Hitung total per baris (Series → aman pakai fillna)
        try:
            edited["total"] = (
                pd.to_numeric(edited["laki_laki"], errors="coerce").fillna(0).astype(int) +
                pd.to_numeric(edited["perempuan"], errors="coerce").fillna(0).astype(int) +
                pd.to_numeric(edited["jk_tidak_terdata"], errors="coerce").fillna(0).astype(int)
            )
        except Exception:
            edited["total"] = 0

        save = st.form_submit_button("💾 Simpan Semua Baris")

    if save:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                for usia, row in edited.iterrows():
                    payload = {
                        "kelompok_usia": str(usia),
                        "laki_laki": safe_int(row.get("laki_laki", 0)),
                        "perempuan": safe_int(row.get("perempuan", 0)),
                        "jk_tidak_terdata": safe_int(row.get("jk_tidak_terdata", 0)),
                        "total": safe_int(row.get("total", 0)),
                    }
                    insert_row(payload, kode_organisasi)
                st.success(f"Semua baris berhasil disimpan untuk **{selected_hmhi}**.")

with tab_data:
    st.subheader("📄 Data Tersimpan")
    df_x = read_with_join(limit=300)

    # ===== Unduh Template Excel =====
    st.caption("Gunakan template berikut saat mengunggah data (kolom dan urutan baris Kelompok Usia dianjurkan).")
    tmpl_records = []
    for usia in TEMPLATE_AGE_ORDER:
        row = {
            "HMHI cabang": "",
            "Kelompok Usia": usia,
            "Laki-Laki": 0,
            "Perempuan": 0,
            "Jenis Kelamin Tidak Terdata": 0,
            "Total": 0,  # boleh pust, akan dihitung jika kosong
        }
        tmpl_records.append(row)
    tmpl_df = pd.DataFrame(tmpl_records, columns=TEMPLATE_COLUMNS)

    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_vwd.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="vwd::dl_template"
    )

    # ===== Tabel tampil =====
    if df_x.empty:
        st.info("Belum ada data.")
    else:
        order = ["hmhi_cabang", "kota_cakupan_cabang", "created_at", "kelompok_usia",
                 "laki_laki", "perempuan", "jk_tidak_terdata", "total"]
        order = [c for c in order if c in df_x.columns]

        view = df_x[order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "kelompok_usia": "Kelompok Usia",
            "laki_laki": "Laki-Laki",
            "perempuan": "Perempuan",
            "jk_tidak_terdata": "Jenis Kelamin Tidak Terdata",
            "total": "Total",
        })

        st.dataframe(view, use_container_width=True)

        # Unduh Excel (tampilan)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="DataVWD")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buffer.getvalue(),
            file_name="data_penyandang_vwd.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="vwd::download"
        )

    # ===== Unggah Excel =====
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan header persis seperti template",
        type=["xlsx"],
        key="vwd::uploader"
    )

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

        # alias -> key internal
        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        # Pratinjau
        st.caption("Pratinjau 20 baris pertama dari file yang diunggah:")
        st.dataframe(raw.head(20), use_container_width=True)

        if st.button("🚀 Proses & Simpan", type="primary", key="vwd::process"):
            hmhi_map, _ = load_hmhi_to_kode()
            results = []

            nrows = len(df_up)
            for i in range(nrows):
                try:
                    s = df_up.iloc[i]  # Series
                    hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                    if not hmhi:
                        raise ValueError("Kolom 'HMHI cabang' kosong.")
                    kode_organisasi = hmhi_map.get(hmhi)
                    if not kode_organisasi:
                        raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                    # Ambil angka
                    laki = safe_int(s.get("laki_laki"))
                    pr = safe_int(s.get("perempuan"))
                    nd = safe_int(s.get("jk_tidak_terdata"))
                    total = s.get("total")
                    total = safe_int(total)
                    # Jika total kosong/0 namun komponen ada, hitung
                    if total == 0 and (laki or pr or nd):
                        total = laki + pr + nd

                    kelompok = str((s.get("kelompok_usia") or "")).strip()
                    if not kelompok:
                        raise ValueError("Kolom 'Kelompok Usia' kosong.")

                    payload = {
                        "kelompok_usia": kelompok,
                        "laki_laki": max(laki, 0),
                        "perempuan": max(pr, 0),
                        "jk_tidak_terdata": max(nd, 0),
                        "total": max(total, 0),
                    }
                    insert_row(payload, kode_organisasi)
                    results.append({"Baris": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {kelompok}"})
                except Exception as e:
                    results.append({"Baris": i + 2, "Status": "GAGAL", "Keterangan": str(e)})

            res_df = pd.DataFrame(results)
            st.write("**Hasil unggah:**")
            st.dataframe(res_df, use_container_width=True)

            ok = (res_df["Status"] == "OK").sum()
            fail = (res_df["Status"] == "GAGAL").sum()
            if ok:
                st.success(f"Berhasil menyimpan {ok} baris.")
            if fail:
                st.error(f"Gagal menyimpan {fail} baris.")

            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                res_df.to_excel(w, index=False, sheet_name="Hasil")
            st.download_button(
                "📄 Unduh Log Hasil",
                log_buf.getvalue(),
                file_name="log_hasil_unggah_vwd.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="vwd::dl_log"
            )
