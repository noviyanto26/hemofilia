import streamlit as st
import sqlite3
import pandas as pd
from datetime import datetime
import io

st.set_page_config(page_title="Jumlah Penyandang VWD Berat (Tipe 3)", page_icon="🩸", layout="wide")
st.title("🩸 Jumlah Penyandang VWD Berat (Tipe 3)")

DB_PATH = "hemofilia.db"
TABLE = "vwd_berat_jumlah"

# Baris input (tanpa Total) — untuk input manual
ROW_LABELS = [
    "Penyandang VWD Laki-Laki",
    "Penyandang VWD Perempuan",
    "Penyandang VWD Tanpa Data Jenis Kelamin",
]
TOTAL_LABEL = "Total"

COLS = [
    ("jumlah", "Jumlah Penyandang"),
    ("jumlah_medis", "Jumlah Penyandang VWD Berat yang Menerima Penanganan Medis"),
]

# ===== Template unggah & alias =====
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Baris",  # contoh: tiga baris di atas + "Total" (opsional)
    "Jumlah Penyandang",
    "Jumlah Penyandang VWD Berat yang Menerima Penanganan Medis",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Baris": "label",
    "Jumlah Penyandang": "jumlah",
    "Jumlah Penyandang VWD Berat yang Menerima Penanganan Medis": "jumlah_medis",
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

def migrate_if_needed():
    """Pastikan skema final tersedia (termasuk kode_organisasi & kolom angka)."""
    with connect() as conn:
        if not _table_exists(conn, TABLE):
            return
        needed = ["kode_organisasi", "label", "jumlah", "jumlah_medis", "is_total_row"]
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({TABLE})")
        have = [r[1] for r in cur.fetchall()]
        if all(col in have for col in needed):
            return

        st.warning("Migrasi skema: menyesuaikan tabel VWD Berat (Jumlah)…")
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT,
                    created_at TEXT NOT NULL,
                    label TEXT,
                    jumlah INTEGER,
                    jumlah_medis INTEGER,
                    is_total_row TEXT,
                    FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
                )
            """)
            # Salin kolom yang ada saja
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
                label TEXT,
                jumlah INTEGER,
                jumlah_medis INTEGER,
                is_total_row TEXT,
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

def safe_int(val):
    try:
        x = pd.to_numeric(val, errors="coerce")
        if pd.isna(x):
            return 0
        return int(x)
    except Exception:
        return 0

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
    with connect() as conn:
        if not _has_column(conn, TABLE, "kode_organisasi"):
            st.error("Kolom 'kode_organisasi' belum tersedia. Coba refresh setelah migrasi.")
            return pd.read_sql_query(f"SELECT * FROM {TABLE} ORDER BY id DESC LIMIT ?", conn, params=[limit])

        return pd.read_sql_query(
            f"""
            SELECT
              t.id, t.kode_organisasi, t.created_at, t.label,
              t.jumlah, t.jumlah_medis, t.is_total_row,
              io.hmhi_cabang, io.kota_cakupan_cabang
            FROM {TABLE} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            ORDER BY t.id DESC
            LIMIT ?
            """, conn, params=[limit]
        )

# ======================== Startup ========================
migrate_if_needed()
init_db()

# ======================== UI ========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # 🔁 Sumber pilihan: HMHI cabang → kode_organisasi
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="vwd3::hmhi_select"
        )

    # Template editor (tanpa baris Total)
    df_default = pd.DataFrame(0, index=ROW_LABELS, columns=[c for c, _ in COLS])
    df_default.index.name = "Baris"

    col_cfg = {
        "jumlah": st.column_config.NumberColumn("Jumlah Penyandang", min_value=0, step=1),
        "jumlah_medis": st.column_config.NumberColumn(
            "Jumlah Penyandang VWD Berat yang Menerima Penanganan Medis", min_value=0, step=1
        ),
    }

    with st.form("vwd3::form"):
        edited = st.data_editor(
            df_default,
            key="vwd3::editor",
            column_config=col_cfg,
            use_container_width=True,
            num_rows="fixed",
        )
        submitted = st.form_submit_button("💾 Simpan")

    if submitted:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                # Simpan 3 baris
                total_jumlah = 0
                total_medis = 0
                for label in ROW_LABELS:
                    j = safe_int(edited.loc[label, "jumlah"])
                    m = safe_int(edited.loc[label, "jumlah_medis"])
                    total_jumlah += j
                    total_medis += m
                    payload = {
                        "label": label,
                        "jumlah": j,
                        "jumlah_medis": m,
                        "is_total_row": "0",
                    }
                    insert_row(payload, kode_organisasi)

                # Simpan baris Total otomatis
                payload = {
                    "label": TOTAL_LABEL,
                    "jumlah": total_jumlah,
                    "jumlah_medis": total_medis,
                    "is_total_row": "1",
                }
                insert_row(payload, kode_organisasi)

                st.success(f"Data berhasil disimpan untuk **{selected_hmhi}**.")

# ======================== Data Tersimpan & Unggah Excel ========================
with tab_data:
    st.subheader("📄 Data Tersimpan")
    df = read_with_kota(limit=500)

    # ===== Unduh Template Excel =====
    st.caption("Gunakan template berikut saat mengunggah data (kolom harus sesuai).")
    tmpl_rows = [
        {"HMHI cabang": "", "Baris": "Penyandang VWD Laki-Laki", "Jumlah Penyandang": 0, "Jumlah Penyandang VWD Berat yang Menerima Penanganan Medis": 0},
        {"HMHI cabang": "", "Baris": "Penyandang VWD Perempuan", "Jumlah Penyandang": 0, "Jumlah Penyandang VWD Berat yang Menerima Penanganan Medis": 0},
        {"HMHI cabang": "", "Baris": "Penyandang VWD Tanpa Data Jenis Kelamin", "Jumlah Penyandang": 0, "Jumlah Penyandang VWD Berat yang Menerima Penanganan Medis": 0},
        {"HMHI cabang": "", "Baris": "Total", "Jumlah Penyandang": 0, "Jumlah Penyandang VWD Berat yang Menerima Penanganan Medis": 0},  # opsional, bisa 0
    ]
    tmpl_df = pd.DataFrame(tmpl_rows, columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_vwd_berat_jumlah.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="vwd3::dl_template"
    )

    # ===== Tabel tampilan =====
    if df.empty:
        st.info("Belum ada data.")
    else:
        # Tampilkan tanpa kode_organisasi/is_total_row
        cols_order = ["hmhi_cabang", "kota_cakupan_cabang", "created_at", "label", "jumlah", "jumlah_medis"]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "label": "Baris",
            "jumlah": "Jumlah Penyandang",
            "jumlah_medis": "Jumlah Penyandang VWD Berat yang Menerima Penanganan Medis",
        })

        # Posisikan baris "Total" di paling bawah per entri (yang terbaru paling atas)
        if "Baris" in view.columns:
            view["__is_total__"] = view["Baris"].astype(str).str.strip().eq(TOTAL_LABEL).astype(int)
            view = view.sort_values(by=["Created At", "__is_total__"], ascending=[False, True]).drop(columns="__is_total__")

        st.dataframe(view, use_container_width=True)

        # Unduh Excel (tampilan)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="VWD_Berat_Jumlah")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buffer.getvalue(),
            file_name="jumlah_penyandang_vwd_berat.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="vwd3::download"
        )

    # ===== Unggah Excel =====
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan header persis seperti template",
        type=["xlsx"],
        key="vwd3::uploader"
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

        df_up = raw.rename(columns=ALIAS_TO_DB).copy()

        # Pratinjau
        st.caption("Pratinjau 20 baris pertama dari file yang diunggah:")
        st.dataframe(raw.head(20), use_container_width=True)

        if st.button("🚀 Proses & Simpan", type="primary", key="vwd3::process"):
            hmhi_map, _ = load_hmhi_to_kode()
            results = []

            for i in range(len(df_up)):
                try:
                    s = df_up.iloc[i]  # pandas.Series
                    hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                    if not hmhi:
                        raise ValueError("Kolom 'HMHI cabang' kosong.")
                    kode_organisasi = hmhi_map.get(hmhi)
                    if not kode_organisasi:
                        raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                    label = str((s.get("label") or "")).strip()
                    if not label:
                        raise ValueError("Kolom 'Baris' kosong.")

                    jml = safe_int(s.get("jumlah"))
                    jml_medis = safe_int(s.get("jumlah_medis"))
                    is_total = "1" if label.strip().lower() == TOTAL_LABEL.lower() else "0"

                    payload = {
                        "label": label,
                        "jumlah": max(jml, 0),
                        "jumlah_medis": max(jml_medis, 0),
                        "is_total_row": is_total,
                    }
                    insert_row(payload, kode_organisasi)
                    results.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {label}"})
                except Exception as e:
                    results.append({"Baris Excel": i + 2, "Status": "GAGAL", "Keterangan": str(e)})

            res_df = pd.DataFrame(results)
            st.write("**Hasil unggah:**")
            st.dataframe(res_df, use_container_width=True)

            ok = (res_df["Status"] == "OK").sum()
            fail = (res_df["Status"] == "GAGAL").sum()
            if ok:
                st.success(f"Berhasil menyimpan {ok} baris.")
            if fail:
                st.error(f"Gagal menyimpan {fail} baris.")

            # Unduh log hasil
            log_buf = io.BytesIO()
            with pd.ExcelWriter(log_buf, engine="xlsxwriter") as w:
                res_df.to_excel(w, index=False, sheet_name="Hasil")
            st.download_button(
                "📄 Unduh Log Hasil",
                log_buf.getvalue(),
                file_name="log_hasil_unggah_vwd_berat_jumlah.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="vwd3::dl_log"
            )
