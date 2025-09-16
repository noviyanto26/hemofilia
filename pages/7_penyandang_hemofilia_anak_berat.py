import io
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# =========================
# Konfigurasi & Konstanta
# =========================
st.set_page_config(page_title="Penyandang Hemofilia Anak (Berat)", page_icon="🩸", layout="wide")
st.title("🩸 Penyandang Hemofilia Anak (Di bawah 18 Tahun) — Tingkat Berat (<1%)")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = str((BASE_DIR / "hemofilia.db").resolve())

# Samakan dengan nama tabel di skema DB kamu
TABLE = "penyandang_hemofilia_anak_berat"

# Baris input manual (tanpa Total)
ROW_LABELS = [
    "Hemofilia A Laki-laki",
    "Hemofilia A Perempuan",
    "Hemofilia B Laki-laki",
    "Hemofilia B Perempuan",
]
TOTAL_LABEL = "Total"

# ===== Template unggah & alias =====
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Kategori",
    "Berat (<1%)",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Kategori": "kategori",
    "Berat (<1%)": "berat",
}

# ================== Helper DB ==================
def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def _has_column(conn, table, col):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return any((row[1] == col) for row in cur.fetchall())

def _table_exists(conn, table):
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
    return cur.fetchone() is not None

def _create_final_schema(conn, as_new: bool=False):
    name = f"{TABLE}_new" if as_new else TABLE
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kode_organisasi TEXT,
            created_at TEXT NOT NULL,
            kategori TEXT,
            berat INTEGER,
            is_total_row TEXT,
            FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
        )
    """)

def migrate_if_needed():
    """
    Pastikan skema final tersedia dan kolom 'label' (jika ada) dimigrasikan menjadi 'kategori'.
    Skema final: id, kode_organisasi, created_at, kategori, berat, is_total_row
    """
    with connect() as conn:
        # Jika tabel belum ada → buat dengan skema final
        if not _table_exists(conn, TABLE):
            _create_final_schema(conn)
            conn.commit()
            return

        needed = ["kode_organisasi", "kategori", "berat", "is_total_row", "created_at"]
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({TABLE})")
        have = [r[1] for r in cur.fetchall()]
        if all(col in have for col in needed):
            return

        st.warning("Migrasi skema: menyesuaikan tabel Penyandang Hemofilia Anak (Berat)…")
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            # Buat tabel baru dengan skema final
            _create_final_schema(conn, as_new=True)

            # Ambil kolom lama
            cur.execute(f"PRAGMA table_info({TABLE})")
            old_cols = [r[1] for r in cur.fetchall()]

            # Susun SELECT untuk copy data lama → baru
            select_parts = []
            # id
            if "id" in old_cols:
                select_parts.append("id")
            else:
                select_parts.append("NULL AS id")
            # kode_organisasi
            select_parts.append("kode_organisasi" if "kode_organisasi" in old_cols else "NULL AS kode_organisasi")
            # created_at
            select_parts.append("created_at" if "created_at" in old_cols else "NULL AS created_at")
            # kategori (prioritas pakai 'kategori', fallback 'label')
            if "kategori" in old_cols:
                select_parts.append("kategori")
            elif "label" in old_cols:
                select_parts.append("label AS kategori")
            else:
                select_parts.append("NULL AS kategori")
            # berat
            select_parts.append("berat" if "berat" in old_cols else "NULL AS berat")
            # is_total_row
            select_parts.append("is_total_row" if "is_total_row" in old_cols else "NULL AS is_total_row")

            select_sql = ", ".join(select_parts)
            cur.execute(f"""
                INSERT INTO {TABLE}_new (id, kode_organisasi, created_at, kategori, berat, is_total_row)
                SELECT {select_sql} FROM {TABLE}
            """)
            # Tukar tabel
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
        _create_final_schema(conn, as_new=False)
        conn.commit()

# ================== Helpers ==================
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

def _to_nonneg_int(v):
    try:
        x = pd.to_numeric(v, errors="coerce")
        if pd.isna(x):
            return 0
        return max(int(x), 0)
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

def read_with_join(limit=500):
    with connect() as conn:
        if not _has_column(conn, TABLE, "kode_organisasi"):
            st.error("Kolom 'kode_organisasi' belum tersedia. Coba refresh setelah migrasi.")
            return pd.read_sql_query(f"SELECT * FROM {TABLE} ORDER BY id DESC LIMIT ?", conn, params=[limit])
        return pd.read_sql_query(
            f"""
            SELECT
              t.id, t.kode_organisasi, t.created_at, t.kategori,
              t.berat, t.is_total_row,
              io.hmhi_cabang, io.kota_cakupan_cabang
            FROM {TABLE} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            ORDER BY t.id DESC
            LIMIT ?
            """,
            conn, params=[limit]
        )

# ================== Startup ==================
migrate_if_needed()
init_db()

# ================== UI ==================
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
            key="anakb::hmhi_select"
        )

    # Editor input manual
    df_default = pd.DataFrame(0, index=ROW_LABELS, columns=["berat"])
    df_default.index.name = "Kategori"

    col_cfg = {"berat": st.column_config.NumberColumn("Berat (<1%)", min_value=0, step=1)}

    with st.form("anakb::form"):
        edited = st.data_editor(
            df_default,
            key="anakb::editor",
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
                total_val = 0
                rows_saved = 0
                # Simpan 4 baris utama
                for kategori in ROW_LABELS:
                    val = _to_nonneg_int(edited.loc[kategori, "berat"])
                    total_val += val
                    payload = {"kategori": kategori, "berat": val, "is_total_row": "0"}
                    # Simpan jika >0 agar tidak menambah baris kosong
                    if val > 0:
                        insert_row(payload, kode_organisasi)
                        rows_saved += 1
                # Tambah baris Total otomatis
                insert_row({"kategori": TOTAL_LABEL, "berat": total_val, "is_total_row": "1"}, kode_organisasi)
                rows_saved += 1
                st.success(f"{rows_saved} baris berhasil disimpan untuk **{selected_hmhi}**.")

with tab_data:
    st.subheader("📄 Data Tersimpan")
    df = read_with_join(limit=500)

    # ===== Unduh Template Excel =====
    st.caption("Gunakan template berikut saat mengunggah data (kolom harus sesuai).")
    tmpl_rows = [
        {"HMHI cabang": "", "Kategori": "Hemofilia A Laki-laki", "Berat (<1%)": 0},
        {"HMHI cabang": "", "Kategori": "Hemofilia A Perempuan", "Berat (<1%)": 0},
        {"HMHI cabang": "", "Kategori": "Hemofilia B Laki-laki", "Berat (<1%)": 0},
        {"HMHI cabang": "", "Kategori": "Hemofilia B Perempuan", "Berat (<1%)": 0},
        {"HMHI cabang": "", "Kategori": "Total", "Berat (<1%)": 0},
    ]
    tmpl_df = pd.DataFrame(tmpl_rows, columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel",
        buf_tmpl.getvalue(),
        file_name="template_anak_berat.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="anakb::dl_template"
    )

    # ===== Tabel tampilan =====
    if df.empty:
        st.info("Belum ada data.")
    else:
        cols_order = ["hmhi_cabang", "kota_cakupan_cabang", "created_at", "kategori", "berat"]
        cols_order = [c for c in cols_order if c in df.columns]
        view = df[cols_order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "kategori": "Kategori",
            "berat": "Berat (<1%)",
        })

        # Pastikan "Total" muncul di bawah dalam batch yang sama (Created At turun)
        if "Kategori" in view.columns:
            view["__is_total__"] = view["Kategori"].astype(str).str.strip().eq(TOTAL_LABEL).astype(int)
            view = view.sort_values(by=["Created At", "__is_total__"], ascending=[False, True]).drop(columns="__is_total__")

        st.dataframe(view, use_container_width=True)

        # Unduh Excel (tampilan)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="Anak_Berat")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buffer.getvalue(),
            file_name="anak_hemofilia_berat.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="anakb::download"
        )

    # ===== Unggah Excel =====
    st.markdown("### ⬆️ Unggah Excel")
    up = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan header persis seperti template",
        type=["xlsx"],
        key="anakb::uploader"
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

        if st.button("🚀 Proses & Simpan", type="primary", key="anakb::process"):
            hmhi_map, _ = load_hmhi_to_kode()
            results = []

            for i in range(len(df_up)):
                try:
                    s = df_up.iloc[i]
                    hmhi = str((s.get("hmhi_cabang_info") or "")).strip()
                    if not hmhi:
                        raise ValueError("Kolom 'HMHI cabang' kosong.")
                    kode_organisasi = hmhi_map.get(hmhi)
                    if not kode_organisasi:
                        raise ValueError(f"HMHI cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                    kategori = str((s.get("kategori") or "")).strip()
                    if not kategori:
                        raise ValueError("Kolom 'Kategori' kosong.")

                    berat_val = _to_nonneg_int(s.get("berat"))
                    payload = {
                        "kategori": kategori,
                        "berat": berat_val,
                        "is_total_row": "1" if kategori.strip().lower() == TOTAL_LABEL.lower() else "0",
                    }
                    insert_row(payload, kode_organisasi)
                    results.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {kategori}"})
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
                file_name="log_hasil_unggah_anak_berat.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="anakb::dl_log"
            )
