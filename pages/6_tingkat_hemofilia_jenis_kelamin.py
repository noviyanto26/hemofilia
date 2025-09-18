import os
import io
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# 👉 Tambahan impor Postgres
from sqlalchemy import text
from db import exec_sql, read_sql_df  # <— read_sql_df dipakai untuk SELECT ke Postgres

# =========================
# Konfigurasi & Konstanta
# =========================
st.set_page_config(page_title="Tingkat Hemofilia & Jenis Kelamin", page_icon="🩸", layout="wide")
st.title("🩸 Tingkat Hemofilia dan Jenis Kelamin")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = str((BASE_DIR / "hemofilia.db").resolve())

TABLE = "tingkat_hemofilia_jenis_kelamin"

SEVERITY_COLS = [
    ("ringan", "Ringan (>5%)"),
    ("sedang", "Sedang (1-5%)"),
    ("berat", "Berat (<1%)"),
    ("tidak_diketahui", "Tidak diketahui"),
]
TOTAL_COL = "total"

# ===== Template unggah & alias (untuk upload Excel) =====
TEMPLATE_COLUMNS = [
    "HMHI cabang",
    "Baris",
    "Ringan (>5%)",
    "Sedang (1-5%)",
    "Berat (<1%)",
    "Tidak diketahui",
    "Total",
]
ALIAS_TO_DB = {
    "HMHI cabang": "hmhi_cabang_info",
    "Baris": "label",
    "Ringan (>5%)": "ringan",
    "Sedang (1-5%)": "sedang",
    "Berat (<1%)": "berat",
    "Tidak diketahui": "tidak_diketahui",
    "Total": "total",
}

# 🆕 Template unggah khusus Penyandang Perempuan
FEMALE_TEMPLATE_COLUMNS = [
    "HMHI Cabang",
    "Jenis Hemofilia",
    "Carrier (>40%)",
    "Ringan (>5%)",
    "Sedang (1-5%)",
    "Berat (<1%)",
]
FEMALE_ALIAS = {
    "HMHI Cabang": "hmhi_cabang",
    "Jenis Hemofilia": "jenis_hemofilia",
    "Carrier (>40%)": "carrier",
    "Ringan (>5%)": "ringan",
    "Sedang (1-5%)": "sedang",
    "Berat (<1%)": "berat",
}

# =========================
# Utilitas Database (SQLite untuk bagian lama — tidak diubah)
# =========================
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

# =========================
# Migrasi Skema (tetap)
# =========================
def migrate_if_needed():
    with connect() as conn:
        if not _table_exists(conn, TABLE):
            return
        needed = ["kode_organisasi", "label", "ringan", "sedang", "berat", "tidak_diketahui", "total", "is_total_row", "created_at"]
        cur = conn.cursor()
        cur.execute(f"PRAGMA table_info({TABLE})")
        have = [r[1] for r in cur.fetchall()]
        if all(col in have for col in needed):
            return

        st.warning("Migrasi skema: menyesuaikan tabel Tingkat Hemofilia & Jenis Kelamin…")
        cur.execute("PRAGMA foreign_keys=OFF")
        try:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE}_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kode_organisasi TEXT,
                    created_at TEXT NOT NULL,
                    label TEXT,
                    ringan INTEGER,
                    sedang INTEGER,
                    berat INTEGER,
                    tidak_diketahui INTEGER,
                    total INTEGER,
                    is_total_row TEXT,
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
                label TEXT,
                ringan INTEGER,
                sedang INTEGER,
                berat INTEGER,
                tidak_diketahui INTEGER,
                total INTEGER,
                is_total_row TEXT,
                FOREIGN KEY (kode_organisasi) REFERENCES identitas_organisasi(kode_organisasi)
            )
        """)
        conn.commit()

# =========================
# Helper CRUD & Loader (lama)
# =========================
def load_hmhi_to_kode():
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
              t.id, t.kode_organisasi, t.created_at, t.label,
              t.ringan, t.sedang, t.berat, t.tidak_diketahui, t.total, t.is_total_row,
              io.kota_cakupan_cabang, io.hmhi_cabang
            FROM {TABLE} t
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
            ORDER BY t.id DESC
            LIMIT ?
            """,
            conn, params=[limit]
        )

# =========================
# Startup
# =========================
migrate_if_needed()
init_db()

# =========================
# Antarmuka
# =========================
tab_input, tab_data = st.tabs(["📝 Input", "📄 Data"])

with tab_input:
    # Pilihan organisasi (HMHI → kode)
    hmhi_map, hmhi_list = load_hmhi_to_kode()
    if not hmhi_list:
        st.warning("Belum ada data Identitas Organisasi (kolom HMHI cabang).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox(
            "Pilih HMHI Cabang (Provinsi)",
            options=hmhi_list,
            key="thjk::hmhi_select"
        )

    # ---------- Tabel 1: Penyandang Laki-laki ----------
    st.subheader("👨 Penyandang Laki-laki")
    df_lk = pd.DataFrame(
        0,
        index=["Hemofilia A laki-laki", "Hemofilia B laki-laki", "Total Laki-laki"],
        columns=[c for c, _ in SEVERITY_COLS] + [TOTAL_COL]
    )
    df_lk.index.name = "Baris"

    col_cfg = {c: st.column_config.NumberColumn(lbl, min_value=0, step=1) for c, lbl in SEVERITY_COLS}
    col_cfg[TOTAL_COL] = st.column_config.NumberColumn("Total", min_value=0, step=1, disabled=True)

    with st.form("thjk::form_lk"):
        ed_lk = st.data_editor(
            df_lk,
            key="thjk::editor_lk",
            column_config=col_cfg,
            use_container_width=True,
            num_rows="fixed",
        )
        for row_label in ["Hemofilia A laki-laki", "Hemofilia B laki-laki"]:
            ed_lk.loc[row_label, TOTAL_COL] = sum(_to_nonneg_int(ed_lk.loc[row_label, c]) for c, _ in SEVERITY_COLS)
        for c, _ in SEVERITY_COLS:
            ed_lk.loc["Total Laki-laki", c] = _to_nonneg_int(ed_lk.loc["Hemofilia A laki-laki", c]) + _to_nonneg_int(ed_lk.loc["Hemofilia B laki-laki", c])
        ed_lk.loc["Total Laki-laki", TOTAL_COL] = sum(_to_nonneg_int(ed_lk.loc["Total Laki-laki", c]) for c, _ in SEVERITY_COLS)

        submit_lk = st.form_submit_button("💾 Simpan Data Laki-laki")

    if submit_lk:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                for label in ed_lk.index.tolist():
                    payload = {
                        "label": label,
                        "ringan": _to_nonneg_int(ed_lk.loc[label, "ringan"]),
                        "sedang": _to_nonneg_int(ed_lk.loc[label, "sedang"]),
                        "berat": _to_nonneg_int(ed_lk.loc[label, "berat"]),
                        "tidak_diketahui": _to_nonneg_int(ed_lk.loc[label, "tidak_diketahui"]),
                        "total": _to_nonneg_int(ed_lk.loc[label, "total"]),
                        "is_total_row": "1" if label.startswith("Total ") else "0",
                    }
                    insert_row(payload, kode_organisasi)
                st.success(f"Data laki-laki tersimpan untuk **{selected_hmhi}**.")

    st.divider()

    # ---------- Tabel 2: Penyandang Perempuan ----------
    st.subheader("👩 Penyandang Perempuan")

    FEMALE_ROWS = ["Hemofilia A perempuan", "Hemofilia B perempuan"]
    FEMALE_COLS = [
        ("carrier", "Carrier (>40%)"),
        ("ringan", "Ringan (>5%)"),
        ("sedang", "Sedang (1-5%)"),
        ("berat",  "Berat (<1%)"),
    ]
    df_pr_new = pd.DataFrame(0, index=FEMALE_ROWS, columns=[c for c, _ in FEMALE_COLS])
    df_pr_new.index.name = "Jenis Hemofilia"
    col_cfg_pr = {c: st.column_config.NumberColumn(lbl, min_value=0, step=1) for c, lbl in FEMALE_COLS}

    with st.form("thjk::form_pr_new"):
        ed_pr_new = st.data_editor(
            df_pr_new,
            key="thjk::editor_pr_new",
            column_config=col_cfg_pr,
            use_container_width=True,
            num_rows="fixed",
        )
        submit_pr_new = st.form_submit_button("💾 Simpan Data Penyandang Perempuan")

    if submit_pr_new:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            try:
                for jh in ed_pr_new.index.tolist():
                    carrier = _to_nonneg_int(ed_pr_new.loc[jh, "carrier"])
                    ringan  = _to_nonneg_int(ed_pr_new.loc[jh, "ringan"])
                    sedang  = _to_nonneg_int(ed_pr_new.loc[jh, "sedang"])
                    berat   = _to_nonneg_int(ed_pr_new.loc[jh, "berat"])
                    exec_sql(
                        text("""
                            INSERT INTO public.hemofilia_perempuan
                                (jenis_hemofilia, carrier, ringan, sedang, berat)
                            VALUES
                                (:jenis_hemofilia, :carrier, :ringan, :sedang, :berat)
                        """),
                        {
                            "jenis_hemofilia": jh,
                            "carrier": carrier,
                            "ringan": ringan,
                            "sedang": sedang,
                            "berat": berat,
                        }
                    )
                st.success("Data perempuan berhasil disimpan ke Postgres (public.hemofilia_perempuan).")
            except Exception as e:
                st.error(f"Gagal menyimpan data perempuan ke Postgres: {e}")

# =========================
# Data Tersimpan & Unggah Excel
# =========================
with tab_data:
    st.subheader("📄 Data Tersimpan")

    # ----- 1) Data Tingkat Hemofilia & JK (SQLite) -----
    df = read_with_join(limit=500)

    st.caption("Gunakan template berikut saat mengunggah data (kolom harus sesuai).")
    template_rows = [
        {"HMHI cabang": "", "Baris": "Hemofilia A laki-laki", "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
        {"HMHI cabang": "", "Baris": "Hemofilia B laki-laki", "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
        {"HMHI cabang": "", "Baris": "Total Laki-laki",       "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
        {"HMHI cabang": "", "Baris": "Hemofilia A perempuan",  "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
        {"HMHI cabang": "", "Baris": "Hemofilia B perempuan",  "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
        {"HMHI cabang": "", "Baris": "Total Perempuan",        "Ringan (>5%)": 0, "Sedang (1-5%)": 0, "Berat (<1%)": 0, "Tidak diketahui": 0, "Total": 0},
    ]
    tmpl_df = pd.DataFrame(template_rows, columns=TEMPLATE_COLUMNS)
    buf_tmpl = io.BytesIO()
    with pd.ExcelWriter(buf_tmpl, engine="xlsxwriter") as w:
        tmpl_df.to_excel(w, index=False, sheet_name="Template")
    st.download_button(
        "📥 Unduh Template Excel (Tingkat Hemofilia & JK)",
        buf_tmpl.getvalue(),
        file_name="template_tingkat_hemofilia_jenis_kelamin.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="thjk::dl_template"
    )

    # Tabel tampil (JK)
    if df.empty:
        st.info("Belum ada data JK.")
    else:
        order = ["hmhi_cabang", "kota_cakupan_cabang", "created_at", "label",
                 "ringan", "sedang", "berat", "tidak_diketahui", "total"]
        order = [c for c in order if c in df.columns]
        view = df[order].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "label": "Baris",
            "ringan": "Ringan (>5%)",
            "sedang": "Sedang (1-5%)",
            "berat": "Berat (<1%)",
            "tidak_diketahui": "Tidak diketahui",
            "total": "Total",
        })
        st.dataframe(view, use_container_width=True)

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="xlsxwriter") as w:
            view.to_excel(w, index=False, sheet_name="TingkatHemofiliaJK")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan JK)",
            buffer.getvalue(),
            file_name="tingkat_hemofilia_jenis_kelamin.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="thjk::download"
        )

    st.divider()

    # ----- 2) 🆕 Data Penyandang Perempuan (Postgres) -----
    st.subheader("👩 Data Tersimpan — Penyandang Perempuan (Postgres)")

    # Query dengan string biasa (bukan TextClause) agar cocok dengan read_sql_df
    sql_female = """
        SELECT
          hp.id,
          hp.created_at,
          hp.kode_organisasi,
          io.hmhi_cabang,
          io.kota_cakupan_cabang,
          hp.jenis_hemofilia,
          hp.carrier,
          hp.ringan,
          hp.sedang,
          hp.berat
        FROM public.hemofilia_perempuan hp
        LEFT JOIN public.identitas_organisasi io
          ON io.kode_organisasi = hp.kode_organisasi
        ORDER BY hp.id DESC
        LIMIT :lim
    """
    df_female = read_sql_df(sql_female, params={"lim": 500}) or pd.DataFrame()

    if df_female.empty:
        st.info("Belum ada data Penyandang Perempuan.")
    else:
        order_f = ["hmhi_cabang", "kota_cakupan_cabang", "created_at",
                   "jenis_hemofilia", "carrier", "ringan", "sedang", "berat"]
        order_f = [c for c in order_f if c in df_female.columns]
        view_f = df_female[order_f].rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "created_at": "Created At",
            "jenis_hemofilia": "Jenis Hemofilia",
            "carrier": "Carrier (>40%)",
            "ringan": "Ringan (>5%)",
            "sedang": "Sedang (1-5%)",
            "berat": "Berat (<1%)",
        })
        st.dataframe(view_f, use_container_width=True)

        buf_f = io.BytesIO()
        with pd.ExcelWriter(buf_f, engine="xlsxwriter") as w:
            view_f.to_excel(w, index=False, sheet_name="PenyandangPerempuan")
        st.download_button(
            "⬇️ Unduh Excel (Penyandang Perempuan)",
            buf_f.getvalue(),
            file_name="penyandang_perempuan.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="thjk::download_female"
        )

    # ====== Unggah Excel ======
    st.markdown("### ⬆️ Unggah Excel")
    st.caption("Anda bisa mengunggah salah satu: (1) Template Tingkat Hemofilia & JK, atau (2) Template Penyandang Perempuan.")
    up = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan header persis seperti template yang diunduh",
        type=["xlsx"],
        key="thjk::uploader"
    )

    if up is not None:
        try:
            raw = pd.read_excel(up)
            raw.columns = [str(c).strip() for c in raw.columns]
        except Exception as e:
            st.error(f"Gagal membaca file: {e}")
            st.stop()

        cols = set(raw.columns)
        is_general = set(TEMPLATE_COLUMNS).issubset(cols)
        is_female  = set(FEMALE_TEMPLATE_COLUMNS).issubset(cols)

        if not is_general and not is_female:
            st.error(
                "Header kolom tidak sesuai. "
                "Unggah salah satu template berikut:\n"
                f"- Template umum: {', '.join(TEMPLATE_COLUMNS)}\n"
                f"- Template perempuan: {', '.join(FEMALE_TEMPLATE_COLUMNS)}"
            )
            st.stop()

        # ===== Jalur 1: Template Umum =====
        if is_general:
            df_up = raw.rename(columns=ALIAS_TO_DB).copy()
            st.caption("Pratinjau 20 baris pertama dari file yang diunggah:")
            st.dataframe(raw.head(20), use_container_width=True)

            if st.button("🚀 Proses & Simpan (Template Umum)", type="primary", key="thjk::process_general"):
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

                        label = str((s.get("label") or "")).strip()
                        if not label:
                            raise ValueError("Kolom 'Baris' kosong.")

                        ringan = _to_nonneg_int(s.get("ringan"))
                        sedang = _to_nonneg_int(s.get("sedang"))
                        berat = _to_nonneg_int(s.get("berat"))
                        td = _to_nonneg_int(s.get("tidak_diketahui"))
                        total = _to_nonneg_int(s.get("total"))
                        if total == 0 and (ringan or sedang or berat or td):
                            total = ringan + sedang + berat + td

                        payload = {
                            "label": label,
                            "ringan": ringan,
                            "sedang": sedang,
                            "berat": berat,
                            "tidak_diketahui": td,
                            "total": total,
                            "is_total_row": "1" if label.strip().lower().startswith("total ") else "0",
                        }
                        insert_row(payload, kode_organisasi)
                        results.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {label}"})
                    except Exception as e:
                        results.append({"Baris Excel": i + 2, "Status": "GAGAL", "Keterangan": str(e)})

                res_df = pd.DataFrame(results)
                st.write("**Hasil unggah (Template Umum):**")
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
                    file_name="log_hasil_unggah_tingkat_jk.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="thjk::dl_log_general"
                )

        # ===== Jalur 2: Template Penyandang Perempuan =====
        if is_female:
            df_fp = raw.rename(columns=FEMALE_ALIAS).copy()
            st.caption("Pratinjau 20 baris pertama dari file yang diunggah (Penyandang Perempuan):")
            st.dataframe(raw.head(20), use_container_width=True)

            if st.button("🚀 Proses & Simpan (Penyandang Perempuan)", type="primary", key="thjk::process_female"):
                hmhi_map, _ = load_hmhi_to_kode()
                results_f = []
                for i in range(len(df_fp)):
                    try:
                        s = df_fp.iloc[i]
                        hmhi = str((s.get("hmhi_cabang") or "")).strip()
                        if not hmhi:
                            raise ValueError("Kolom 'HMHI Cabang' kosong.")
                        kode_organisasi = hmhi_map.get(hmhi)
                        if not kode_organisasi:
                            raise ValueError(f"HMHI Cabang '{hmhi}' tidak ditemukan di identitas_organisasi.")

                        jenis = str((s.get("jenis_hemofilia") or "")).strip()
                        if jenis not in ["Hemofilia A perempuan", "Hemofilia B perempuan"]:
                            raise ValueError("Kolom 'Jenis Hemofilia' harus 'Hemofilia A perempuan' atau 'Hemofilia B perempuan'.")

                        carrier = _to_nonneg_int(s.get("carrier"))
                        ringan  = _to_nonneg_int(s.get("ringan"))
                        sedang  = _to_nonneg_int(s.get("sedang"))
                        berat   = _to_nonneg_int(s.get("berat"))

                        try:
                            exec_sql(
                                text("""
                                    INSERT INTO public.hemofilia_perempuan
                                        (kode_organisasi, jenis_hemofilia, carrier, ringan, sedang, berat)
                                    VALUES
                                        (:kode_organisasi, :jenis_hemofilia, :carrier, :ringan, :sedang, :berat)
                                """),
                                {
                                    "kode_organisasi": kode_organisasi,
                                    "jenis_hemofilia": jenis,
                                    "carrier": carrier,
                                    "ringan": ringan,
                                    "sedang": sedang,
                                    "berat": berat,
                                }
                            )
                        except Exception:
                            exec_sql(
                                text("""
                                    INSERT INTO public.hemofilia_perempuan
                                        (jenis_hemofilia, carrier, ringan, sedang, berat)
                                    VALUES
                                        (:jenis_hemofilia, :carrier, :ringan, :sedang, :berat)
                                """),
                                {
                                    "jenis_hemofilia": jenis,
                                    "carrier": carrier,
                                    "ringan": ringan,
                                    "sedang": sedang,
                                    "berat": berat,
                                }
                            )

                        results_f.append({"Baris Excel": i + 2, "Status": "OK", "Keterangan": f"Simpan → {hmhi} / {jenis}"})
                    except Exception as e:
                        results_f.append({"Baris Excel": i + 2, "Status": "GAGAL", "Keterangan": str(e)})

                res_df_f = pd.DataFrame(results_f)
                st.write("**Hasil unggah (Penyandang Perempuan):**")
                st.dataframe(res_df_f, use_container_width=True)

                ok = (res_df_f["Status"] == "OK").sum()
                fail = (res_df_f["Status"] == "GAGAL").sum()
                if ok:
                    st.success(f"Berhasil menyimpan {ok} baris.")
                if fail:
                    st.error(f"Gagal menyimpan {fail} baris.")

                log_buf_f = io.BytesIO()
                with pd.ExcelWriter(log_buf_f, engine="xlsxwriter") as w:
                    res_df_f.to_excel(w, index=False, sheet_name="Hasil")
                st.download_button(
                    "📄 Unduh Log Hasil (Penyandang Perempuan)",
                    log_buf_f.getvalue(),
                    file_name="log_hasil_unggah_penyandang_perempuan.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key="thjk::dl_log_female"
                )
