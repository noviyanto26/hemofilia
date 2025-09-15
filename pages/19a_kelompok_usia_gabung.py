# -*- coding: utf-8 -*-
import io
import pandas as pd
import streamlit as st
from sqlalchemy import text

# ====== Koneksi via util db.py (Postgres-ready dengan fallback SQLite) ======
# db.py minimal menyediakan:
# - get_engine(): sqlalchemy Engine
# - exec_sql(sql: str, params: dict|None)
from db import get_engine, exec_sql

PAGE_TITLE = "🧮 Rekap Gabungan Kelompok Usia (Rebuild)"
SRC_TABLE = "kelompok_usia"
DST_TABLE = "kelompok_usia_gabung"
ORG_TABLE = "identitas_organisasi"   # untuk ambil HMHI Cabang (join via kode_organisasi)

st.set_page_config(page_title="Rekap Gabungan Kelompok Usia", page_icon="🧮", layout="wide")
st.title(PAGE_TITLE)

# ---------- fetch_df lokal (tanpa mengubah db.py) ----------
def fetch_df(sql: str, params: dict | None = None) -> pd.DataFrame:
    eng = get_engine()
    with eng.connect() as conn:
        return pd.read_sql_query(text(sql), conn, params=params or {})

# ---------- Helper Dialect ----------
def _is_postgres() -> bool:
    try:
        eng = get_engine()
        return (eng.dialect.name or "").lower() in ("postgresql", "postgres")
    except Exception:
        return False

# ---------- DDL: create table if not exists ----------
def ensure_dst_table():
    """
    Buat tabel gabungan jika belum ada (Postgres/SQLite aware).
    """
    if _is_postgres():
        # Postgres
        exec_sql(f"""
        CREATE TABLE IF NOT EXISTS {DST_TABLE} (
            id BIGSERIAL PRIMARY KEY,
            kode_organisasi VARCHAR(255),
            created_at TIMESTAMPTZ,
            kelompok_usia VARCHAR(255),
            hemo_a INTEGER DEFAULT 0,
            hemo_b INTEGER DEFAULT 0,
            hemo_tipe_lain INTEGER DEFAULT 0,
            vwd_tipe1 INTEGER DEFAULT 0,
            vwd_tipe2 INTEGER DEFAULT 0
        );
        """)
        exec_sql(f"CREATE INDEX IF NOT EXISTS idx_{DST_TABLE}_kode_usia ON {DST_TABLE}(kode_organisasi, kelompok_usia);")
    else:
        # SQLite
        exec_sql(f"""
        CREATE TABLE IF NOT EXISTS {DST_TABLE} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kode_organisasi TEXT,
            created_at TEXT,
            kelompok_usia TEXT,
            hemo_a INTEGER DEFAULT 0,
            hemo_b INTEGER DEFAULT 0,
            hemo_tipe_lain INTEGER DEFAULT 0,
            vwd_tipe1 INTEGER DEFAULT 0,
            vwd_tipe2 INTEGER DEFAULT 0
        );
        """)
        exec_sql(f"CREATE INDEX IF NOT EXISTS idx_{DST_TABLE}_kode_usia ON {DST_TABLE}(kode_organisasi, kelompok_usia);")

# ---------- REBUILD: truncate + insert (tanpa kolom id) ----------
def rebuild_gabungan():
    """
    Rebuild penuh dari sumber 'kelompok_usia' ke 'kelompok_usia_gabung'.
    - Kosongkan tabel target + reset sequence/autoincrement
    - Insert agregasi tanpa menyertakan kolom 'id'
    """
    ensure_dst_table()

    if _is_postgres():
        # Kosongkan dan reset identity (hindari UniqueViolation)
        exec_sql(f"TRUNCATE TABLE {DST_TABLE} RESTART IDENTITY;")
    else:
        # SQLite: hapus isi + reset autoincrement
        exec_sql(f"DELETE FROM {DST_TABLE};")
        try:
            exec_sql(f"DELETE FROM sqlite_sequence WHERE name = '{DST_TABLE}';")
        except Exception:
            pass  # sqlite_sequence bisa belum ada

    # Insert-agregasi dari sumber
    exec_sql(f"""
        INSERT INTO {DST_TABLE} (
            kode_organisasi, created_at, kelompok_usia,
            hemo_a, hemo_b, hemo_tipe_lain, vwd_tipe1, vwd_tipe2
        )
        SELECT
            ku.kode_organisasi,
            ku.created_at,
            ku.kelompok_usia,
            COALESCE(ku.ha_ringan,0) + COALESCE(ku.ha_sedang,0) + COALESCE(ku.ha_berat,0) AS hemo_a,
            COALESCE(ku.hb_ringan,0) + COALESCE(ku.hb_sedang,0) + COALESCE(ku.hb_berat,0) AS hemo_b,
            COALESCE(ku.hemo_tipe_lain,0) AS hemo_tipe_lain,
            COALESCE(ku.vwd_tipe1,0)      AS vwd_tipe1,
            COALESCE(ku.vwd_tipe2,0)      AS vwd_tipe2
        FROM {SRC_TABLE} ku
    """)

    # (Opsional) sinkronkan sequence jika dulu pernah menyisipkan id manual
    if _is_postgres():
        exec_sql(f"""
        SELECT setval(
            pg_get_serial_sequence('{DST_TABLE}','id'),
            COALESCE((SELECT MAX(id) FROM {DST_TABLE}), 0)
        );
        """)

# ---------- READ: tampilan gabungan + join HMHI Cabang ----------
def read_joined_df():
    """
    Tampilkan data gabungan + HMHI Cabang.
    Nama kolom HMHI di identitas_organisasi bisa bervariasi; dicoba beberapa kemungkinan.
    """
    try_cols = ["hmhi_cabang", '"HMHI Cabang"', '"HMHI_cabang"', "HMHI_cabang", "HMHI_CABANG"]

    hmhi_col_expr = None
    for c in try_cols:
        try:
            _ = fetch_df(f"SELECT {c} FROM {ORG_TABLE} LIMIT 0;")  # uji keberadaan kolom
            hmhi_col_expr = c
            break
        except Exception:
            continue

    if hmhi_col_expr is None:
        hmhi_select = "NULL AS hmhi_cabang"
        order_expr = "hmhi_cabang"
    else:
        hmhi_select = f"{hmhi_col_expr} AS hmhi_cabang"
        # untuk ORDER BY, gunakan alias yang sama setelah SELECT agar portable
        order_expr = "hmhi_cabang"

    sql = f"""
        SELECT
            g.id,
            g.kode_organisasi,
            g.created_at,
            o.{hmhi_select},
            g.kelompok_usia,
            g.hemo_a,
            g.hemo_b,
            g.hemo_tipe_lain,
            g.vwd_tipe1,
            g.vwd_tipe2
        FROM {DST_TABLE} g
        LEFT JOIN {ORG_TABLE} o
          ON o.kode_organisasi = g.kode_organisasi
        ORDER BY {order_expr} NULLS LAST, g.kelompok_usia
    """
    try:
        df = fetch_df(sql)
    except Exception:
        # SQLite tidak dukung "NULLS LAST"
        sql_sqlite = sql.replace(" NULLS LAST", "")
        df = fetch_df(sql_sqlite)

    view_df = df.copy()
    if "hmhi_cabang" in view_df.columns:
        view_df.rename(columns={"hmhi_cabang": "HMHI Cabang"}, inplace=True)
    view_df.rename(columns={
        "kelompok_usia": "Kelompok Usia",
        "hemo_a": "Hemofilia A",
        "hemo_b": "Hemofilia B",
        "hemo_tipe_lain": "Hemofilia Tipe Lain",
        "vwd_tipe1": "vWD - Tipe 1",
        "vwd_tipe2": "vWD - Tipe 2",
    }, inplace=True)

    cols_order = [c for c in [
        "HMHI Cabang", "Kelompok Usia", "Hemofilia A", "Hemofilia B",
        "Hemofilia Tipe Lain", "vWD - Tipe 1", "vWD - Tipe 2"
    ] if c in view_df.columns]

    view_df = view_df[cols_order] if cols_order else view_df
    return df, view_df

# ---------- UI ----------
with st.expander("ℹ️ Keterangan", expanded=True):
    st.markdown("""
- **Rebuild Rekap** akan mengosongkan tabel gabungan dan mengisinya ulang dari tabel sumber **kelompok_usia**.
- Konflik primary key `id` dihindari karena **tidak menyalin `id`** dari sumber.
- Tampilan sudah **join HMHI Cabang** dari `identitas_organisasi`.
- Bisa **unduh Excel** hasil rekap.
""")

c1, c2 = st.columns([1, 3])
with c1:
    if st.button("🔨 Rebuild Rekap", type="primary", use_container_width=True):
        try:
            rebuild_gabungan()
            st.success("Rekap berhasil dibangun ulang.")
        except Exception as e:
            st.error(f"Gagal rebuild: {e}")

with c2:
    st.info(f"Sumber: `{SRC_TABLE}` → Target: `{DST_TABLE}`", icon="📦")

st.divider()

raw_df, view_df = read_joined_df()

st.subheader("📊 Rekap Gabungan (Tampilan)")
st.dataframe(view_df, use_container_width=True, hide_index=True)

st.subheader("⬇️ Unduh Rekap (Excel)")
with io.BytesIO() as buffer:
    with pd.ExcelWriter(buffer, engine="xlsxwriter") as writer:
        view_df.to_excel(writer, index=False, sheet_name="rekap_tampilan")
        raw_df.to_excel(writer, index=False, sheet_name="rekap_raw")
    data = buffer.getvalue()
st.download_button(
    label="Download Excel Rekap",
    data=data,
    file_name="rekap_kelompok_usia_gabung.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    use_container_width=True
)

st.caption("Kolom `id`, `kode_organisasi`, dan `created_at` disembunyikan di tampilan namun tetap tersimpan di database.")
