# pages/19a_kelompok_usia_gabung.py
# -*- coding: utf-8 -*-
import io
import pandas as pd
import streamlit as st

# Util DB (Postgres-ready via Supabase, fallback SQLite) — pastikan db.py terbaru sudah dipakai
from db import exec_sql, fetch_df, is_postgres

PAGE_TITLE = "🧮 Rekap Gabungan Kelompok Usia (Rebuild)"
SRC_TABLE = "kelompok_usia"
DST_TABLE = "kelompok_usia_gabung"
ORG_TABLE = "identitas_organisasi"   # join via kode_organisasi

st.set_page_config(page_title="Rekap Gabungan Kelompok Usia", page_icon="🧮", layout="wide")
st.title(PAGE_TITLE)


# ---------- DDL: create table if not exists ----------
def ensure_dst_table():
    """
    Buat tabel gabungan jika belum ada (dialect-aware).
    Untuk Postgres: pakai BIGSERIAL agar id punya default sequence.
    """
    if is_postgres():
        exec_sql(f"""
        CREATE TABLE IF NOT EXISTS public.{DST_TABLE} (
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
        exec_sql(f"CREATE INDEX IF NOT EXISTS idx_{DST_TABLE}_kode_usia ON public.{DST_TABLE}(kode_organisasi, kelompok_usia);")
    else:
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


# ---------- Postgres schema fixer: pasang default sequence pada kolom id jika hilang ----------
def _pg_fix_id_default_if_needed():
    """
    Perbaiki default sequence kolom id jika tabel lama dibuat tanpa BIGSERIAL/IDENTITY.
    Aman dijalankan berkali-kali (idempotent).
    """
    if not is_postgres():
        return

    q = """
    SELECT column_default
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'kelompok_usia_gabung' AND column_name = 'id'
    """
    try:
        df = fetch_df(q)
        col_default = (df.iloc[0, 0] if not df.empty else None)
    except Exception:
        col_default = None

    if not col_default:
        exec_sql("""
        DO $$
        DECLARE
          has_seq boolean;
        BEGIN
          SELECT EXISTS (
            SELECT 1 FROM pg_class c
            WHERE c.relkind = 'S' AND c.relname = 'kelompok_usia_gabung_id_seq'
          ) INTO has_seq;

          IF NOT has_seq THEN
            EXECUTE 'CREATE SEQUENCE public.kelompok_usia_gabung_id_seq AS BIGINT START WITH 1 INCREMENT BY 1';
          END IF;

          EXECUTE 'ALTER SEQUENCE public.kelompok_usia_gabung_id_seq OWNED BY public.kelompok_usia_gabung.id';
          EXECUTE 'ALTER TABLE public.kelompok_usia_gabung ALTER COLUMN id SET DEFAULT nextval(''public.kelompok_usia_gabung_id_seq'')';
          EXECUTE 'SELECT setval(''public.kelompok_usia_gabung_id_seq'', COALESCE((SELECT MAX(id) FROM public.kelompok_usia_gabung), 0))';
        END $$;
        """)


# ---------- REBUILD: truncate + insert (tanpa kolom id) ----------
def rebuild_gabungan():
    """
    Rebuild penuh dari sumber 'kelompok_usia' ke 'kelompok_usia_gabung':
    - Pastikan tabel ada
    - Perbaiki default id (PG) jika hilang
    - Kosongkan (TRUNCATE RESTART IDENTITY di PG / DELETE + reset di SQLite)
    - Insert agregasi tanpa menyertakan kolom 'id'
    """
    ensure_dst_table()
    _pg_fix_id_default_if_needed()

    if is_postgres():
        exec_sql(f"TRUNCATE TABLE public.{DST_TABLE} RESTART IDENTITY;")
    else:
        exec_sql(f"DELETE FROM {DST_TABLE};")
        try:
            exec_sql(f"DELETE FROM sqlite_sequence WHERE name = '{DST_TABLE}';")
        except Exception:
            pass  # sqlite_sequence bisa belum ada

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


# ---------- READ: tampilan gabungan + join HMHI Cabang ----------
def read_joined_df():
    """
    Tampilkan data gabungan + HMHI Cabang dari identitas_organisasi.
    Nama kolom HMHI bisa bervariasi; coba beberapa kemungkinan.
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
        sql = sql.replace(" NULLS LAST", "")
        df = fetch_df(sql)

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
- Tombol **Rebuild Rekap** akan mengosongkan tabel gabungan dan mengisinya ulang dari **kelompok_usia**.
- Aman di Postgres meski tabel lama belum punya sequence: halaman ini akan **memperbaiki default `id`** jika diperlukan.
- Tampilan sudah **join HMHI Cabang** dari `identitas_organisasi`.
- Sediakan **unduh Excel** hasil rekap.
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
