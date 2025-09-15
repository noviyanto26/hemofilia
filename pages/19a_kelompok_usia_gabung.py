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
    Aman dijalankan berkali-kali (idempotent), dan sinkronkan sequence
    TANPA pernah set ke 0 (pakai 1,false saat tabel kosong).
    """
    if not is_postgres():
        return

    # 1) Pastikan default sequence terpasang
    exec_sql("""
    DO $$
    DECLARE
      col_default text;
      has_seq boolean;
    BEGIN
      SELECT column_default INTO col_default
      FROM information_schema.columns
      WHERE table_schema='public' AND table_name='kelompok_usia_gabung' AND column_name='id';

      IF col_default IS NULL THEN
        SELECT EXISTS (
          SELECT 1 FROM pg_class c WHERE c.relkind='S' AND c.relname='kelompok_usia_gabung_id_seq'
        ) INTO has_seq;

        IF NOT has_seq THEN
          EXECUTE 'CREATE SEQUENCE public.kelompok_usia_gabung_id_seq AS BIGINT START WITH 1 INCREMENT BY 1';
        END IF;

        EXECUTE 'ALTER SEQUENCE public.kelompok_usia_gabung_id_seq OWNED BY public.kelompok_usia_gabung.id';
        EXECUTE 'ALTER TABLE public.kelompok_usia_gabung ALTER COLUMN id SET DEFAULT nextval(''public.kelompok_usia_gabung_id_seq'')';
      END IF;
    END $$;
    """)

    # 2) Sinkronkan nilai sequence dengan isi tabel (tanpa set 0)
    exec_sql("""
    DO $$
    DECLARE
      max_id BIGINT;
      row_count BIGINT;
    BEGIN
      SELECT COUNT(*), COALESCE(MAX(id), 0) INTO row_count, max_id
      FROM public.kelompok_usia_gabung;

      IF row_count = 0 THEN
        PERFORM setval('public.kelompok_usia_gabung_id_seq', 1, false);
      ELSE
        PERFORM setval('public.kelompok_usia_gabung_id_seq', max_id, true);
      END IF;
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

# ==================== ANALISIS & GRAFIK ====================
st.divider()
st.header("📈 Analisis Rekapitulasi & Grafik")

# --- Siapkan data agregasi numeric ---
num_cols = ["hemo_a", "hemo_b", "hemo_tipe_lain", "vwd_tipe1", "vwd_tipe2"]
df_num = raw_df.copy()
for c in num_cols:
    if c in df_num.columns:
        df_num[c] = pd.to_numeric(df_num[c], errors="coerce").fillna(0).astype(int)

# --- KPI Ringkas ---
total_a = int(df_num["hemo_a"].sum()) if "hemo_a" in df_num else 0
total_b = int(df_num["hemo_b"].sum()) if "hemo_b" in df_num else 0
total_hemo = total_a + total_b
total_lain = int(df_num["hemo_tipe_lain"].sum()) if "hemo_tipe_lain" in df_num else 0
total_vwd1 = int(df_num["vwd_tipe1"].sum()) if "vwd_tipe1" in df_num else 0
total_vwd2 = int(df_num["vwd_tipe2"].sum()) if "vwd_tipe2" in df_num else 0
grand_total = total_hemo + total_lain + total_vwd1 + total_vwd2

cKPI1, cKPI2, cKPI3, cKPI4 = st.columns(4)
cKPI1.metric("Hemofilia A (total)", f"{total_a:,}")
cKPI2.metric("Hemofilia B (total)", f"{total_b:,}")
cKPI3.metric("vWD (total)", f"{(total_vwd1 + total_vwd2):,}")
cKPI4.metric("Grand Total", f"{grand_total:,}")

# --- Tabs Analitik ---
tab1, tab2, tab3, tab4 = st.tabs(["Ringkasan", "Per Kelompok Usia", "Per HMHI Cabang", "Tren Waktu"])

with tab1:
    st.subheader("Distribusi Total per Kategori")
    total_df = pd.DataFrame({
        "Kategori": ["Hemofilia A", "Hemofilia B", "Hemofilia Tipe Lain", "vWD - Tipe 1", "vWD - Tipe 2"],
        "Jumlah": [total_a, total_b, total_lain, total_vwd1, total_vwd2],
    }).set_index("Kategori")
    st.bar_chart(total_df, use_container_width=True)

    if total_hemo > 0:
        ratio_ab = (total_a / total_hemo) if total_hemo else 0
        st.write(f"**Rasio Hemofilia A:B** → A = {total_a:,}, B = {total_b:,} (A menyumbang {ratio_ab:.1%} dari total Hemofilia A+B).")
    else:
        st.write("Belum ada data Hemofilia A+B untuk menghitung rasio.")

with tab2:
    st.subheader("Agregasi per Kelompok Usia")
    if "kelompok_usia" in df_num.columns:
        usia_df = (
            df_num.groupby("kelompok_usia", dropna=False)[num_cols]
            .sum()
            .sort_index()
        )
        st.dataframe(usia_df, use_container_width=True)
        st.bar_chart(usia_df, use_container_width=True)

        # Rasio A:B per kelompok usia
        if {"hemo_a", "hemo_b"}.issubset(usia_df.columns):
            ratio_df = usia_df[["hemo_a", "hemo_b"]].copy()
            # Hindari pembagian nol; gunakan NaN lalu isi 0 agar float
            ratio_df["A_B_Ratio"] = (ratio_df["hemo_a"] / ratio_df["hemo_b"]).replace([float("inf")], float("nan"))
            ratio_df["A_B_Ratio"] = ratio_df["A_B_Ratio"].fillna(0.0)

            st.write("**Rasio A vs B per Kelompok Usia** (0 jika B=0):")
            st.bar_chart(ratio_df[["A_B_Ratio"]], use_container_width=True)
    else:
        st.info("Kolom 'kelompok_usia' tidak ditemukan.")



with tab3:
    st.subheader("Agregasi per HMHI Cabang")
    df_cabang = raw_df.copy()
    # Ambil label cabang dari view_df (yang sudah dialias)
    if "HMHI Cabang" in view_df.columns:
        # gabungkan id baris untuk memastikan urutan sinkron jika diperlukan
        df_cabang = df_cabang.join(view_df["HMHI Cabang"])
    else:
        df_cabang["HMHI Cabang"] = None

    df_cabang["HMHI Cabang"] = df_cabang["HMHI Cabang"].fillna("— (Tidak terisi)")
    cabang_df = (
        df_cabang.groupby("HMHI Cabang", dropna=False)[num_cols]
        .sum()
        .sort_values(["hemo_a", "hemo_b", "hemo_tipe_lain", "vwd_tipe1", "vwd_tipe2"], ascending=False)
    )
    st.dataframe(cabang_df, use_container_width=True)
    st.bar_chart(cabang_df[["hemo_a", "hemo_b"]], use_container_width=True)

    # Top 10 Cabang berdasarkan Hemofilia (A+B)
    if {"hemo_a", "hemo_b"}.issubset(cabang_df.columns):
        cabang_df["total_hemo"] = cabang_df["hemo_a"] + cabang_df["hemo_b"]
        top10 = cabang_df.sort_values("total_hemo", ascending=False).head(10)
        st.write("**Top 10 HMHI Cabang berdasarkan total Hemofilia (A+B):**")
        st.dataframe(top10[["total_hemo", "hemo_a", "hemo_b"]], use_container_width=True)

with tab4:
    st.subheader("Tren Waktu (berdasarkan created_at)")
    if "created_at" in df_num.columns:
        # Normalisasi ke tanggal (hari)
        ts = df_num.copy()
        ts["created_at"] = pd.to_datetime(ts["created_at"], errors="coerce").dt.date
        ts = ts.dropna(subset=["created_at"])
        if not ts.empty:
            daily = ts.groupby("created_at")[num_cols].sum().sort_index()
            st.line_chart(daily, use_container_width=True)
        else:
            st.info("Tidak ada data tanggal yang valid untuk dianalisis.")
    else:
        st.info("Kolom 'created_at' tidak ditemukan.")

# ==================== Unduh Excel ====================
st.divider()
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
