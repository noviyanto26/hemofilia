# 19b_rekap_jumlah_individu.py — Postgres-ready via db.py (adaptive backend)
import streamlit as st
import pandas as pd
import io

from db import read_sql_df

# ============== Konfigurasi Halaman ==============
st.set_page_config(page_title="Rekap Jumlah Individu Hemofilia", page_icon="📊", layout="wide")
st.title("📊 Rekapitulasi Jumlah Individu Hemofilia")

TABLE = "jumlah_individu_hemofilia"

# === Definisi kolom sumber (DB) dan alias (tampilan) ===
FIELDS = [
    ("jumlah_total_ab", "Jumlah total penyandang hemofilia A dan B"),
    ("hemofilia_lain", "Hemofilia lain/tidak dikenal"),
    ("terduga", "Terduga hemofilia/diagnosis belum ditegakkan"),
    ("vwd", "Von Willebrand Disease (vWD)"),
    ("lainnya", "Kelainan pembekuan darah genetik lainnya"),
]
DB_COLS = [c for c, _ in FIELDS]
ALIAS_MAP = {c: a for c, a in FIELDS}

# ============== Helpers DB ==============

def table_exists(table: str) -> bool:
    """Deteksi adaptif: coba cek di Postgres (information_schema) lalu fallback ke SQLite."""
    # Coba Postgres (schema public)
    try:
        df = read_sql_df(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema IN ('public') AND table_name = :t
            LIMIT 1
            """,
            {"t": table},
        )
        return not df.empty
    except Exception:
        # Fallback SQLite
        try:
            df = read_sql_df("SELECT name FROM sqlite_master WHERE type='table' AND name=:t", {"t": table})
            return not df.empty
        except Exception:
            return False


def load_raw() -> pd.DataFrame:
    """Ambil semua data dari jumlah_individu_hemofilia JOIN identitas_organisasi."""
    if not table_exists(TABLE):
        return pd.DataFrame()

    base_sql = f"""
        SELECT
          j.created_at,
          j.kode_organisasi,
          io.hmhi_cabang,
          io.kota_cakupan_cabang,
          {", ".join([f"j.{c}" for c in DB_COLS])}
        FROM public.{TABLE} j
        LEFT JOIN public.identitas_organisasi io ON io.kode_organisasi = j.kode_organisasi
        ORDER BY j.created_at DESC
    """
    try:
        df = read_sql_df(base_sql)
    except Exception:
        # Fallback tanpa schema qualifier (mis. SQLite / search_path custom)
        df = read_sql_df(
            f"""
            SELECT
              j.created_at,
              j.kode_organisasi,
              io.hmhi_cabang,
              io.kota_cakupan_cabang,
              {", ".join([f"j.{c}" for c in DB_COLS])}
            FROM {TABLE} j
            LEFT JOIN identitas_organisasi io ON io.kode_organisasi = j.kode_organisasi
            ORDER BY j.created_at DESC
            """
        )

    for c in DB_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).clip(lower=0).astype(int)
    return df


def alias_df(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns=ALIAS_MAP).rename(columns={
        "hmhi_cabang": "HMHI Cabang",
        "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
        "kode_organisasi": "Kode Organisasi",
        "created_at": "Created At",
    })

# ============== Muat Data ==============
df_raw = load_raw()
if df_raw.empty:
    st.info("Belum ada data tersimpan di tabel jumlah_individu_hemofilia.")
    st.stop()

# ============== Tabs Halaman ==============
tab_per_cabang, tab_nasional, tab_unduh = st.tabs(
    ["🏷️ Rekap per HMHI Cabang", "🇮🇩 Rekap Nasional & Grafik", "⬇️ Unduh"]
)

# ============== Rekap per HMHI Cabang ==============
with tab_per_cabang:
    st.subheader("🏷️ Rekap per HMHI Cabang (Provinsi)")
    grp_cols = ["hmhi_cabang"]
    agg_map = {c: "sum" for c in DB_COLS}
    rekap_cabang = (
        df_raw.groupby(grp_cols, dropna=False)
        .agg(agg_map)
        .reset_index()
        .fillna({"hmhi_cabang": "-"})
    )
    rekap_cabang["total_semua_kategori"] = rekap_cabang[DB_COLS].sum(axis=1)

    rekap_view = alias_df(rekap_cabang).rename(columns={"total_semua_kategori": "Total (Semua Kategori)"})
    rekap_view = rekap_view.sort_values("Total (Semua Kategori)", ascending=False)

    st.dataframe(rekap_view, use_container_width=True)

    top10 = (
        rekap_view.loc[:, ["HMHI Cabang", "Total (Semua Kategori)"]]
        .head(10)
        .set_index("HMHI Cabang")
    )
    st.markdown("**Grafik Top 10 Cabang – Total (Semua Kategori)**")
    if not top10.empty:
        st.bar_chart(top10, use_container_width=True, height=280)
    else:
        st.info("Belum ada data untuk grafik Top 10.")

# ============== Rekap Nasional + Grafik ==============
with tab_nasional:
    st.subheader("🇮🇩 Rekap Nasional")
    total_row = {c: int(df_raw[c].sum()) for c in DB_COLS}
    total_df = pd.DataFrame([total_row], columns=DB_COLS)

    total_alias = alias_df(total_df)
    total_alias["Total (Semua Kategori)"] = total_alias.sum(axis=1, numeric_only=True)
    grand_total = int(total_alias["Total (Semua Kategori)"].iloc[0])

    pct_series = {}
    if grand_total > 0:
        for c, a in FIELDS:
            alias = ALIAS_MAP[c]
            pct_series[alias] = round(100 * total_alias[alias].iloc[0] / grand_total, 2)

    cols = st.columns(2)
    with cols[0]:
        st.markdown("**Tabel Total Nasional**")
        st.dataframe(total_alias, use_container_width=True)
    with cols[1]:
        if pct_series:
            st.markdown("**Persentase per Kategori (%)**")
            pct_df = pd.DataFrame([pct_series])
            st.dataframe(pct_df, use_container_width=True)

    st.markdown("**Grafik Nasional per Kategori**")
    bar_nat = pd.Series({ALIAS_MAP[c]: int(total_df[c].iloc[0]) for c in DB_COLS}).rename("Jumlah").to_frame()
    st.bar_chart(bar_nat, use_container_width=True, height=280)

# ============== Unduh ==============
with tab_unduh:
    st.subheader("⬇️ Unduh Rekap")

    grp_cols = ["hmhi_cabang"]
    agg_map = {c: "sum" for c in DB_COLS}
    rekap_cabang = (
        df_raw.groupby(grp_cols, dropna=False)
        .agg(agg_map)
        .reset_index()
        .fillna({"hmhi_cabang": "-"})
    )
    rekap_cabang["total_semua_kategori"] = rekap_cabang[DB_COLS].sum(axis=1)
    rekap_cabang_alias = alias_df(rekap_cabang).rename(columns={"total_semua_kategori": "Total (Semua Kategori)"})

    total_row = {c: int(df_raw[c].sum()) for c in DB_COLS}
    total_df = pd.DataFrame([total_row], columns=DB_COLS)
    total_alias = alias_df(total_df)
    total_alias["Total (Semua Kategori)"] = total_alias.sum(axis=1, numeric_only=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        rekap_cabang_alias.to_excel(w, index=False, sheet_name="Rekap per Cabang")
        total_alias.to_excel(w, index=False, sheet_name="Rekap Nasional")
    st.download_button(
        "📦 Unduh Rekap (Excel)",
        buf.getvalue(),
        file_name="rekap_jumlah_individu_hemofilia.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="rekap::dl"
    )

st.caption("Catatan: tampilan menyembunyikan kolom teknis seperti **Kode Organisasi** dan **Created At**.")
