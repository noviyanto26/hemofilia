# 20_rekap_gender_per_kelainan.py — Postgres-ready via db.py (fallback SQLite)
import streamlit as st
import pandas as pd
import io

from db import read_sql_df, ping
IS_PG = (ping() == "postgresql")

# ===================== Konfigurasi Halaman =====================
st.set_page_config(page_title="Rekap Gender per Kelainan", page_icon="📊", layout="wide")
st.title("📊 Rekapitulasi Data Berdasarkan Jenis Kelamin per Kelainan")

TABLE = "gender_per_kelainan"

KELAINAN_LIST = [
    "Hemofilia A",
    "Hemofilia B",
    "Hemofilia tipe lain/tidak dikenal",
    "Terduga Hemofilia/diagnosis belum ditegakkan",
    "VWD",
    "Kelainan pembekuan darah lain",
]
GENDER_COLS = [
    ("laki_laki", "Laki-laki"),
    ("perempuan", "Perempuan"),
    ("tidak_ada_data_gender", "Tidak ada data gender"),
]
TOTAL_COL = "total"
DB_NUM_COLS = [c for c, _ in GENDER_COLS] + [TOTAL_COL]

# ===================== Helpers DB =====================
def table_exists(table: str) -> bool:
    if IS_PG:
        df = read_sql_df(
            """
            SELECT 1 FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog','information_schema')
              AND table_name = :t
            """, {"t": table}
        )
        return not df.empty
    else:
        df = read_sql_df("SELECT name FROM sqlite_master WHERE type='table' AND name=:t", {"t": table})
        return not df.empty

def load_all() -> pd.DataFrame:
    """
    Ambil SEMUA data dari gender_per_kelainan + join identitas_organisasi.
    Kolom:
      g.created_at, g.kelainan, g.laki_laki, g.perempuan, g.tidak_ada_data_gender, g.total, g.is_total_row,
      g.kode_organisasi, io.hmhi_cabang, io.kota_cakupan_cabang
    """
    if not table_exists(TABLE):
        return pd.DataFrame()
    sql = f"""
        SELECT
          g.created_at, g.kelainan,
          g.laki_laki, g.perempuan, g.tidak_ada_data_gender,
          g.{TOTAL_COL}, g.is_total_row,
          g.kode_organisasi,
          io.hmhi_cabang, io.kota_cakupan_cabang
        FROM {TABLE} g
        LEFT JOIN identitas_organisasi io ON io.kode_organisasi = g.kode_organisasi
        ORDER BY g.id DESC
    """
    df = read_sql_df(sql)

    for c in DB_NUM_COLS:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).clip(lower=0).astype(int)

    df["is_total_row"] = df.get("is_total_row", "").astype(str).str.strip().fillna("")
    return df

def alias_df(df: pd.DataFrame) -> pd.DataFrame:
    alias_map = {
        "hmhi_cabang": "HMHI Cabang",
        "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
        "kode_organisasi": "Kode Organisasi",
        "created_at": "Created At",
        "kelainan": "Kelainan",
        "laki_laki": "Laki-laki",
        "perempuan": "Perempuan",
        "tidak_ada_data_gender": "Tidak ada data gender",
        "total": "Total",
    }
    return df.rename(columns=alias_map)

# ===================== Muat Data =====================
df_raw = load_all()
if df_raw.empty:
    st.info("Belum ada data pada tabel gender_per_kelainan.")
    st.stop()

# Untuk rekap, abaikan baris sintetis 'Total' agar tidak double-count
df = df_raw[df_raw["is_total_row"] != "1"].copy()

# ===================== Tabs =====================
tab_nasional, tab_per_cabang, tab_per_kelainan, tab_unduh = st.tabs(
    ["🇮🇩 Rekap Nasional & Grafik", "🏷️ Rekap per HMHI Cabang", "🧬 Rekap per Kelainan", "⬇️ Unduh"]
)

# ===================== Rekap Nasional =====================
with tab_nasional:
    st.subheader("🇮🇩 Rekap Nasional")
    total_nasional = {
        "laki_laki": int(df["laki_laki"].sum()),
        "perempuan": int(df["perempuan"].sum()),
        "tidak_ada_data_gender": int(df["tidak_ada_data_gender"].sum()),
        "total": int(df["total"].sum()),
    }
    tot_df = pd.DataFrame([total_nasional])
    tot_alias = alias_df(tot_df)

    grand_total = int(tot_df["total"].iloc[0])
    pct = {}
    if grand_total > 0:
        pct = {
            "Laki-laki": round(100 * tot_df["laki_laki"].iloc[0] / grand_total, 2),
            "Perempuan": round(100 * tot_df["perempuan"].iloc[0] / grand_total, 2),
            "Tidak ada data gender": round(100 * tot_df["tidak_ada_data_gender"].iloc[0] / grand_total, 2),
        }

    colA, colB = st.columns(2)
    with colA:
        st.markdown("**Tabel Total Nasional**")
        st.dataframe(tot_alias.rename(columns={"total": "Total"}), use_container_width=True)
    with colB:
        st.markdown("**Persentase per Gender (%)**")
        if pct:
            st.dataframe(pd.DataFrame([pct]), use_container_width=True)
        else:
            st.info("Belum dapat menghitung persentase (Total = 0).")

    st.markdown("**Grafik Nasional per Gender**")
    bar_gender = pd.Series(
        {"Laki-laki": total_nasional["laki_laki"],
         "Perempuan": total_nasional["perempuan"],
         "Tidak ada data gender": total_nasional["tidak_ada_data_gender"]}
    ).rename("Jumlah").to_frame()
    st.bar_chart(bar_gender, use_container_width=True, height=280)

    st.markdown("**Grafik Nasional per Kelainan (Total)**")
    per_kelainan_total = (
        df.groupby("kelainan", dropna=False)["total"].sum().reindex(KELAINAN_LIST).fillna(0).astype(int)
    )
    kel_total_df = per_kelainan_total.rename("Total").to_frame()
    kel_total_df.index.name = "Kelainan"
    st.bar_chart(kel_total_df, use_container_width=True, height=280)

# ===================== Rekap per HMHI Cabang =====================
with tab_per_cabang:
    st.subheader("🏷️ Rekap per HMHI Cabang (Provinsi)")
    agg_map = {c: "sum" for c in DB_NUM_COLS}
    rekap_cabang = (
        df.groupby("hmhi_cabang", dropna=False).agg(agg_map).reset_index().fillna({"hmhi_cabang": "-"})
    )
    rekap_cabang["total_semua_gender"] = rekap_cabang["total"]
    view_cabang = alias_df(
        rekap_cabang[["hmhi_cabang", "laki_laki", "perempuan", "tidak_ada_data_gender", "total_semua_gender"]]
    ).rename(columns={"total_semua_gender": "Total (Semua Gender)"})

    view_cabang = view_cabang.sort_values("Total (Semua Gender)", ascending=False)
    st.dataframe(view_cabang, use_container_width=True)

    st.markdown("**Grafik Top 10 Cabang – Total (Semua Gender)**")
    top10 = (view_cabang[["HMHI Cabang", "Total (Semua Gender)"]].head(10).set_index("HMHI Cabang"))
    if not top10.empty:
        st.bar_chart(top10, use_container_width=True, height=280)
    else:
        st.info("Belum ada data untuk grafik Top 10.")

    st.markdown("**Grafik Top 10 Cabang – Breakdown per Gender**")
    top10_gender = (rekap_cabang.sort_values("total", ascending=False)
                    .head(10)
                    .set_index("hmhi_cabang")[["laki_laki", "perempuan", "tidak_ada_data_gender"]])
    top10_gender = alias_df(top10_gender.reset_index()).set_index("HMHI Cabang")
    st.bar_chart(top10_gender, use_container_width=True, height=320)

# ===================== Rekap per Kelainan =====================
with tab_per_kelainan:
    st.subheader("🧬 Rekap per Kelainan")
    agg_kel = (
        df.groupby("kelainan", dropna=False)[["laki_laki", "perempuan", "tidak_ada_data_gender", "total"]]
          .sum()
          .reindex(KELAINAN_LIST)
          .fillna(0)
          .astype(int)
          .reset_index()
    )
    view_kel = alias_df(agg_kel).rename(columns={"total": "Total"})
    st.dataframe(view_kel, use_container_width=True)

    st.markdown("**Grafik per Kelainan – Breakdown per Gender (Nasional)**")
    kel_gender = agg_kel.set_index("kelainan")[["laki_laki", "perempuan", "tidak_ada_data_gender"]]
    kel_gender = alias_df(kel_gender.reset_index()).set_index("Kelainan")
    st.bar_chart(kel_gender, use_container_width=True, height=320)

# ===================== Unduh =====================
with tab_unduh:
    st.subheader("⬇️ Unduh Rekap")

    rekap_cabang_x = (
        df.groupby("hmhi_cabang", dropna=False)[["laki_laki", "perempuan", "tidak_ada_data_gender", "total"]]
          .sum()
          .reset_index()
          .fillna({"hmhi_cabang": "-"})
    )
    rekap_cabang_x = alias_df(rekap_cabang_x).rename(columns={"total": "Total"})

    rekap_kel_x = (
        df.groupby("kelainan", dropna=False)[["laki_laki", "perempuan", "tidak_ada_data_gender", "total"]]
          .sum()
          .reset_index()
    )
    rekap_kel_x = alias_df(rekap_kel_x).rename(columns={"total": "Total"})

    tot_alias_x = alias_df(pd.DataFrame([{
        "laki_laki": int(df["laki_laki"].sum()),
        "perempuan": int(df["perempuan"].sum()),
        "tidak_ada_data_gender": int(df["tidak_ada_data_gender"].sum()),
        "total": int(df["total"].sum()),
    }])).rename(columns={"total": "Total"})

    raw_preview = alias_df(df_raw.copy())

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
        rekap_cabang_x.to_excel(w, index=False, sheet_name="Rekap per Cabang")
        rekap_kel_x.to_excel(w, index=False, sheet_name="Rekap per Kelainan")
        tot_alias_x.to_excel(w, index=False, sheet_name="Total Nasional")
        raw_preview.to_excel(w, index=False, sheet_name="Raw (Audit)")

    st.download_button(
        "📦 Unduh Rekap (Excel)",
        buf.getvalue(),
        file_name="rekap_gender_per_kelainan.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        key="dl_excel"
    )

st.caption(
    "Catatan: rekap **mengabaikan** baris sintetis 'Total' (is_total_row=1) agar tidak terjadi penghitungan ganda. "
    "Pada tampilan UI, kolom teknis seperti **Created At** dan **Kota/Provinsi Cakupan Cabang** disembunyikan; "
    "namun sheet **Raw (Audit)** pada file unduhan tetap menyertakan informasi tersebut untuk keperluan audit data."
)
