# 19a_kelompok_usia_gabung.py — Postgres-ready via db.py (fallback SQLite)
from __future__ import annotations

import io
from typing import Optional, List
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import text

from db import read_sql_df, exec_sql, table_exists, ping

# ======================== Konfigurasi Halaman ========================
st.set_page_config(page_title="Kelompok Usia (Gabung)", page_icon="🧮", layout="wide")
st.title("🧮 Rekap Gabungan Kelompok Usia")

SRC_TABLE = "kelompok_usia"
DST_TABLE = "kelompok_usia_gabung"
ORG_TABLE = "identitas_organisasi"

# Urutan tampilan Kelompok Usia
KLP_ORDER: List[str] = [">45", "19-44", "14-18", "5-13", "0-4"]
KLP_LABEL_KOSONG = "Tidak ada data usia"
KLP_ORDER_EXT = KLP_ORDER + [KLP_LABEL_KOSONG]

# ======================== Util DB umum ========================
def get_columns(table: str) -> List[str]:
    """
    Daftar kolom lintas-DB.
    1) Coba information_schema (Postgres).
    2) Fallback PRAGMA (SQLite).
    """
    if not table_exists(table):
        return []

    # Postgres / ANSI first
    try:
        df = read_sql_df(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = :t
            ORDER BY ordinal_position
            """,
            {"t": table}
        )
        if not df.empty:
            return df["column_name"].tolist()
    except Exception:
        pass

    # SQLite fallback
    try:
        df = read_sql_df(f'PRAGMA table_info("{table}")')
        # kolom "name" adalah nama kolom di PRAGMA table_info
        if not df.empty:
            # compat: kadang pandas mengembalikan integer index (1) utk nama; prefer "name" jika ada
            if "name" in df.columns:
                return df["name"].astype(str).tolist()
            elif 1 in df.columns:
                return df[1].astype(str).tolist()
    except Exception:
        pass

    return []

def ensure_dst_table():
    """
    Buat tabel tujuan bila belum ada, dengan skema yang cocok untuk SQL kamu.
    Skema generik ini compatible untuk Postgres & SQLite.
    """
    exec_sql(f"""
        CREATE TABLE IF NOT EXISTS {DST_TABLE} (
            id BIGINT,
            kode_organisasi TEXT NOT NULL,
            created_at TEXT NOT NULL,
            kelompok_usia TEXT,
            hemo_a INTEGER,
            hemo_b INTEGER,
            hemo_tipe_lain INTEGER,
            vwd_tipe1 INTEGER,
            vwd_tipe2 INTEGER
        )
    """)

def find_hmhi_cabang_col() -> Optional[str]:
    """
    Cari nama kolom HMHI Cabang yang mungkin bervariasi penulisannya.
    Preferensi: hmhi_cabang -> "HMHI Cabang" -> HMHI_Cabang -> "hmhi cabang"
    """
    if not table_exists(ORG_TABLE):
        return None
    cols = set(get_columns(ORG_TABLE))
    candidates = ["hmhi_cabang", "HMHI Cabang", "HMHI_Cabang", "hmhi cabang"]
    for c in candidates:
        if c in cols:
            return c
    return None

# ======================== Bangun/Perbarui Tabel Gabung ========================
def build_gabung():
    """
    Refresh isi DST_TABLE dari SRC_TABLE (jika SRC_TABLE ada).
    Jika SRC_TABLE tidak ada, hanya memastikan tabel tujuan tersedia.
    """
    ensure_dst_table()
    if not table_exists(SRC_TABLE):
        return

    # Pastikan kolom sumber yang dibutuhkan ada (toleran: kalau tidak ada, treat 0)
    src_cols = set(get_columns(SRC_TABLE))

    # Nama kolom yang biasa ada pada sumber
    ha_cols = ["ha_ringan", "ha_sedang", "ha_berat"]
    hb_cols = ["hb_ringan", "hb_sedang", "hb_berat"]
    other_cols = ["hemo_tipe_lain", "vwd_tipe1", "vwd_tipe2"]

    def coalesce_or_zero(col: str) -> str:
        # jika kolom ada, pakai COALESCE(kol,0), jika tidak, pakai literal 0
        return f"COALESCE({col},0)" if col in src_cols else "0"

    expr_ha = " + ".join(coalesce_or_zero(c) for c in ha_cols) or "0"
    expr_hb = " + ".join(coalesce_or_zero(c) for c in hb_cols) or "0"
    expr_hemo_lain = coalesce_or_zero("hemo_tipe_lain")
    expr_vwd1 = coalesce_or_zero("vwd_tipe1")
    expr_vwd2 = coalesce_or_zero("vwd_tipe2")

    # Field ID/Kode/Created/Usia: kalau tidak ada di sumber, isi NULL/konstanta aman
    id_expr = "id" if "id" in src_cols else "NULL"
    kode_expr = "kode_organisasi" if "kode_organisasi" in src_cols else "NULL"
    created_expr = "created_at" if "created_at" in src_cols else f"'{datetime.utcnow().strftime('%Y-%m-%d')}'"
    usia_expr = "kelompok_usia" if "kelompok_usia" in src_cols else "NULL"

    # Bersihkan isi agar selalu sesuai sumber terbaru
    # (TRUNCATE lebih cepat, tapi DELETE kompatibel lintas DB)
    try:
        exec_sql(f"TRUNCATE TABLE {DST_TABLE}")
    except Exception:
        exec_sql(f"DELETE FROM {DST_TABLE}")

    # Insert dari sumber dengan penjumlahan & COALESCE agar NULL jadi 0
    exec_sql(text(f"""
        INSERT INTO {DST_TABLE} (
            id, kode_organisasi, created_at, kelompok_usia,
            hemo_a, hemo_b, hemo_tipe_lain, vwd_tipe1, vwd_tipe2
        )
        SELECT
            {id_expr} AS id,
            {kode_expr} AS kode_organisasi,
            {created_expr} AS created_at,
            {usia_expr} AS kelompok_usia,
            ({expr_ha})     AS hemo_a,
            ({expr_hb})     AS hemo_b,
            ({expr_hemo_lain}) AS hemo_tipe_lain,
            ({expr_vwd1})   AS vwd_tipe1,
            ({expr_vwd2})   AS vwd_tipe2
        FROM {SRC_TABLE}
    """))

# ======================== Load & Tampilkan ========================
def load_with_join() -> pd.DataFrame:
    ensure_dst_table()
    hmhi_col = find_hmhi_cabang_col()
    if hmhi_col:
        q = f"""
            SELECT
                g.id, g.kode_organisasi, g.created_at,
                g.kelompok_usia,
                g.hemo_a, g.hemo_b, g.hemo_tipe_lain, g.vwd_tipe1, g.vwd_tipe2,
                o."{hmhi_col}" AS hmhi_cabang
            FROM {DST_TABLE} g
            LEFT JOIN {ORG_TABLE} o
              ON g.kode_organisasi = o.kode_organisasi
        """
    else:
        q = f"""
            SELECT
                g.id, g.kode_organisasi, g.created_at,
                g.kelompok_usia,
                g.hemo_a, g.hemo_b, g.hemo_tipe_lain, g.vwd_tipe1, g.vwd_tipe2,
                NULL AS hmhi_cabang
            FROM {DST_TABLE} g
        """
    return read_sql_df(q)

def apply_display_alias(df: pd.DataFrame) -> pd.DataFrame:
    if "kelompok_usia" in df.columns:
        df["kelompok_usia"] = (
            df["kelompok_usia"].astype(object).where(lambda s: ~s.isna(), KLP_LABEL_KOSONG)
        )
        df["kelompok_usia"] = df["kelompok_usia"].replace("None", KLP_LABEL_KOSONG)
        cat = pd.Categorical(df["kelompok_usia"], categories=KLP_ORDER_EXT, ordered=True)
        df = df.assign(kelompok_usia=cat)

    rename_map = {
        "hmhi_cabang": "HMHI Cabang",
        "kelompok_usia": "Kelompok Usia",
        "hemo_a": "Hemofilia A",
        "hemo_b": "Hemofilia B",
        "hemo_tipe_lain": "Hemofilia Tipe Lain",
        "vwd_tipe1": "vWD Tipe 1",
        "vwd_tipe2": "vWD Tipe 2",
    }
    df_disp = df.rename(columns=rename_map)

    drop_cols = [c for c in ["id", "kode_organisasi", "created_at"] if c in df_disp.columns]
    df_disp = df_disp.drop(columns=drop_cols, errors="ignore")

    cols_order = [c for c in [
        "HMHI Cabang","Kelompok Usia","Hemofilia A","Hemofilia B","Hemofilia Tipe Lain","vWD Tipe 1","vWD Tipe 2",
    ] if c in df_disp.columns]
    if cols_order:
        df_disp = df_disp[cols_order]

    if "HMHI Cabang" in df_disp.columns:
        hmhi_sort = df_disp["HMHI Cabang"].astype(str).fillna("").str.strip().str.casefold()
        df_disp = df_disp.assign(_hmhi_sort=hmhi_sort)
        by_cols = ["_hmhi_sort"]
        if "Kelompok Usia" in df_disp.columns:
            by_cols.append("Kelompok Usia")
        df_disp = df_disp.sort_values(by=by_cols, kind="mergesort").drop(columns=["_hmhi_sort"], errors="ignore")
    else:
        if "Kelompok Usia" in df_disp.columns:
            df_disp = df_disp.sort_values(by=["Kelompok Usia"], kind="mergesort")
    return df_disp

# =============== Template & Unduh/Upload Excel (alias kolom) ===============
TEMPLATE_COLUMNS = [
    "HMHI Cabang","Kelompok Usia","Hemofilia A","Hemofilia B","Hemofilia Tipe Lain","vWD Tipe 1","vWD Tipe 2",
]

def make_template_df() -> pd.DataFrame:
    example = {
        "HMHI Cabang": "Contoh Cabang",
        "Kelompok Usia": "19-44",
        "Hemofilia A": 0,
        "Hemofilia B": 0,
        "Hemofilia Tipe Lain": 0,
        "vWD Tipe 1": 0,
        "vWD Tipe 2": 0,
    }
    return pd.DataFrame([example], columns=TEMPLATE_COLUMNS)

def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Data") -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
    return bio.getvalue()

def current_display_to_excel_bytes(df_disp: pd.DataFrame) -> bytes:
    if df_disp is None or df_disp.empty:
        return df_to_excel_bytes(pd.DataFrame(columns=TEMPLATE_COLUMNS), sheet_name="KelompokUsiaGabung")
    return df_to_excel_bytes(df_disp, sheet_name="KelompokUsiaGabung")

def upload_excel_to_db(df_upload: pd.DataFrame):
    """
    Terima DataFrame dengan kolom alias TEMPLATE_COLUMNS.
    Map:
      - HMHI Cabang -> kode_organisasi (via ORG_TABLE)
      - Kelompok Usia (harus salah satu dari KLP_ORDER_EXT)
      - Nilai numeric -> masukkan ke DST_TABLE (overwrite per (kode_organisasi, kelompok_usia)).
    """
    ensure_dst_table()

    missing = [c for c in TEMPLATE_COLUMNS if c not in df_upload.columns]
    if missing:
        raise ValueError(f"Kolom berikut wajib ada di file: {missing}")

    num_cols = ["Hemofilia A", "Hemofilia B", "Hemofilia Tipe Lain", "vWD Tipe 1", "vWD Tipe 2"]
    for c in num_cols:
        df_upload[c] = pd.to_numeric(df_upload[c], errors="coerce").fillna(0).astype(int)

    df_upload["Kelompok Usia"] = (
        df_upload["Kelompok Usia"].astype(str).str.strip().replace("None", KLP_LABEL_KOSONG)
    )
    invalid_ku = sorted(set(df_upload["Kelompok Usia"]) - set(KLP_ORDER_EXT))
    if invalid_ku:
        raise ValueError(f"Nilai 'Kelompok Usia' tidak valid: {invalid_ku}. Harus salah satu dari {KLP_ORDER_EXT}")

    hmhi_col = find_hmhi_cabang_col()
    if not hmhi_col:
        raise RuntimeError("Kolom HMHI Cabang tidak ditemukan di tabel identitas_organisasi.")

    map_df = read_sql_df(f'SELECT kode_organisasi, "{hmhi_col}" AS hmhi_cabang FROM {ORG_TABLE}')
    map_df = map_df.dropna(subset=["hmhi_cabang"]).drop_duplicates(subset=["hmhi_cabang"])

    merged = df_upload.merge(map_df, how="left", left_on="HMHI Cabang", right_on="hmhi_cabang")
    if merged["kode_organisasi"].isna().any():
        missing_cab = sorted(merged.loc[merged["kode_organisasi"].isna(), "HMHI Cabang"].unique())
        raise ValueError(f"Nama HMHI Cabang berikut tidak ditemukan di '{ORG_TABLE}': {missing_cab}")

    today = datetime.utcnow().strftime("%Y-%m-%d")
    # Overwrite per (kode_organisasi, kelompok_usia)
    for _, row in merged.iterrows():
        kode = row["kode_organisasi"]
        ku = row["Kelompok Usia"]
        payload_del = {"kode": kode, "ku": ku}
        exec_sql(text(f"DELETE FROM {DST_TABLE} WHERE kode_organisasi = :kode AND kelompok_usia = :ku"), payload_del)

        payload_ins = {
            "kode": kode, "created_at": today, "ku": ku,
            "a": int(row["Hemofilia A"]), "b": int(row["Hemofilia B"]),
            "lain": int(row["Hemofilia Tipe Lain"]), "v1": int(row["vWD Tipe 1"]), "v2": int(row["vWD Tipe 2"]),
        }
        exec_sql(text(f"""
            INSERT INTO {DST_TABLE} (
                kode_organisasi, created_at, kelompok_usia,
                hemo_a, hemo_b, hemo_tipe_lain, vwd_tipe1, vwd_tipe2
            ) VALUES (:kode, :created_at, :ku, :a, :b, :lain, :v1, :v2)
        """), payload_ins)

# ======================== Main ========================
def main():
    # Info koneksi singkat
    p = ping()
    if not p.get("ok"):
        st.error("Database belum siap.")
        st.code(p, language="json")
        st.stop()

    df_disp = pd.DataFrame()
    df_raw = pd.DataFrame()

    # Bangun/refresh tabel gabung dari sumber (jika ada)
    try:
        build_gabung()
    except Exception as e:
        st.error("Gagal membangun tabel gabungan dari sumber.")
        st.exception(e)

    try:
        df_raw = load_with_join()
        if df_raw is not None and not df_raw.empty:
            df_disp = apply_display_alias(df_raw.copy())
    except Exception as e:
        st.error("Gagal memuat data gabungan.")
        st.exception(e)

    # --------- Toolbar Unduh / Unggah ----------
    st.subheader("⬇️ Unduh / ⬆️ Unggah Excel")

    col1, col2, _ = st.columns([1,1,2])
    with col1:
        template_df = make_template_df()
        st.download_button(
            "📥 Unduh Template Excel",
            data=df_to_excel_bytes(template_df, sheet_name="Template"),
            file_name="template_kelompok_usia_gabung.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help=("Kolom: HMHI Cabang, Kelompok Usia "
                  f"(boleh: {', '.join(KLP_ORDER_EXT)}), "
                  "Hemofilia A, Hemofilia B, Hemofilia Tipe Lain, vWD Tipe 1, vWD Tipe 2"),
        )
    with col2:
        st.download_button(
            "📥 Unduh Data Saat Ini",
            data=current_display_to_excel_bytes(df_disp),
            file_name="kelompok_usia_gabung_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Ekspor tampilan tabel saat ini (dengan alias kolom).",
        )

    st.divider()

    # --------- Upload Form ----------
    st.subheader("⬆️ Unggah Excel")
    uploaded = st.file_uploader(
        "Pilih file Excel (.xlsx) dengan kolom: "
        "`HMHI Cabang`, `Kelompok Usia`, `Hemofilia A`, `Hemofilia B`, "
        "`Hemofilia Tipe Lain`, `vWD Tipe 1`, `vWD Tipe 2`",
        type=["xlsx"]
    )

    if uploaded is not None:
        try:
            up_df = pd.read_excel(uploaded)
            upload_excel_to_db(up_df)
            # Reload setelah tulis
            df_raw = load_with_join()
            df_disp = apply_display_alias(df_raw.copy()) if not df_raw.empty else pd.DataFrame()
            st.success("Data berhasil diunggah & diperbarui.")
        except Exception as e:
            st.error(f"Gagal memproses unggahan: {e}")

    # --------- Tabel Tampilan ----------
    st.subheader("📊 Tabel Kelompok Usia")
    if df_disp.empty:
        st.warning("Belum ada data pada tabel gabungan. Unggah Excel atau isi data sumber terlebih dahulu.")
    else:
        st.dataframe(df_disp, use_container_width=True)

    # =============== ANALISIS / REKAP / GRAFIK ===============
    st.divider()
    st.subheader("📈 Analisis, Rekapitulasi, dan Grafik")

    if df_raw is None or df_raw.empty:
        st.info("Tidak ada data untuk dianalisis.")
    else:
        dat = df_raw.copy()
        dat["kelompok_usia"] = (
            dat["kelompok_usia"].astype(object)
            .where(lambda s: ~s.isna(), KLP_LABEL_KOSONG)
            .replace("None", KLP_LABEL_KOSONG)
        )

        def _to_date(x):
            try:
                return datetime.strptime(str(x), "%Y-%m-%d").date()
            except Exception:
                return None
        dat["created_at_date"] = dat["created_at"].apply(_to_date)

        with st.expander("🔎 Filter"):
            cabang_list = sorted([c for c in dat["hmhi_cabang"].dropna().unique()])
            pilih_cabang = st.selectbox("HMHI Cabang", ["(Semua)"] + cabang_list, index=0)

            valid_dates = [d for d in dat["created_at_date"] if d is not None]
            min_d = min(valid_dates) if valid_dates else None
            max_d = max(valid_dates) if valid_dates else None
            if min_d and max_d:
                r_start, r_end = st.date_input("Rentang tanggal created_at", (min_d, max_d))
            else:
                r_start, r_end = None, None

        if pilih_cabang != "(Semua)":
            dat = dat[dat["hmhi_cabang"] == pilih_cabang]
        if r_start and r_end:
            dat = dat[(dat["created_at_date"] >= r_start) & (dat["created_at_date"] <= r_end)]

        if dat.empty:
            st.warning("Tidak ada data sesuai filter.")
            return

        # ===== REKAP NASIONAL =====
        st.markdown("### 🇮🇩 Rekap Nasional (Total)")
        total_series = dat[["hemo_a","hemo_b","hemo_tipe_lain","vwd_tipe1","vwd_tipe2"]].sum()
        rekap_nasional = pd.DataFrame({
            "Kategori": ["Hemofilia A","Hemofilia B","Hemofilia Tipe Lain","vWD Tipe 1","vWD Tipe 2"],
            "Jumlah": [int(total_series.get("hemo_a", 0)),
                       int(total_series.get("hemo_b", 0)),
                       int(total_series.get("hemo_tipe_lain", 0)),
                       int(total_series.get("vwd_tipe1", 0)),
                       int(total_series.get("vwd_tipe2", 0))]
        })
        c1, c2 = st.columns([2,1])
        with c1:
            st.dataframe(rekap_nasional, use_container_width=True)
        with c2:
            pie = px.pie(rekap_nasional, names="Kategori", values="Jumlah", hole=0.3)
            st.plotly_chart(pie, use_container_width=True)

        def _df_to_bytes(df, sheet="RekapNasional"):
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as w:
                df.to_excel(w, index=False, sheet_name=sheet)
            return bio.getvalue()
        st.download_button("📥 Unduh Rekap Nasional (Excel)",
                           data=_df_to_bytes(rekap_nasional),
                           file_name="rekap_nasional_kelompok_usia.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        st.divider()

        # ===== REKAP PER KELOMPOK USIA =====
        st.markdown("### 👥 Rekap per Kelompok Usia")
        dat["kelompok_usia"] = pd.Categorical(dat["kelompok_usia"], categories=KLP_ORDER_EXT, ordered=True)
        rekap_usia = (dat.groupby("kelompok_usia")[["hemo_a","hemo_b","hemo_tipe_lain","vwd_tipe1","vwd_tipe2"]]
                          .sum()
                          .reset_index()
                          .rename(columns={
                              "kelompok_usia":"Kelompok Usia",
                              "hemo_a":"Hemofilia A",
                              "hemo_b":"Hemofilia B",
                              "hemo_tipe_lain":"Hemofilia Tipe Lain",
                              "vwd_tipe1":"vWD Tipe 1",
                              "vwd_tipe2":"vWD Tipe 2",
                          }))
        st.dataframe(rekap_usia, use_container_width=True)
        st.download_button("📥 Unduh Rekap per Kelompok Usia (Excel)",
                           data=_df_to_bytes(rekap_usia, "RekapUsia"),
                           file_name="rekap_per_kelompok_usia.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        # ===== REKAP PER CABANG =====
        st.markdown("### 🏢 Rekap per HMHI Cabang")
        rekap_cabang = (dat.fillna({"hmhi_cabang":"(Tidak diketahui)"})
                          .groupby("hmhi_cabang")[["hemo_a","hemo_b","hemo_tipe_lain","vwd_tipe1","vwd_tipe2"]]
                          .sum()
                          .reset_index()
                          .rename(columns={
                              "hmhi_cabang":"HMHI Cabang",
                              "hemo_a":"Hemofilia A",
                              "hemo_b":"Hemofilia B",
                              "hemo_tipe_lain":"Hemofilia Tipe Lain",
                              "vwd_tipe1":"vWD Tipe 1",
                              "vwd_tipe2":"vWD Tipe 2",
                          }))
        rekap_cabang = rekap_cabang.sort_values("HMHI Cabang", key=lambda s: s.astype(str).str.casefold())
        st.dataframe(rekap_cabang, use_container_width=True)
        st.download_button("📥 Unduh Rekap per Cabang (Excel)",
                           data=_df_to_bytes(rekap_cabang, "RekapCabang"),
                           file_name="rekap_per_cabang.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

        st.divider()

        # ===== GRAFIK =====
        st.markdown("### 📊 Grafik")
        df_plot_usia = rekap_usia.melt(
            id_vars="Kelompok Usia",
            value_vars=["Hemofilia A","Hemofilia B","Hemofilia Tipe Lain","vWD Tipe 1","vWD Tipe 2"],
            var_name="Kategori", value_name="Nilai"
        )
        if st.checkbox("Tampilkan grafik sebagai persentase (normalize)", value=False, key="persen_global"):
            denom = df_plot_usia.groupby("Kelompok Usia")["Nilai"].transform("sum").where(lambda s: s != 0, 1)
            df_plot_usia["Persen"] = (df_plot_usia["Nilai"] / denom) * 100
            fig1 = px.bar(df_plot_usia, x="Kelompok Usia", y="Persen", color="Kategori", barmode="stack",
                          labels={"Persen":"Persen (%)"})
        else:
            fig1 = px.bar(df_plot_usia, x="Kelompok Usia", y="Nilai", color="Kategori", barmode="stack",
                          labels={"Nilai":"Jumlah"})
        st.plotly_chart(fig1, use_container_width=True)

        N = st.slider("Tampilkan Top-N Cabang (berdasarkan total kasus)", 5, max(5, min(30, len(rekap_cabang))), 10)
        rekap_cabang["Total"] = rekap_cabang[["Hemofilia A","Hemofilia B","Hemofilia Tipe Lain","vWD Tipe 1","vWD Tipe 2"]].sum(axis=1)
        top_cabang = rekap_cabang.nlargest(N, "Total").drop(columns=["Total"])
        df_plot_cbg = top_cabang.melt(
            id_vars="HMHI Cabang",
            value_vars=["Hemofilia A","Hemofilia B","Hemofilia Tipe Lain","vWD Tipe 1","vWD Tipe 2"],
            var_name="Kategori", value_name="Nilai"
        )
        if st.checkbox("Normalize Top-N Cabang ke Persen", value=False, key="persen_cabang"):
            denom2 = df_plot_cbg.groupby("HMHI Cabang")["Nilai"].transform("sum").where(lambda s: s != 0, 1)
            df_plot_cbg["Persen"] = (df_plot_cbg["Nilai"] / denom2) * 100
            fig2 = px.bar(df_plot_cbg, x="HMHI Cabang", y="Persen", color="Kategori", barmode="stack",
                          labels={"Persen":"Persen (%)"})
        else:
            fig2 = px.bar(df_plot_cbg, x="HMHI Cabang", y="Nilai", color="Kategori", barmode="stack",
                          labels={"Nilai":"Jumlah"})
        st.plotly_chart(fig2, use_container_width=True)

    # Info kecil kondisi join
    if not table_exists(ORG_TABLE):
        st.info(f"ℹ️ Tabel '{ORG_TABLE}' belum ada. Kolom **HMHI Cabang** ditampilkan kosong.")
    else:
        hmhi_col = find_hmhi_cabang_col()
        if not hmhi_col:
            st.info("ℹ️ Kolom **HMHI Cabang** tidak ditemukan di tabel identitas_organisasi. "
                    "Direkomendasikan menamai kolomnya `hmhi_cabang`.")

if __name__ == "__main__":
    main()
