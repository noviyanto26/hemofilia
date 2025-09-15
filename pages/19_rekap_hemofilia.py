# 19_rekap_hemofilia.py — Postgres/Supabase rework
import io
import re
from pathlib import Path

import pandas as pd
import streamlit as st

st.set_page_config(page_title="Rekapitulasi Hemofilia", page_icon="📄", layout="wide")
st.title("📄 Rekapitulasi Data Hemofilia — Multi-sheet Excel")

# ======================== Konektor DB (via db.py) ========================
# Kompatibilitas: sebagian repo memakai exec_sql, bukan execute
try:
    from db import fetch_df as pg_fetch_df, execute as pg_execute
except ImportError:
    from db import fetch_df as pg_fetch_df, exec_sql as pg_execute  # type: ignore

# ======================== Util schema ========================
PUBLIC_SCHEMA = "public"
IDENTITAS_TABLE = f"{PUBLIC_SCHEMA}.identitas_organisasi"


def list_tables() -> list[str]:
    """Ambil daftar tabel di schema public (kecuali internal backup/new)."""
    df = pg_fetch_df(
        """
        SELECT tablename
        FROM pg_catalog.pg_tables
        WHERE schemaname = :schema
          AND tablename NOT LIKE '%_backup'
          AND tablename NOT LIKE '%_new'
          AND tablename NOT LIKE 'pg_%'
          AND tablename NOT LIKE 'sql_%'
        ORDER BY tablename
        """,
        {"schema": PUBLIC_SCHEMA},
    )
    return df["tablename"].astype(str).tolist() if not df.empty else []


def read_table_public(table: str) -> pd.DataFrame:
    """SELECT * FROM public.<table>. Identifier disanitasi dengan whitelist dari list_tables()."""
    valid = set(list_tables())
    if table not in valid:
        return pd.DataFrame()
    sql = f'SELECT * FROM "{PUBLIC_SCHEMA}"."{table}"'  # quote identifier aman
    return pg_fetch_df(sql)


def read_identitas_for_join() -> pd.DataFrame:
    """Ambil kolom kunci dari identitas_organisasi untuk keperluan join/alias."""
    try:
        df = pg_fetch_df(
            f"""
            SELECT kode_organisasi, hmhi_cabang, kota_cakupan_cabang
            FROM {IDENTITAS_TABLE}
            """
        )
    except Exception:
        return pd.DataFrame()

    if df.empty or "kode_organisasi" not in df.columns:
        return pd.DataFrame()

    d = df.copy()
    d["kode_organisasi"] = d["kode_organisasi"].astype(str)
    d["Propinsi"] = d.get("kota_cakupan_cabang", pd.Series(dtype=str)).astype(str)
    return d[["kode_organisasi", "hmhi_cabang", "kota_cakupan_cabang", "Propinsi"]].drop_duplicates()


DF_IO = read_identitas_for_join()
HAS_IO = not DF_IO.empty

# ======================== Alias: nama sheet per tabel ========================
TABLE_ALIASES = {
    "identitas_organisasi": "Identitas Organisasi",
    "jumlah_individu_hemofilia": "Jumlah Individu dengan Hemofilia",
    "anak_hemofilia_berat": "Penyandang Hemofilia Anak (Di bawah 18 Tahun) — Tingkat Berat (<1%)",
    "hemo_berat_prophylaxis_usia": "Persentase Penyandang Hemofilia Berat dengan Prophylaxis Berdasarkan Usia",
    "hemofilia_inhibitor": "Jumlah Penyandang Hemofilia dengan Inhibitor",
    "infeksi_transfusi_darah": "Jumlah Penyandang Hemofilia Terinfeksi Penyakit Menular Melalui Transfusi Darah",
    "informasi_donasi": "Informasi Donasi",
    "kelompok_usia": "Data Berdasarkan Kelompok Usia",
    "kematian_hemofilia_2024kini": "Jumlah Kematian Penyandang Hemofilia (1 Januari 2024–Sekarang)",
    "ketersediaan_produk_replacement": "Ketersediaan Produk Replacement Therapy",
    "pasien_nonfaktor_inhibitor": "Pasien Pengguna Nonfaktor Dengan Inhibitor",
    "pasien_nonfaktor_tanpa_inhibitor": "Pasien Pengguna Nonfaktor Tanpa Inhibitor",
    "penanganan_kesehatan": "Penanganan Kesehatan",
    "perkembangan_pelayanan_penanganan": "Perkembangan Pelayanan Penanganan Hemofilia",
    "rs_penangan_hemofilia": "Rumah Sakit yang Menangani Hemofilia",
    "vwd_berat_jumlah": "Jumlah Penyandang VWD Berat (Tipe 3)",
    "vwd_usia_gender": "Data Penyandang von Willebrand Disease (vWD) — per Kelompok Usia & Jenis Kelamin",
}

# ======================== Filter kandidat tabel (opsional) ========================
module_files = [
    "1_identitas_organisasi.py",
    "2_jumlah_individu_hemofilia.py",
    "3_berdasarkan_kelompok_usia.py",
    "4_data_penyandang_vwd.py",
    "5_berdasarkan_jenis_kelamin.py",
    "6_tingkat_hemofilia_jenis_kelamin.py",
    "7_penyandang_hemofilia_anak_berat.py",
    "8_jumlah_penyandang_vwd_berat.py",
    "9_hemofilia_inhibitor.py",
    "10_pasien_nonfaktor.py",
    "11_rs_penangan_hemofilia.py",
    "12_replacement_therapy.py",
    "13_penanganan_kesehatan.py",
    "14_hemo_berat_prophylaxis_usia.py",
    "15_perkembangan_pelayanan_penanganan.py",
    "16_informasi_donasi.py",
    "17_infeksi_transfusi_darah.py",
    "18_kematian_hemofilia_2024_sekarang.py",
]


def file_to_keywords(fname: str) -> list[str]:
    base = Path(fname).stem
    base = re.sub(r"^\d+_", "", base)
    tokens = re.split(r"[^a-zA-Z0-9]+", base)
    return [t for t in tokens if t]


flat_keywords = set()
for f in module_files:
    toks = file_to_keywords(f)
    for t in toks:
        flat_keywords.add(t.lower())
    joined = "_".join(toks).lower()
    if joined:
        flat_keywords.add(joined)

existing_tables = list_tables()
st.caption(f"📋 Tabel terdeteksi (schema public): {len(existing_tables)}")


def matches_module_keywords(tname: str) -> bool:
    lname = tname.lower()
    return any(k in lname for k in flat_keywords)


candidates_from_keywords = [t for t in existing_tables if matches_module_keywords(t)]
candidates = candidates_from_keywords if candidates_from_keywords else existing_tables
# Sembunyikan tabel tingkat_hemofilia_jk dari pilihan default (kalau ada)
tables_default = [t for t in candidates if t != "tingkat_hemofilia_jk"]


tables_selected = st.multiselect(
    "Pilih tabel untuk diekspor (default: sesuai nama modul; bila kosong → semua tabel):",
    options=sorted(existing_tables),
    default=sorted(tables_default),
)
if not tables_selected:
    tables_selected = existing_tables

# ======================== Per-tabel: proses khusus & alias ========================
HIDE_COLS_COMMON = {"id", "kode_organisasi", "created_at", "is_total_row"}


def join_with_hmhi(df: pd.DataFrame) -> pd.DataFrame:
    """Join dengan DF_IO dan menambahkan HMHI Cabang (jika ada)."""
    if df.empty or not HAS_IO or "kode_organisasi" not in df.columns:
        return df
    d = df.copy()
    d["kode_organisasi"] = d["kode_organisasi"].astype(str)
    merged = d.merge(DF_IO[["kode_organisasi", "hmhi_cabang"]], on="kode_organisasi", how="left", suffixes=("", "_io"))
    # Gunakan nama display konsisten
    merged.rename(columns={"hmhi_cabang": "HMHI Cabang"}, inplace=True)
    return merged


def reorder_cols(df: pd.DataFrame, hide_cols: set) -> pd.DataFrame:
    all_cols = [c for c in df.columns if c not in hide_cols and not c.endswith("_io")]
    ordered: list[str] = []
    if "HMHI Cabang" in all_cols:
        ordered.append("HMHI Cabang")
    for col in all_cols:
        if col not in ordered:
            ordered.append(col)
    return df[ordered]


# --- Existing processing functions (disesuaikan agar tidak bergantung SQLite) ---

def process_identitas(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    d["Propinsi"] = d.get("kota_cakupan_cabang", pd.Series(dtype=str)).astype(str)
    alias_map = {
        "hmhi_cabang": "HMHI Cabang",
        "diisi_oleh": "Diisi Oleh",
        "jabatan": "Jabatan",
        "no_telp": "No Telp",
        "email": "Email",
        "sumber_data": "SumberData",
        "tanggal": "Tanggal",
        "catatan": "Catatan",
    }
    d.rename(columns=alias_map, inplace=True)
    cols = [c for c in d.columns if c not in HIDE_COLS_COMMON]
    head_cols = [c for c in ["HMHI Cabang", "Diisi Oleh", "Jabatan"] if c in cols]
    ordered: list[str] = []
    ordered.extend(head_cols)
    if "Propinsi" in cols:
        ordered.append("Propinsi")
    for c in cols:
        if c not in ordered:
            ordered.append(c)
    return d[ordered]


def process_jumlah_individu(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    alias_map = {
        "Propinsi": "Propinsi",
        "jumlah_total_ab": "Jumlah total penyandang hemofilia A dan B",
        "hemofilia_lain": "Hemofilia lain/tidak dikenal",
        "terduga": "Terduga hemofilia/diagnosis belum ditegakkan",
        "vwd": "Von Willebrand Disease (vWD)",
        "lainnya": "Kelainan pembekuan darah genetik lainnya",
    }
    d.rename(columns=alias_map, inplace=True)
    cols = [c for c in d.columns if c not in HIDE_COLS_COMMON]
    ordered: list[str] = []
    for c in ["HMHI Cabang", "Propinsi"]:
        if c in cols:
            ordered.append(c)
    preferred_tail = [
        "Jumlah total penyandang hemofilia A dan B",
        "Hemofilia lain/tidak dikenal",
        "Terduga hemofilia/diagnosis belum ditegakkan",
        "Von Willebrand Disease (vWD)",
        "Kelainan pembekuan darah genetik lainnya",
    ]
    for c in preferred_tail:
        if c in cols and c not in ordered:
            ordered.append(c)
    for c in cols:
        if c not in ordered:
            ordered.append(c)
    return d[ordered]


def process_anak_berat(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    d.rename(columns={"kategori": "Kategori", "berat": "Berat (<1%)"}, inplace=True)
    hide_extra = {"kode_organisasi", "created_at", "is_total_row"}
    cols = [c for c in d.columns if c not in hide_extra]
    return d[cols]


def process_hemo_berat_prophylaxis_usia(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    alias_map = {
        "jenis": "Jenis",
        "persen_0_18": "0-18 tahun",
        "persen_gt_18": "lebih dari 18 tahun",
        "frekuensi": "Frekuensi",
        "produk": "Produk yang digunakan",
        "tidak_ada_data": "Tidak ada data",
        "dosis_per_kedatangan": "Dosis diterima (IU)/kedatangan",
        "estimasi_data_real": "Estimasi/Data real",
    }
    d.rename(columns=alias_map, inplace=True)
    hide_extra = {"id", "kode_organisasi", "created_at"}
    cols = [c for c in d.columns if c not in hide_extra]
    return d[cols]


def process_hemofilia_inhibitor(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    alias_map = {
        "Jenis Hemofilia": "Jenis Hemofilia",
        "label": "Jenis Hemofilia",
        "terdiagnosis_aktif": "Terdiagnosis Inhibitor Aktif",
        "kasus_baru_2025": "Kasus baru 2025",
        "penanganan": "Penanganan",
    }
    d.rename(columns=alias_map, inplace=True)
    return reorder_cols(d, HIDE_COLS_COMMON)


def process_infeksi_transfusi_darah(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    d.rename(
        columns={
            "kasus": "Kasus",
            "jml_hepatitis_c": "Jumlah Hepatitis C",
            "jml_hiv": "Jumlah HIV",
            "penyakit_menular_lainnya": "Penyakit Menular Lainnya",
        },
        inplace=True,
    )
    visible_cols = [c for c in d.columns if c not in HIDE_COLS_COMMON]
    return d[visible_cols]


def process_informasi_donasi(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    d.rename(
        columns={
            "jenis_donasi": "Jenis Donasi",
            "merk": "Merek",
            "jumlah_total_iu_setahun": "Jumlah Total IU/tahun",
            "kegunaan": "Kegunaan",
        },
        inplace=True,
    )
    visible_cols = [c for c in d.columns if c not in HIDE_COLS_COMMON]
    return d[visible_cols]


def process_kelompok_usia(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    d.rename(
        columns={
            "kelompok_usia": "Kelompok Usia",
            "ha_ringan": "Hemofilia A - Ringan",
            "ha_sedang": "Hemofilia A - Sedang",
            "ha_berat": "Hemofilia A - Berat",
            "hb_ringan": "Hemofilia B - Ringan",
            "hb_sedang": "Hemofilia B - Sedang",
            "hb_berat": "Hemofilia B - Berat",
            "hemo_tipe_lain": "Hemofilia Tipe Lain",
            "vwd_tipe1": "vWD - Tipe 1",
            "vwd_tipe2": "vWD - Tipe 2",
        },
        inplace=True,
    )
    return reorder_cols(d, HIDE_COLS_COMMON)


def process_kematian_hemofilia(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    d.rename(
        columns={
            "penyebab_kematian": "Penyebab Kematian",
            "perdarahan": "Perdarahan",
            "gangguan_hati": "Gangguan Hati",
            "hiv": "HIV",
            "penyebab_lain": "Penyebab Lain",
            "tahun_kematian": "Tahun Kematian",
        },
        inplace=True,
    )
    return reorder_cols(d, HIDE_COLS_COMMON)


def process_ketersediaan_produk(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    d.rename(
        columns={
            "produk": "Produk",
            "ketersediaan": "Ketersediaan",
            "digunakan": "Digunakan",
            "merk": "Merk",
            "jumlah_pengguna": "Jumlah Pengguna",
            "jumlah_iu_per_kemasan": "Jumlah IU/vial dalam 1 kemasan",
            "harga": "Harga",
        },
        inplace=True,
    )
    return reorder_cols(d, HIDE_COLS_COMMON)


def process_pasien_nonfaktor_inhibitor(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    d.rename(
        columns={
            "jenis_penanganan": "Jenis Penanganan",
            "ketersediaan": "Ketersediaan",
            "jumlah_pengguna": "Jumlah Pengguna",
        },
        inplace=True,
    )
    return reorder_cols(d, HIDE_COLS_COMMON)


def process_pasien_nonfaktor_tanpa_inhibitor(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    d.rename(
        columns={
            "jenis_penanganan": "Jenis Penanganan",
            "ketersediaan": "Ketersediaan",
            "jumlah_pengguna": "Jumlah Pengguna",
        },
        inplace=True,
    )
    return reorder_cols(d, HIDE_COLS_COMMON)


def process_penanganan_kesehatan(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    d.rename(
        columns={
            "jenis_hemofilia": "Jenis Hemofilia",
            "jenis_penanganan": "Jenis Penanganan",
            "layanan_rawat": "Layanan Rawat",
            "dosis_per_orang_per_kedatangan": "Dosis/orang/kedatangan (IU)",
            "frekuensi": "Frekuensi",
        },
        inplace=True,
    )
    return reorder_cols(d, HIDE_COLS_COMMON)

# === Tambahan sesuai permintaan ===

def process_perkembangan_pelayanan(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    d.rename(
        columns={
            "jenis": "Jenis",
            "jumlah_terapi_gen": "Jumlah Penyandang yang telah menjalani Terapi Gen",
            "tahun": "Tahun",
            "nama_rumah_sakit": "Nama Rumah Sakit",
            "lokasi": "Kab/Kota",
            "propinsi": "Propinsi",
        },
        inplace=True,
    )
    return reorder_cols(d, HIDE_COLS_COMMON)


def process_rs_penangan_hemofilia(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    d.rename(
        columns={
            "nama_rumah_sakit": "Nama Rumah Sakit",
            "tipe_rs": "Tipe RS",
            "dokter_hematologi": "Terdapat Dokter Hematologi",
            "tim_terpadu": "Terdapat Tim Terpadu",
            "lokasi": "Kab/Kota",
            "propinsi": "Propinsi",
        },
        inplace=True,
    )
    return reorder_cols(d, HIDE_COLS_COMMON)


def process_vwd_berat_jumlah(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    d.rename(
        columns={
            "label": "Kelainan",
            "jumlah": "Jumlah Penyandang",
            "jumlah_medis": "Jumlah Penyandang vWD Berat yang Menerima Penanganan Medis",
        },
        inplace=True,
    )
    return reorder_cols(d, HIDE_COLS_COMMON)


def process_vwd_usia_gender(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = join_with_hmhi(df)
    d.rename(
        columns={
            "kelompok_usia": "Kelompok Usia",
            "laki_laki": "Laki-laki",
            "perempuan": "Perempuan",
            "jk_tidak_terdata": "Jenis Kelamin Tidak Terdata",
            "total": "Total",
        },
        inplace=True,
    )
    hide_cols = HIDE_COLS_COMMON.copy()
    if "is_total_row" in hide_cols:
        hide_cols.remove("is_total_row")  # 'total' adalah kolom data, bukan flag
    return reorder_cols(d, hide_cols)


def process_generic(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    d = df.copy()
    cols = [c for c in d.columns if c not in HIDE_COLS_COMMON]
    return d[cols]


def process_table(tname: str, raw: pd.DataFrame) -> pd.DataFrame:
    if tname == "identitas_organisasi":
        return process_identitas(raw)
    if tname == "jumlah_individu_hemofilia":
        return process_jumlah_individu(raw)
    if tname == "anak_hemofilia_berat":
        return process_anak_berat(raw)
    if tname == "hemo_berat_prophylaxis_usia":
        return process_hemo_berat_prophylaxis_usia(raw)
    if tname == "hemofilia_inhibitor":
        return process_hemofilia_inhibitor(raw)
    if tname == "infeksi_transfusi_darah":
        return process_infeksi_transfusi_darah(raw)
    if tname == "informasi_donasi":
        return process_informasi_donasi(raw)
    if tname == "kelompok_usia":
        return process_kelompok_usia(raw)
    if tname == "kematian_hemofilia_2024kini":
        return process_kematian_hemofilia(raw)
    if tname == "ketersediaan_produk_replacement":
        return process_ketersediaan_produk(raw)
    if tname == "pasien_nonfaktor_inhibitor":
        return process_pasien_nonfaktor_inhibitor(raw)
    if tname == "pasien_nonfaktor_tanpa_inhibitor":
        return process_pasien_nonfaktor_tanpa_inhibitor(raw)
    if tname == "penanganan_kesehatan":
        return process_penanganan_kesehatan(raw)

    if tname == "perkembangan_pelayanan_penanganan":
        return process_perkembangan_pelayanan(raw)
    if tname == "rs_penangan_hemofilia":
        return process_rs_penangan_hemofilia(raw)
    if tname == "vwd_berat_jumlah":
        return process_vwd_berat_jumlah(raw)
    if tname == "vwd_usia_gender":
        return process_vwd_usia_gender(raw)

    return process_generic(raw)

# ======================== Pratinjau per tabel ========================
with st.expander("🔎 Pratinjau cepat (maks 200 baris per tabel)"):
    for t in sorted(tables_selected):
        raw = read_table_public(t)
        processed = process_table(t, raw)
        prev = processed.head(200)
        label = TABLE_ALIASES.get(t, t)
        st.markdown(f"**{label}** — menampilkan {len(prev)} dari total {len(processed)} baris")
        st.dataframe(prev, use_container_width=True)

# ======================== Util: formatting Excel ========================

def autosize_and_print_setup(writer, sheet_name: str, df: pd.DataFrame):
    ws = writer.sheets[sheet_name]
    for idx, col in enumerate(df.columns):
        try:
            max_len = max([len(str(col))] + [len(str(x)) for x in df[col].astype(str).values[:1000]])
        except Exception:
            max_len = len(str(col))
        ws.set_column(idx, idx, min(max_len + 2, 50))
    ws.freeze_panes(1, 0)
    ws.autofilter(0, 0, max(len(df), 1), max(len(df.columns) - 1, 0))
    ws.set_portrait()
    ws.set_paper(9)      # A4
    ws.fit_to_pages(1, 1)
    ws.set_margins(left=0.3, right=0.3, top=0.5, bottom=0.5)

# ======================== Buat Excel: 1 sheet per tabel ========================
st.subheader("⬇️ Rekapitulasi - Unduh Excel (Multi-Sheet)")
buf = io.BytesIO()
with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
    for t in tables_selected:
        raw = read_table_public(t)
        df = process_table(t, raw)
        safe_name = TABLE_ALIASES.get(t, t)
        safe_name = safe_name.replace("/", "_").replace("\\", "_")[:31] or "Sheet"
        n = 2
        base = safe_name[:29]
        while safe_name in writer.sheets:
            safe_name = f"{base}_{n}"
            n += 1
        (df if not df.empty else pd.DataFrame()).to_excel(writer, index=False, sheet_name=safe_name)
        autosize_and_print_setup(writer, safe_name, df if not df.empty else pd.DataFrame())

st.download_button(
    "Unduh file Excel",
    data=buf.getvalue(),
    file_name="rekapitulasi_hemofilia.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

# ======================== Catatan ========================
if not HAS_IO:
    st.warning(
        "Tabel 'identitas_organisasi' tidak tersedia/lengkap untuk keperluan join.\n"
        "- Sheet 'Identitas Organisasi' tetap ditampilkan dengan kolom yang ada.\n"
        "- Sheet lain yang membutuhkan HMHI/Propinsi akan tampil tanpa kolom tersebut."
    )
