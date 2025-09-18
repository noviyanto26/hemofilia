# 19_rekap_hemofilia.py
import io
from datetime import datetime

import pandas as pd
import streamlit as st

# --- import util/fungsi asli kamu di sini (tetap seperti sebelumnya) ---
# from db import read_sql_df, exec_sql, get_engine, ping
# from sqlalchemy import text
# ... fungsi2/konstanta/mapper TABLE_ALIASES, read_table_public, process_table,
# autosize_and_print_setup, dsb. TIDAK diubah ...

st.set_page_config(page_title="Rekap Hemofilia", page_icon="🩸", layout="wide")
st.title("🩸 Rekap Data Hemofilia")

# ======================================================================================
# Helper baru: pastikan semua kolom datetime menjadi timezone-naive sebelum ke Excel
# ======================================================================================
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_datetime64tz_dtype,
    is_object_dtype,
)

def make_excel_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Excel (xlsxwriter) tidak menerima datetime yang timezone-aware.
    Fungsi ini mengubah seluruh kolom datetime menjadi timezone-naive.
    """
    if df is None or df.empty:
        return df

    d = df.copy()
    for c in d.columns:
        s = d[c]
        try:
            # Sudah datetime bertz
            if is_datetime64tz_dtype(s):
                d[c] = s.dt.tz_localize(None)
                continue

            # Object: coba parse ke datetime(utc) lalu buang tz
            if is_object_dtype(s):
                s2 = pd.to_datetime(s, errors="ignore", utc=True)
                if is_datetime64_any_dtype(s2):
                    if is_datetime64tz_dtype(s2):
                        s2 = s2.dt.tz_localize(None)
                    d[c] = s2
        except Exception:
            # Jangan biarkan 1 kolom menggagalkan keseluruhan
            pass
    return d

# ======================================================================================
# Seluruh kode/komponen UI asli kamu di atas dan di bawah ini TIDAK diubah
# (filter, pilihan tabel, read_table_public, process_table, dsb.)
# ======================================================================================

# --- Contoh UI filter/daftar tabel (pertahankan punyamu sendiri) ---
# Misal:
TABLE_ALIASES = {
    # contoh alias; gunakan mapping asli kamu
    "kelompok_usia": "Kelompok Usia",
    "jumlah_individu_hemofilia": "Jumlah Individu Hemofilia",
    # ... dst ...
}

# Misal input multiselect daftar tabel (gunakan yang sudah ada pada file kamu)
all_tables = list(TABLE_ALIASES.keys())
tables_selected = st.multiselect(
    "Pilih tabel untuk diekspor",
    options=[TABLE_ALIASES.get(t, t) for t in all_tables],
    default=[TABLE_ALIASES.get(t, t) for t in all_tables[:1]],
)

# Utility untuk balik alias -> nama tabel (agar kompatibel dengan kode lama)
alias_to_table = {v: k for k, v in TABLE_ALIASES.items()}
tables_selected = [alias_to_table.get(x, x) for x in tables_selected]

# --------------------------------------------------------------------------------------
# Placeholder: gunakan implementasi asli kamu untuk membaca & memproses setiap tabel
# --------------------------------------------------------------------------------------
def read_table_public(table_name: str) -> pd.DataFrame:
    """
    Ganti isi fungsi ini dengan implementasi asli kamu (tidak diubah).
    Di sini hanya placeholder agar file ini berdiri sendiri.
    """
    return pd.DataFrame()

def process_table(table_name: str, df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Ganti isi fungsi ini dengan implementasi asli kamu (tidak diubah).
    """
    return df_raw

def autosize_and_print_setup(writer, sheet_name: str, df: pd.DataFrame):
    """
    Ganti isi fungsi ini dengan implementasi asli kamu (tidak diubah).
    Biasanya: autosize kolom + set print area/option.
    """
    try:
        ws = writer.sheets[sheet_name]
        # autosize sederhana: lebar = max(len header, len konten)
        for idx, col in enumerate(df.columns, start=0):
            try:
                max_len = max(
                    [len(str(col))] + [len(str(x)) for x in df[col].astype(str).values.tolist()]
                )
                ws.set_column(idx, idx, min(max_len + 2, 60))
            except Exception:
                pass
    except Exception:
        pass

# ======================================================================================
# TOMBOL/PROSES EKSPOR EXCEL — INI BAGIAN YANG DIBETULKAN (pakai make_excel_safe)
# ======================================================================================
st.subheader("⬇️ Rekapitulasi - Unduh Excel (Multi-Sheet)")

if st.button("📤 Buat & Unduh Excel", type="primary"):
    if not tables_selected:
        st.warning("Pilih minimal satu tabel terlebih dahulu.")
    else:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
            for t in tables_selected:
                raw = read_table_public(t)
                df = process_table(t, raw)

                # Nama sheet aman
                safe_name = TABLE_ALIASES.get(t, t)
                safe_name = safe_name.replace("/", "_").replace("\\", "_").strip() or "Sheet"
                safe_name = safe_name[:31]  # batas Excel
                # hindari duplikat nama sheet
                base = safe_name[:29]
                n = 2
                while safe_name in writer.sheets:
                    safe_name = f"{base}_{n}"
                    n += 1

                # >>>>> PERBAIKAN: pastikan datetime NAIVE sebelum to_excel <<<<<
                df_xl = make_excel_safe(df if df is not None else pd.DataFrame())

                # Tulis sheet
                (df_xl if not df_xl.empty else pd.DataFrame()).to_excel(
                    writer, index=False, sheet_name=safe_name
                )
                autosize_and_print_setup(writer, safe_name, df_xl if df_xl is not None else pd.DataFrame())

        st.download_button(
            "💾 Unduh File Excel",
            data=buf.getvalue(),
            file_name=f"rekap_hemofilia_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

# ======================================================================================
# Sisa UI/komponen lain pada file asli kamu tetap sama
# ======================================================================================
