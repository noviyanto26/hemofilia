# pages/11_rs_penangan_hemofilia_pg.py
import io
from datetime import datetime
import os

import pandas as pd
import psycopg2
import streamlit as st

# =========================
# Konfigurasi Halaman
# =========================
st.set_page_config(page_title="Rumah Sakit Penangan Hemofilia", page_icon="🏥", layout="wide")
st.title("🏥 Rumah Sakit yang Menangani Hemofilia")

# =========================
# Koneksi Postgres (Supabase)
# =========================
def pg_connect():
    # Ambil dari st.secrets atau env var
    dsn = os.environ.get("DATABASE_URL") or st.secrets.get("DATABASE_URL")
    if not dsn:
        st.stop()  # Berhenti dengan jelas
    # Supabase biasanya butuh sslmode=require (masukkan di URL)
    return psycopg2.connect(dsn)

def fetch_df(sql: str, params=None) -> pd.DataFrame:
    with pg_connect() as conn:
        df = pd.read_sql_query(sql, conn, params=params)
    return df

def exec_many(sql: str, rows: list[tuple]) -> tuple[int, list]:
    """Eksekusi INSERT/UPSERT banyak baris. Return (n_ok, logs[tuple])."""
    ok, logs = 0, []
    with pg_connect() as conn:
        cur = conn.cursor()
        for row in rows:
            try:
                cur.execute(sql, row)
                ok += 1
                logs.append(("OK", row[0] if row else "", "Tersimpan"))
            except Exception as e:
                logs.append(("GAGAL", row[0] if row else "", str(e)))
        conn.commit()
    return ok, logs

# =========================
# Helpers: Query Data
# =========================
def load_hmhi_to_kode():
    """
    Ambil mapping hmhi_cabang -> kode_organisasi dari public.identitas_organisasi
    """
    sql = """
        SELECT kode_organisasi, hmhi_cabang
        FROM public.identitas_organisasi
        WHERE kode_organisasi IS NOT NULL AND hmhi_cabang IS NOT NULL
        ORDER BY 1 DESC
    """
    df = fetch_df(sql)
    if df.empty:
        return {}, []
    mapping = {}
    for _, r in df.iterrows():
        hmhi = str(r["hmhi_cabang"] or "").strip()
        ko = str(r["kode_organisasi"] or "").strip()
        if hmhi:
            mapping[hmhi] = ko
    return mapping, sorted(mapping.keys())

def load_rs_master():
    """
    Ambil daftar RS dari public.rumah_sakit untuk selectbox:
    label: "<kode_rs> — <nama_rs> (kota, provinsi)"
    """
    sql = """
        SELECT kode_rs, nama_rs, kota, provinsi, tipe_rs, kelas_rs, kontak
        FROM public.rumah_sakit
        ORDER BY nama_rs
    """
    df = fetch_df(sql)
    if df.empty:
        return df, {}
    def to_s(x): return "" if pd.isna(x) else str(x).strip()
    df["kode_rs"] = df["kode_rs"].apply(to_s)
    df["nama_rs"] = df["nama_rs"].apply(to_s)
    df["kota"] = df["kota"].apply(to_s)
    df["provinsi"] = df["provinsi"].apply(to_s)
    df["label"] = df.apply(lambda r: f"{r['kode_rs']} — {r['nama_rs']}"
                           + (f" ({r['kota']}, {r['provinsi']})" if (r["kota"] or r["provinsi"]) else ""), axis=1)
    label_to_kode = {r["label"]: r["kode_rs"] for _, r in df.iterrows()}
    return df, label_to_kode

def upsert_master_rs(rows: list[dict]) -> tuple[int, list]:
    """
    UPSERT ke public.rumah_sakit berdasar (kode_rs)
    rows item: {kode_rs, nama_rs, provinsi, kota, tipe_rs, kelas_rs, kontak}
    """
    sql = """
    INSERT INTO public.rumah_sakit
        (kode_rs, nama_rs, provinsi, kota, tipe_rs, kelas_rs, kontak)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (kode_rs) DO UPDATE SET
        nama_rs = EXCLUDED.nama_rs,
        provinsi = EXCLUDED.provinsi,
        kota = EXCLUDED.kota,
        tipe_rs = EXCLUDED.tipe_rs,
        kelas_rs = EXCLUDED.kelas_rs,
        kontak = EXCLUDED.kontak
    """
    tuples = [
        (
            r.get("kode_rs"), r.get("nama_rs"), r.get("provinsi"),
            r.get("kota"), r.get("tipe_rs"), r.get("kelas_rs"), r.get("kontak")
        )
        for r in rows
    ]
    return exec_many(sql, tuples)

def insert_rs_penangan(rows: list[dict]) -> tuple[int, list]:
    """
    Insert ke public.rs_penangan_hemofilia
    rows item: {kode_organisasi, kode_rs, layanan, catatan}
    `d_at` diisi NOW() dari Postgres.
    """
    sql = """
    INSERT INTO public.rs_penangan_hemofilia
        (kode_organisasi, kode_rs, d_at, layanan, catatan)
    VALUES (%s, %s, NOW(), %s, %s)
    """
    tuples = [(r["kode_organisasi"], r["kode_rs"], r.get("layanan"), r.get("catatan")) for r in rows]
    return exec_many(sql, tuples)

def read_rs_penangan_with_join(limit=500):
    sql = f"""
        SELECT
          t.id, t.d_at, t.layanan, t.catatan,
          t.kode_organisasi, io.hmhi_cabang, io.kota_cakupan_cabang,
          t.kode_rs, rs.nama_rs, rs.kota, rs.provinsi, rs.tipe_rs, rs.kelas_rs, rs.kontak
        FROM public.rs_penangan_hemofilia t
        LEFT JOIN public.identitas_organisasi io ON io.kode_organisasi = t.kode_organisasi
        LEFT JOIN public.rumah_sakit rs ON rs.kode_rs = t.kode_rs
        ORDER BY t.id DESC
        LIMIT %s
    """
    return fetch_df(sql, params=[limit])

# =========================
# UI
# =========================
tab_input, tab_data, tab_master = st.tabs([
    "📝 Input RS Penangan", "📄 Data & Excel", "🏥 Master Rumah Sakit"
])

# ---------- TAB MASTER (untuk mengisi master bila kosong/ubah data) ----------
with tab_master:
    st.subheader("🏥 Master Rumah Sakit")
    st.caption("Gunakan editor di bawah untuk menambah/memperbarui master RS (UPSERT per Kode RS).")

    # Tampilkan data saat ini
    df_master, _ = load_rs_master()
    if df_master.empty:
        st.info("Master `public.rumah_sakit` masih kosong. Tambahkan data pada editor di bawah.")
        default_rows = 8
    else:
        view = df_master.rename(columns={
            "kode_rs": "Kode RS",
            "nama_rs": "Nama RS",
            "provinsi": "Provinsi",
            "kota": "Kota",
            "tipe_rs": "Tipe RS",
            "kelas_rs": "Kelas RS",
            "kontak": "Kontak",
        })
        order = ["Kode RS", "Nama RS", "Kota", "Provinsi", "Tipe RS", "Kelas RS", "Kontak"]
        st.dataframe(view[order], use_container_width=True)
        default_rows = 3

    # Editor master (baris dinamis)
    df_default = pd.DataFrame({
        "kode_rs": ["" for _ in range(default_rows)],
        "nama_rs": ["" for _ in range(default_rows)],
        "provinsi": ["" for _ in range(default_rows)],
        "kota": ["" for _ in range(default_rows)],
        "tipe_rs": ["" for _ in range(default_rows)],
        "kelas_rs": ["" for _ in range(default_rows)],
        "kontak": ["" for _ in range(default_rows)],
    })
    with st.form("rs_master::editor"):
        edited = st.data_editor(
            df_default,
            key="rs_master::editor_tbl",
            column_config={
                "kode_rs": st.column_config.TextColumn("Kode RS", help="Wajib unik (contoh: RS-001)"),
                "nama_rs": st.column_config.TextColumn("Nama RS", help="Wajib diisi"),
                "provinsi": st.column_config.TextColumn("Provinsi"),
                "kota": st.column_config.TextColumn("Kota"),
                "tipe_rs": st.column_config.TextColumn("Tipe RS"),
                "kelas_rs": st.column_config.TextColumn("Kelas RS"),
                "kontak": st.column_config.TextColumn("Kontak"),
            },
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
        )
        do_save_master = st.form_submit_button("💾 UPSERT Master RS")

    if do_save_master:
        rows = []
        for _, r in edited.iterrows():
            kode = str(r.get("kode_rs") or "").strip()
            nama = str(r.get("nama_rs") or "").strip()
            if not kode and not nama:
                continue
            if not kode:
                st.error("Ada baris editor master tanpa **Kode RS**.")
                st.stop()
            if not nama:
                st.error(f"Ada baris editor master Kode RS '{kode}' tetapi **Nama RS** kosong.")
                st.stop()
            rows.append({
                "kode_rs": kode,
                "nama_rs": nama,
                "provinsi": str(r.get("provinsi") or "").strip(),
                "kota": str(r.get("kota") or "").strip(),
                "tipe_rs": str(r.get("tipe_rs") or "").strip(),
                "kelas_rs": str(r.get("kelas_rs") or "").strip(),
                "kontak": str(r.get("kontak") or "").strip(),
            })
        n_ok, logs = upsert_master_rs(rows)
        if n_ok:
            st.success(f"Berhasil menyimpan {n_ok} baris master RS.")
        if logs:
            st.dataframe(pd.DataFrame(logs, columns=["Status", "Kode RS", "Keterangan"]), use_container_width=True)

# ---------- TAB INPUT RS PENANGAN ----------
with tab_input:
    st.caption("Isi data RS penangan hemofilia per HMHI cabang (menggunakan **Kode RS** dari master).")

    hmhi_map, hmhi_list = load_hmhi_to_kode()
    df_rs, label_to_kode = load_rs_master()

    if not hmhi_list:
        st.warning("Belum ada data **identitas_organisasi**. Lengkapi dulu tabel `public.identitas_organisasi` (hmhi_cabang & kode_organisasi).")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox("Pilih HMHI Cabang (Provinsi)", options=hmhi_list, key="rs::hmhi")

    if df_rs.empty:
        st.warning("Master **public.rumah_sakit** kosong. Tambahkan data di tab **🏥 Master Rumah Sakit** terlebih dahulu.")
    option_labels = [""] + sorted(label_to_kode.keys())

    # Editor input transaksi
    df_default = pd.DataFrame({
        "rs_label": [""] * 5,
        "layanan": [""] * 5,
        "catatan": [""] * 5,
    })

    with st.form("rs::form_editor"):
        edited = st.data_editor(
            df_default,
            key="rs::editor_tbl",
            column_config={
                "rs_label": st.column_config.SelectboxColumn(
                    "Rumah Sakit (Kode — Nama RS (Kota, Provinsi))",
                    options=option_labels, required=False
                ),
                "layanan": st.column_config.TextColumn("Layanan", help="Contoh: Klinik hemofilia; IGD 24 jam"),
                "catatan": st.column_config.TextColumn("Catatan (opsional)"),
            },
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
            disabled=df_rs.empty
        )

        # Pratinjau baris yang akan disimpan
        if not edited.empty and not df_rs.empty:
            preview = []
            for _, r in edited.iterrows():
                lbl = str(r.get("rs_label") or "").strip()
                kode_rs = label_to_kode.get(lbl, "")
                nama_rs = ""
                if kode_rs:
                    row = df_rs[df_rs["kode_rs"].str.casefold() == kode_rs.casefold()]
                    if not row.empty:
                        nama_rs = row.iloc[0]["nama_rs"]
                preview.append({
                    "Kode RS": kode_rs,
                    "Nama RS": nama_rs,
                    "Layanan": r.get("layanan", ""),
                    "Catatan": r.get("catatan", ""),
                })
            st.caption("Pratinjau baris yang akan disimpan:")
            st.dataframe(pd.DataFrame(preview), use_container_width=True)

        submitted = st.form_submit_button("💾 Simpan", disabled=df_rs.empty)

    if submitted:
        if not selected_hmhi:
            st.error("Pilih HMHI cabang terlebih dahulu.")
        else:
            kode_organisasi = hmhi_map.get(selected_hmhi)
            if not kode_organisasi:
                st.error("Kode organisasi tidak ditemukan untuk HMHI cabang terpilih.")
            else:
                rows = []
                for _, r in edited.iterrows():
                    lbl = str(r.get("rs_label") or "").strip()
                    if not lbl:
                        continue
                    kode_rs = label_to_kode.get(lbl)
                    if not kode_rs:
                        st.warning(f"Lewati baris tanpa Kode RS valid: '{lbl}'")
                        continue
                    rows.append({
                        "kode_organisasi": kode_organisasi,
                        "kode_rs": kode_rs,
                        "layanan": str(r.get("layanan") or "").strip(),
                        "catatan": str(r.get("catatan") or "").strip(),
                    })
                if not rows:
                    st.info("Tidak ada baris valid untuk disimpan.")
                else:
                    n_ok, logs = insert_rs_penangan(rows)
                    if n_ok:
                        st.success(f"Berhasil menyimpan {n_ok} baris untuk **{selected_hmhi}**.")
                    if logs:
                        st.dataframe(pd.DataFrame(logs, columns=["Status", "Kode/Label", "Keterangan"]), use_container_width=True)

# ---------- TAB DATA ----------
with tab_data:
    st.subheader("📄 Data Tersimpan")
    df = read_rs_penangan_with_join(limit=500)

    if df.empty:
        st.info("Belum ada data.")
    else:
        view = df.rename(columns={
            "hmhi_cabang": "HMHI cabang",
            "kota_cakupan_cabang": "Kota/Provinsi Cakupan Cabang",
            "d_at": "Waktu Input (UTC)",
            "kode_rs": "Kode RS",
            "nama_rs": "Nama RS",
            "kota": "Kota",
            "provinsi": "Provinsi",
            "tipe_rs": "Tipe RS",
            "kelas_rs": "Kelas RS",
            "kontak": "Kontak",
            "layanan": "Layanan",
            "catatan": "Catatan",
        })
        order = [
            "HMHI cabang", "Kota/Provinsi Cakupan Cabang", "Waktu Input (UTC)",
            "Kode RS", "Nama RS", "Kota", "Provinsi", "Tipe RS", "Kelas RS", "Kontak",
            "Layanan", "Catatan",
        ]
        order = [c for c in order if c in view.columns]
        st.dataframe(view[order], use_container_width=True)

        # Unduh Excel (data tersimpan)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="xlsxwriter") as w:
            view[order].to_excel(w, index=False, sheet_name="RS_Penangan_Hemofilia")
        st.download_button(
            "⬇️ Unduh Excel (Data Tersimpan)",
            buf.getvalue(),
            file_name="rs_penangan_hemofilia.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            key="rs::download"
        )
