import io
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# =========================
# Konfigurasi & Konstanta
# =========================
st.set_page_config(page_title="Rumah Sakit Penangan Hemofilia", page_icon="🏥", layout="wide")
st.title("🏥 Rumah Sakit yang Menangani Hemofilia")

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = str((BASE_DIR / "hemofilia.db").resolve())

TABLE_RS_PENANGAN = "rs_penangan_hemofilia"
TABLE_RS_MASTER = "rumah_sakit"
TABLE_ORG = "identitas_organisasi"

# ======================== Util DB ========================
def connect():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def ensure_identitas_schema():
    with connect() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_ORG} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT UNIQUE,
                hmhi_cabang TEXT,
                kota_cakupan_cabang TEXT
            )
        """)
        conn.commit()

def ensure_rumah_sakit_schema():
    """Selaras dengan skema Postgres."""
    with connect() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_RS_MASTER} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_rs   TEXT UNIQUE NOT NULL,
                nama_rs   TEXT NOT NULL,
                provinsi  TEXT,
                kota      TEXT,
                tipe_rs   TEXT,
                kelas_rs  TEXT,
                kontak    TEXT
            )
        """)
        conn.commit()

def ensure_rs_penangan_schema():
    """Tabel transaksi RS penangan hemofilia (mengikuti Postgres)."""
    with connect() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {TABLE_RS_PENANGAN} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kode_organisasi TEXT,
                kode_rs TEXT,
                d_at TEXT NOT NULL,
                layanan TEXT,
                catatan TEXT,
                FOREIGN KEY (kode_organisasi) REFERENCES {TABLE_ORG}(kode_organisasi),
                FOREIGN KEY (kode_rs) REFERENCES {TABLE_RS_MASTER}(kode_rs)
            )
        """)
        conn.commit()

# ======================== Helpers ========================
def load_hmhi_to_kode():
    """Map hmhi_cabang -> kode_organisasi."""
    with connect() as conn:
        df = pd.read_sql_query(
            f"SELECT kode_organisasi, hmhi_cabang FROM {TABLE_ORG} ORDER BY id DESC",
            conn
        )
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
    with connect() as conn:
        try:
            df = pd.read_sql_query(
                f"SELECT kode_rs, nama_rs, kota, provinsi, tipe_rs, kelas_rs FROM {TABLE_RS_MASTER} ORDER BY nama_rs",
                conn
            )
        except Exception:
            df = pd.DataFrame(columns=["kode_rs", "nama_rs", "kota", "provinsi", "tipe_rs", "kelas_rs"])
    df["kode_rs"] = df["kode_rs"].astype(str).str.strip()
    df["nama_rs"] = df["nama_rs"].astype(str).str.strip()
    df["kota"] = df["kota"].astype(str).str.strip()
    df["provinsi"] = df["provinsi"].astype(str).str.strip()
    # Label tampilan
    df["label"] = df.apply(
        lambda r: f"{r['kode_rs']} — {r['nama_rs']}"
                  + (f" ({r['kota']}, {r['provinsi']})" if r["kota"] or r["provinsi"] else ""),
        axis=1
    )
    label_to_kode = {row["label"]: row["kode_rs"] for _, row in df.iterrows()}
    return df, label_to_kode

def insert_rs_penangan(kode_organisasi: str, kode_rs: str, layanan: str, catatan: str):
    with connect() as conn:
        conn.execute(
            f"INSERT INTO {TABLE_RS_PENANGAN} (kode_organisasi, kode_rs, d_at, layanan, catatan) VALUES (?, ?, ?, ?, ?)",
            [kode_organisasi, kode_rs, datetime.utcnow().isoformat(), layanan, catatan]
        )
        conn.commit()

def read_rs_penangan_with_join(limit=500):
    with connect() as conn:
        return pd.read_sql_query(
            f"""
            SELECT
              t.id, t.d_at, t.layanan, t.catatan,
              t.kode_organisasi, io.hmhi_cabang, io.kota_cakupan_cabang,
              t.kode_rs, rs.nama_rs, rs.kota, rs.provinsi, rs.tipe_rs, rs.kelas_rs, rs.kontak
            FROM {TABLE_RS_PENANGAN} t
            LEFT JOIN {TABLE_ORG} io ON io.kode_organisasi = t.kode_organisasi
            LEFT JOIN {TABLE_RS_MASTER} rs ON rs.kode_rs = t.kode_rs
            ORDER BY t.id DESC
            LIMIT ?
            """,
            conn, params=[limit]
        )

def insert_master_rs_rows(rows: list[dict], upsert: bool):
    """
    rows: [{kode_rs, nama_rs, provinsi, kota, tipe_rs, kelas_rs, kontak}]
    upsert: True -> ON CONFLICT(kode_rs) DO UPDATE
    """
    if not rows:
        return 0, []
    sql = f"""
        INSERT INTO {TABLE_RS_MASTER} (kode_rs, nama_rs, provinsi, kota, tipe_rs, kelas_rs, kontak)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    if upsert:
        sql += """
        ON CONFLICT(kode_rs) DO UPDATE SET
          nama_rs=excluded.nama_rs,
          provinsi=excluded.provinsi,
          kota=excluded.kota,
          tipe_rs=excluded.tipe_rs,
          kelas_rs=excluded.kelas_rs,
          kontak=excluded.kontak
        """
    ok, logs = 0, []
    with connect() as conn:
        cur = conn.cursor()
        for r in rows:
            try:
                cur.execute(sql, [
                    r.get("kode_rs"), r.get("nama_rs"), r.get("provinsi"),
                    r.get("kota"), r.get("tipe_rs"), r.get("kelas_rs"), r.get("kontak")
                ])
                ok += 1
                logs.append(("OK", r.get("kode_rs"), "Tersimpan"))
            except Exception as e:
                logs.append(("GAGAL", r.get("kode_rs"), str(e)))
        conn.commit()
    return ok, logs

# ======================== Startup ========================
ensure_identitas_schema()
ensure_rumah_sakit_schema()
ensure_rs_penangan_schema()

# ======================== UI ========================
tab_input, tab_data, tab_master = st.tabs([
    "📝 Input RS Penangan", "📄 Data & Excel", "🏥 Master Rumah Sakit"
])

# ---------- TAB MASTER (Inline saat master kosong) ----------
with tab_master:
    st.subheader("🏥 Master Rumah Sakit")
    st.caption("Isi/ubah data master RS. **Kode RS** harus unik. Gunakan upsert untuk update cepat.")

    df_now, _ = load_rs_master()
    mode = st.radio(
        "Mode simpan:",
        options=["Tambah baru (hindari duplikat)", "Upsert (update jika Kode RS sudah ada)"],
        horizontal=True,
        key="rs_master::mode"
    )
    upsert = (mode == "Upsert (update jika Kode RS sudah ada)")

    # Editor master
    default_rows = 8 if df_now.empty else 3
    df_default = pd.DataFrame({
        "kode_rs": ["" for _ in range(default_rows)],
        "nama_rs": ["" for _ in range(default_rows)],
        "provinsi": ["" for _ in range(default_rows)],
        "kota": ["" for _ in range(default_rows)],
        "tipe_rs": ["" for _ in range(default_rows)],
        "kelas_rs": ["" for _ in range(default_rows)],
        "kontak": ["" for _ in range(default_rows)],
    })

    with st.form("rs_master::form_editor"):
        edited = st.data_editor(
            df_default,
            key="rs_master::editor",
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
        save_master = st.form_submit_button("💾 Simpan Master RS")

    if save_master:
        rows = []
        for _, r in edited.iterrows():
            kode = str(r.get("kode_rs") or "").strip()
            nama = str(r.get("nama_rs") or "").strip()
            if not kode and not nama:
                continue
            if not kode:
                st.error("Ada baris master tanpa **Kode RS**.")
                st.stop()
            if not nama:
                st.error(f"Ada baris master Kode RS '{kode}' tapi **Nama RS** kosong.")
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
        ok, logs = insert_master_rs_rows(rows, upsert=upsert)
        if ok:
            st.success(f"Berhasil menyimpan {ok} baris master RS.")
        if logs:
            st.dataframe(pd.DataFrame(logs, columns=["Status", "Kode RS", "Keterangan"]), use_container_width=True)

    st.divider()
    st.subheader("📄 Data Master Saat Ini")
    df_master_now, _ = load_rs_master()
    if df_master_now.empty:
        st.info("Master rumah_sakit masih kosong.")
    else:
        v = df_master_now.rename(columns={
            "kode_rs": "Kode RS", "nama_rs": "Nama RS",
            "provinsi": "Provinsi", "kota": "Kota",
            "tipe_rs": "Tipe RS", "kelas_rs": "Kelas RS"
        })
        st.dataframe(v[["Kode RS", "Nama RS", "Kota", "Provinsi", "Tipe RS", "Kelas RS"]], use_container_width=True)

# ---------- TAB INPUT RS PENANGAN ----------
with tab_input:
    st.caption("Isi data RS penangan hemofilia per HMHI cabang (gunakan **Kode RS** dari master).")

    hmhi_map, hmhi_list = load_hmhi_to_kode()
    df_rs, label_to_kode = load_rs_master()

    if not hmhi_list:
        st.warning("Belum ada data **identitas_organisasi**. Isi HMHI cabang & kode_organisasi dulu di tabel identitas.")
        selected_hmhi = None
    else:
        selected_hmhi = st.selectbox("Pilih HMHI Cabang (Provinsi)", options=hmhi_list, key="rs::hmhi")

    # — Perbaikan utama: JANGAN blokir jika master kosong.
    #    Tampilkan editor master inline di tab Master; di sini hanya info.
    if df_rs.empty:
        st.warning("Master **rumah_sakit** kosong. Tambahkan data di tab **🏥 Master Rumah Sakit** terlebih dahulu.")
    option_labels = [""] + sorted(label_to_kode.keys())

    # Editor input transaksi
    default_rows = 5
    df_default = pd.DataFrame({
        "rs_label": [""] * default_rows,
        "layanan": [""] * default_rows,
        "catatan": [""] * default_rows,
    })

    with st.form("rs::form_editor"):
        edited = st.data_editor(
            df_default,
            key="rs::editor",
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
            disabled=df_rs.empty  # disable jika master kosong
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
                n_saved = 0
                for _, r in edited.iterrows():
                    lbl = str(r.get("rs_label") or "").strip()
                    if not lbl:
                        continue
                    kode_rs = label_to_kode.get(lbl)
                    if not kode_rs:
                        st.warning(f"Lewati baris tanpa Kode RS valid: '{lbl}'")
                        continue
                    layanan = str(r.get("layanan") or "").strip()
                    catatan = str(r.get("catatan") or "").strip()
                    insert_rs_penangan(kode_organisasi, kode_rs, layanan, catatan)
                    n_saved += 1
                if n_saved:
                    st.success(f"{n_saved} baris tersimpan untuk **{selected_hmhi}**.")
                else:
                    st.info("Tidak ada baris valid untuk disimpan.")

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
