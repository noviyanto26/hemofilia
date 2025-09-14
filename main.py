# main.py
import streamlit as st
import runpy
from pathlib import Path

st.set_page_config(page_title="🩸 Hemofilia - Dashboard", page_icon="🩸", layout="wide")

# =========================
# Konfigurasi Menu & File
# =========================
KAT_PENDATAAN = [
    "1. Identitas Organisasi",
    "2. Jumlah Individu Hemofilia",
    "3. Berdasarkan Kelompok Usia",
    "4. Data Penyandang vWD",
    "5. Berdasarkan Jenis Kelamin",
    "6. Tingkat Hemofilia & Jenis Kelamin",
    "7. Penyandang Hemofilia Anak (Berat)",
    "8. Jumlah Penyandang VWD Berat",
    "9. Hemofilia dengan Inhibitor",
    "10. Pasien Nonfaktor (Dengan & Tanpa Inhibitor)",
]
KAT_PENANGANAN = [
    "11. Rumah Sakit Penangan Hemofilia",
    "12. Replacement Therapy",
    "13. Penanganan Kesehatan",
    "14. Hemofilia Berat - Prophylaxis per Usia",
    "15. Perkembangan Pelayanan Penanganan",
    "16. Informasi Donasi",
]
KAT_INFEKSI = [
    "17. Infeksi melalui Transfusi Darah",
    "18. Kematian Hemofilia (1 Jan 2024 - Sekarang)",
]

# 🔹 Rekapitulasi
REKAP_LABEL = "19. Rekapitulasi Hemofilia"
REKAP_LABEL_USIA = "Rekapitulasi Kelompok Usia"
REKAP_LABEL_JUMLAH = "Rekap Jumlah Individu"
REKAP_LABEL_GENDER = "Rekap Gender per Kelainan"
KAT_REKAP = [REKAP_LABEL, REKAP_LABEL_USIA, REKAP_LABEL_JUMLAH, REKAP_LABEL_GENDER]  # tanpa dummy

# Pemetaan label -> kandidat path file (root atau pages/)
BASE = Path(__file__).parent
CANDIDATES = {
    # 1 - 10
    "1. Identitas Organisasi": ["1_identitas_organisasi.py", "pages/1_identitas_organisasi.py"],
    "2. Jumlah Individu Hemofilia": ["2_jumlah_individu_hemofilia.py", "pages/2_jumlah_individu_hemofilia.py"],
    "3. Berdasarkan Kelompok Usia": ["3_berdasarkan_kelompok_usia.py", "pages/3_berdasarkan_kelompok_usia.py"],
    "4. Data Penyandang vWD": ["4_data_penyandang_vwd.py", "pages/4_data_penyandang_vwd.py"],
    "5. Berdasarkan Jenis Kelamin": ["5_berdasarkan_jenis_kelamin.py", "pages/5_berdasarkan_jenis_kelamin.py"],
    "6. Tingkat Hemofilia & Jenis Kelamin": ["6_tingkat_hemofilia_jenis_kelamin.py", "pages/6_tingkat_hemofilia_jenis_kelamin.py"],
    "7. Penyandang Hemofilia Anak (Berat)": ["7_penyandang_hemofilia_anak_berat.py", "pages/7_penyandang_hemofilia_anak_berat.py"],
    "8. Jumlah Penyandang VWD Berat": ["8_jumlah_penyandang_vwd_berat.py", "pages/8_jumlah_penyandang_vwd_berat.py"],
    "9. Hemofilia dengan Inhibitor": ["9_hemofilia_inhibitor.py", "pages/9_hemofilia_inhibitor.py"],
    "10. Pasien Nonfaktor (Dengan & Tanpa Inhibitor)": ["10_pasien_nonfaktor.py", "pages/10_pasien_nonfaktor.py"],
    # 11 - 16
    "11. Rumah Sakit Penangan Hemofilia": ["11_rs_penangan_hemofilia.py", "pages/11_rs_penangan_hemofilia.py"],
    "12. Replacement Therapy": ["12_replacement_therapy.py", "pages/12_replacement_therapy.py"],
    "13. Penanganan Kesehatan": ["13_penanganan_kesehatan.py", "pages/13_penanganan_kesehatan.py"],
    "14. Hemofilia Berat - Prophylaxis per Usia": ["14_hemo_berat_prophylaxis_usia.py", "pages/14_hemo_berat_prophylaxis_usia.py"],
    "15. Perkembangan Pelayanan Penanganan": ["15_perkembangan_pelayanan_penanganan.py", "pages/15_perkembangan_pelayanan_penanganan.py"],
    "16. Informasi Donasi": ["16_informasi_donasi.py", "pages/16_informasi_donasi.py"],
    # 17 - 18
    "17. Infeksi melalui Transfusi Darah": ["17_infeksi_transfusi_darah.py", "pages/17_infeksi_transfusi_darah.py"],
    "18. Kematian Hemofilia (1 Jan 2024 - Sekarang)": ["18_kematian_hemofilia_2024_sekarang.py", "pages/18_kematian_hemofilia_2024_sekarang.py"],
    # 19 - Rekapitulasi
    REKAP_LABEL: ["19_rekap_hemofilia.py", "pages/19_rekap_hemofilia.py"],
    REKAP_LABEL_USIA: ["19a_kelompok_usia_gabung.py", "pages/19a_kelompok_usia_gabung.py"],
    REKAP_LABEL_JUMLAH: ["19b_rekap_jumlah_individu.py", "pages/19b_rekap_jumlah_individu.py"],
    REKAP_LABEL_GENDER: ["20_rekap_gender_per_kelainan.py", "pages/20_rekap_gender_per_kelainan.py"],
}

# =========================
# Util: jalankan modul
# =========================
def run_page(label: str):
    if isinstance(label, str) and label.strip().endswith(".py"):
        candidate_paths = [label.strip(), f"pages/{label.strip()}"]
    else:
        candidate_paths = CANDIDATES.get(label, [])

    target = None
    for rel in candidate_paths:
        p = BASE / rel
        if p.exists():
            target = p
            break

    if target is None:
        st.error(
            f"File untuk '{label}' tidak ditemukan.\n"
            f"Coba pastikan ada di salah satu path berikut:\n{candidate_paths}"
        )
        return

    try:
        runpy.run_path(str(target), run_name="__main__")
    except Exception as e:
        st.exception(e)

# =========================
# Inisialisasi State
# =========================
DEFAULT_LABEL = KAT_PENDATAAN[0]
ss = st.session_state
if "active_label" not in ss:
    ss["active_label"] = DEFAULT_LABEL

# =========================
# Callback umum
# =========================
def sync_from(key_name: str):
    val = ss.get(key_name)
    if val:
        ss["active_label"] = val

def rekap_on_change():
    val = ss.get("menu_rekap")
    if val:
        ss["active_label"] = val

# =========================
# Sidebar
# =========================
st.sidebar.title("🩸 Menu")

with st.sidebar.expander("Pendataan Hemofilia", expanded=ss.get("active_label") in KAT_PENDATAAN):
    st.radio(
        "Pilih modul (Pendataan Hemofilia)",
        options=KAT_PENDATAAN,
        index=KAT_PENDATAAN.index(ss["active_label"]) if ss["active_label"] in KAT_PENDATAAN else 0,
        key="menu_pendataan",
        label_visibility="collapsed",
        on_change=sync_from,
        args=("menu_pendataan",),
    )

with st.sidebar.expander("Penanganan Perdarahan", expanded=ss.get("active_label") in KAT_PENANGANAN):
    st.radio(
        "Pilih modul (Penanganan Perdarahan)",
        options=KAT_PENANGANAN,
        index=KAT_PENANGANAN.index(ss["active_label"]) if ss["active_label"] in KAT_PENANGANAN else 0,
        key="menu_penanganan",
        label_visibility="collapsed",
        on_change=sync_from,
        args=("menu_penanganan",),
    )

with st.sidebar.expander("Infeksi Penyakit Menular", expanded=ss.get("active_label") in KAT_INFEKSI):
    st.radio(
        "Pilih modul (Infeksi Penyakit Menular)",
        options=KAT_INFEKSI,
        index=KAT_INFEKSI.index(ss["active_label"]) if ss["active_label"] in KAT_INFEKSI else 0,
        key="menu_infeksi",
        label_visibility="collapsed",
        on_change=sync_from,
        args=("menu_infeksi",),
    )

with st.sidebar.expander("Rekapitulasi", expanded=ss.get("active_label") in KAT_REKAP):
    rekap_index = KAT_REKAP.index(ss["active_label"]) if ss.get("active_label") in KAT_REKAP else 0
    st.radio(
        "Pilih modul (Rekapitulasi)",
        options=KAT_REKAP,
        index=rekap_index,
        key="menu_rekap",
        label_visibility="collapsed",
        on_change=rekap_on_change,
    )

# =========================
# Halaman Utama
# =========================
st.caption(f"Modul aktif: **{ss['active_label']}**")
run_page(ss["active_label"])
