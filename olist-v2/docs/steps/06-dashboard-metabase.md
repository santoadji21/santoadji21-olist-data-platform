# Langkah 6 — Dashboard: Metabase

> v2 tidak dinilai, jadi **Metabase adalah dashboard-nya** (bukan Power BI/notebook seperti di tugas). Ketentuan isi T5 tetap: satu visual per pertanyaan + freshness, **satu kalimat insight berangka per visual**, dan angka bisa ditelusuri ke fakta. Agar `outputs/marts_report.md` dan dashboard memakai query yang sama, **logika ada di mart dbt**, Metabase hanya menampilkan.

**Status: rencana** (service ditulis di profil `bi`). Tujuan: empat pertanyaan manajemen dijawab dari `mart.*`, dengan angka yang bisa ditelusuri ke fakta.

## Detail
- Image di-pin lewat digest (sudah ada di mesin). Database aplikasi Metabase memakai database `metabase` di Postgres yang sama.
- Nyalakan: `make up P="bi"`, buka `http://localhost:13000`.
- Tambah sumber data Postgres: host `postgres`, port `5432`, database `olist_dw`, schema `mart`.
- **Dark mode**: Metabase punya tema gelap bawaan (Settings → Appearance).

## Kartu yang dibuat (dari T5)
Q1 pendapatan per bulan dan kategori; Q2 late rate per negara bagian dan skor review; Q3 seller yang perlu ditindak; Q4 RFM; rekonsiliasi nilai pesanan vs dibayar; freshness stream (setelah langkah 7).

## Kriteria selesai
Tiap kartu dibuat dari tabel `mart.*` (bukan SQL ad hoc yang berisi logika bisnis), dan satu angka bisa ditelusuri ke `dw.fact_*` dalam 2 menit.

## Jebakan
Logika yang ditaruh di Metabase tidak bisa diuji. Hitung di dbt, tampilkan di Metabase.
