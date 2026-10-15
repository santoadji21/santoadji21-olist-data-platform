# Langkah 6 — Mart dan dashboard (Metabase)

> v2 tidak dinilai, jadi **Metabase adalah dashboard-nya** (bukan Power BI/notebook seperti di tugas). Ketentuan isi T5 tetap: satu visual per pertanyaan + freshness, **satu kalimat insight berangka per visual**, angka bisa ditelusuri ke fakta, dan laporan memakai query yang sama dengan dashboard.

**Status: selesai dan terbukti.** 8 mart di dbt, 17 pemeriksaan mart = 0, ekspor gold, `outputs/marts_report.md`, dan dashboard Metabase yang dibuat dari kode.

## Mart (dbt, `dbt/models/mart/`)
`sales_monthly`, `revenue_vs_paid_monthly`, `delivery_monthly`, `seller_scorecard`, `customer_rfm`, `reconciliation`, `stream_freshness`, `stream_freshness_batch`. SQL dibawa dari T0–T6; yang berubah hanya `ref()` dan PK lewat `post_hook`. Pemeriksaan: `dbt/tests/mart_checks.sql` (17 pemeriksaan, termasuk rekonsiliasi ≥ 98 %).

Angka kunci: seller 3.095 (634 dinilai), rekonsiliasi cocok 98,91 %, RFM 94.983 orang.

## Satu sumber query: `olist_v2/questions.py`
Tiap pertanyaan punya SQL, jenis visual, dan fungsi insight yang menghitung kalimat dari **hasil query yang sama**. Dipakai oleh:
- `make dashboard` → kartu + dashboard Metabase (`olist_v2/metabase.py`, REST API, idempoten)
- `make marts` → `outputs/marts_report.md` + `outputs/marts.json`

Jadi angka di kalimat, tabel laporan, dan grafik tidak bisa berbeda.

## Menjalankan
```bash
make up P=bi              # Metabase http://localhost:13000 (admin dari .env: V2_METABASE_ADMIN_*)
make dbt-build            # mart harus ada dulu
make marts                # 17 pemeriksaan + ekspor gold + laporan
make dashboard            # http://localhost:13000/dashboard/2
```
`make dashboard` menyiapkan admin pertama, koneksi `olist_dw`, 10 kartu SQL, dan satu dashboard (grafik di kiri, kartu teks insight di kanan). Dijalankan ulang = kartu diperbarui, bukan digandakan.

## Penelusuran angka
`outputs/marts_report.md` memuat tabel mart → model dbt → fakta. Contoh: pendapatan = Σ `fact_order_item.price` untuk order bukan `canceled`/`unavailable` (pemeriksaan `sales_revenue_equals_fact_items` = 0).

## Catatan
- Dashboard hanya menampilkan **bulan lengkap**; total mart penuh berbeda sedikit (13.449.530 vs 13.494.400).
- Metabase memakai ±1 GB (batas compose 1 GB): mepet. Naikkan bila sering restart.
- Metabase v0.63; tema gelap: Settings → Appearance.
- Yang belum: uji beban dashboard (mis. k6 terhadap API kartu Metabase).
