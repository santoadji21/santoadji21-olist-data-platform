# EVALUATION_EVIDENCE — olist-v2

> Dibangkitkan oleh `make report` pada 2026-10-15T21:36:07+00:00 dari `outputs/*.json`. Jangan edit angka di sini dengan tangan.

## 1. Lingkungan & durasi tiap tahap

- Mesin: macOS-26.5.1-arm64-arm-64bit, arm64; Python 3.11.13; Docker version 29.8.2, build 7fc2dff
- Stack: PostgreSQL 16, MinIO, Kafka 3.9 (satu broker, KRaft), Airflow 2.10 (LocalExecutor), Spark (lokal di scheduler), dbt, Metabase, datacontract (ODCS)

Run `make all` dimulai 2026-10-14T22:14:57+00:00, setelah `make reset` (down -v; CSV di data/raw dipertahankan): **161.7 detik (2.7 menit)** untuk 10 perintah (batas tugas: ≤ 15 menit, ≤ 10 perintah) — SELESAI.

| Tahap | Perintah | Durasi (detik) | Exit |
|---|---|---:|---:|
| up | `make up P=stream bi` | 34.3 | 0 |
| download | `make download` | 2.4 | 0 |
| bronze | `make bronze` | 6.3 | 0 |
| silver | `make silver` | 31.9 | 0 |
| gate | `make gate` | 6.8 | 0 |
| load | `make load` | 6.3 | 0 |
| stream | `make stream` | 47.2 | 0 |
| dbt | `make dbt-build` | 12.4 | 0 |
| marts | `make marts` | 7.4 | 0 |
| dashboard | `make dashboard` | 6.7 | 0 |

## 2. Data & kualitas (T0–T2)

- Bronze (run terakhir): uploaded = 9 dari 9 file; ingest 2.84 s.

| Tabel | Baris | Ukuran (MB) | Versi | sha256 (12) |
|---|---:|---:|---:|---|
| customers | 99,441 | 9.0 | 1 | 983a422239e1 |
| geolocation | 1,000,163 | 61.3 | 1 | b514f6fc991b |
| order_items | 112,650 | 15.4 | 1 | 0bc4d068c4fe |
| order_payments | 103,886 | 5.8 | 1 | 4f713964f281 |
| order_reviews | 99,224 | 14.5 | 1 | 012b61c7593e |
| orders | 99,441 | 17.7 | 1 | 8df58ef3d2d7 |
| products | 32,951 | 2.4 | 1 | 3e6569628a17 |
| sellers | 3,095 | 0.2 | 1 | 1f643d2b9503 |
| product_category_translation | 71 | 0.0 | 1 | a81f0d1f27b2 |

Gate kualitas silver (`outputs/dq.json`, mesin spark 3.5.4 (local[2]), 26.0 s):

| Tabel | Sumber | Karantina | Silver | Verdict |
|---|---:|---:|---:|---|
| customers | 99,441 | 0 | 99,441 | PASS |
| geolocation | 1,000,163 | 0 | 1,000,163 | PASS |
| products | 32,951 | 0 | 32,951 | PASS |
| sellers | 3,095 | 0 | 3,095 | PASS |
| product_category_translation | 71 | 0 | 71 | PASS |
| orders | 99,441 | 0 | 99,441 | PASS |
| order_items | 112,650 | 0 | 112,650 | PASS |
| order_payments | 103,886 | 0 | 103,886 | PASS |
| order_reviews | 99,224 | 0 | 99,224 | PASS |

32 aturan hard dan 9 aturan soft (minimal 10 dan 8); 11,762 pelanggaran soft dicatat dan dipertahankan dengan flag; 0 pelanggaran hard (karantina).
- Gate kontrak ODCS (`datacontract test`): **warning** — 189 pemeriksaan, 129 lolos, 0 gagal, 8 peringatan soft, 52 pemeriksaan tipe belum didukung datacontract-cli untuk Parquet (keterbatasan alat, tidak menghentikan DAG).
- Load `raw.*`: customers 99,441, geolocation 1,000,163, order_items 112,650, order_payments 103,886, order_reviews 99,224, orders 99,441, products 32,951, sellers 3,095, product_category_translation 71; 5.0 s, semua cocok dengan sumber − karantina: True.

## 3. Stream (T3)

| Besaran | Producer | Consumer |
|---|---:|---:|
| Order | 99,441 | 99,441 (`order_current`) |
| Event unik | 394,713 | 394,713 |
| Duplikat | 11,812 | 11,812 diabaikan |
| Pesan rusak | 1,964 | 1,964 di DLQ |
| Event ditahan (terlambat) | 7,988 | 6,490 out-of-order tercatat |
| Pesan gagal terkirim | 0 | — |

- Status terkini = `raw.orders.order_status`: **99,441 dari 99,441**; pemeriksaan producer vs consumer lulus 4/4.
- Ujung ke ujung 28.1 s (batas 5 menit).
- Freshness: p50 = 841.1 ms, p95 = 5471.5 ms, maks = 7538.8 ms.

## 4. Warehouse & mart (T4–T5)

| Tabel dw | Baris |
|---|---:|
| dim_date | 800 |
| dim_geo | 18,677 |
| dim_customer | 96,096 |
| dim_seller | 3,095 |
| dim_product | 32,951 |
| fact_order | 99,441 |
| fact_order_item | 112,650 |
| fact_payment | 103,886 |
| fact_review | 98,673 |

- Integritas: **28/28 pemeriksaan = 0 baris bermasalah**; constraint terpasang di database: 9 PK dan 13 FK.
- Orang unik 96,096; order tanpa item 775, tanpa pembayaran 1; order delivered 96,478; late rate 6.77 % dari 96,470; bulan lengkap 20.

Jawaban ringkas (kalimat dibangkitkan dari mart; sama dengan dashboard Metabase, lihat `outputs/marts_report.md`):

- **q1_monthly** — Pendapatan bulanan naik dari R$ 120.098 (2017-01) menjadi R$ 848.860 (2018-08); total 13.449.530 dari 111.752 item, dan uang terbayar R$ 15.686.469 melebihi nilai pesanan R$ 15.683.707 (bunga cicilan/voucher).
- **q1_category** — Kategori teratas health_beauty menyumbang R$ 1.251.146; 10 kategori teratas total R$ 8.393.782.
- **q2_state** — AL paling sering terlambat (21,46 %, 85 dari 396 order), sedangkan AM terbaik (2,76 %).
- **q2_review** — Order tepat waktu rata-rata diberi skor 4,29 (89.182 ulasan), order terlambat 2,27 (6.378 ulasan); keterlambatan keseluruhan 6,79 %.
- **q3_sellers** — 54 dari 634 seller yang dinilai (≥ 30 order) perlu tindakan (38 terlambat, 4 batal, 25 skor rendah); seller yang dinilai mencakup 83,3 % order.
- **q3_summary** — 54 dari 634 seller yang dinilai (≥ 30 order) perlu tindakan (38 terlambat, 4 batal, 25 skor rendah); seller yang dinilai mencakup 83,3 % order.
- **q4_segments** — Dari 94.983 pembeli, segmen terbesar "Baru" berisi 36.757 orang (38,70 %); hanya 1.858 orang yang setia.
- **q4_frequency** — 97,0 % pembeli (92.096 dari 94.983) hanya membeli satu kali.
- **recon** — 98,91 % order (98.362) nilai pesanannya cocok dengan uang terbayar (syarat ≥ 98 %); sisanya dijelaskan: tanpa_item 775, cicilan 246, tidak_dijelaskan 50, voucher 7, tanpa_pembayaran 1.
- **fresh_batch** — Dari 211 batch (p95 tertinggi 7.528 ms), p50 batch awal 231 ms → batch akhir 3.272 ms: lag tumbuh selama replay.

Mart di gold: sales_monthly 11,805, revenue_vs_paid_monthly 24, delivery_monthly 556, seller_scorecard 3,095, customer_rfm 94,983, reconciliation 99,441, stream_freshness 2, stream_freshness_batch 211; 17/17 pemeriksaan mart = 0.

Rekonsiliasi nilai pesanan vs dibayar (toleransi 0,01 BRL):

| Kelas | Order | % | Selisih absolut (BRL) |
|---|---:|---:|---:|
| cocok | 98,362 | 98.91 | 2.73 |
| tanpa_item | 775 | 0.78 | 162,591.95 |
| cicilan | 246 | 0.25 | 3,050.30 |
| tidak_dijelaskan | 50 | 0.05 | 201.44 |
| voucher | 7 | 0.01 | 17.48 |
| tanpa_pembayaran | 1 | 0.0 | 143.46 |

## 5. Failure injection (T6)

**6/6 kasus TERTANGKAP**; pulih: 6/6. Detail dan bukti angka: `outputs/failure_tests.md`.

| Kasus | Verdict | Symptom |
|---|---|---|
| (a) Kolom hilang di sumber | TERTANGKAP | silver berhenti (Postgres/silver tidak disentuh): olist_customers_dataset.csv (customers): kolom hilang: customer_unique_id (exit 1) |
| (b) Producer dijalankan dua kali | TERTANGKAP | 40,614 pesan di topic, tetapi event tersimpan tetap 19,632 (= event unik); 20,802 duplikat diabaikan |
| (c) Consumer mati sebelum commit offset | TERTANGKAP | proses mati exit 137 dengan 5,793 event sudah di Postgres; setelah restart 2,000 pesan diproses ulang (batch ke-3 berisi 2,000 pesan) namun event tersimpan tetap 19,632 (= event unik) |
| (d) Broker Kafka mati beberapa detik di tengah replay | TERTANGKAP | outage pendek: producer exit 0, kegagalan 0, 31,502 event tiba. Outage panjang: producer naif exit 0 dengan 30,627 event hilang; producer yang memeriksa hasil `send()` exit 1 dengan 31375 kegagalan terlapor |
| (e) Seluruh jalur batch dijalankan ulang tanpa perubahan | TERTANGKAP | bronze: semua file `unchanged`; 26 besaran (baris tiap tabel raw/dw/mart, Σ pendapatan, order terlambat) identik |
| (f) Objek bronze berubah diam-diam di bucket | TERTANGKAP | silver berhenti (Postgres/silver tidak disentuh): sellers: isi objek bronze tidak cocok dengan sha256 di manifest (olist/sellers/ingest_date=2026-10-14/sha256=1f643d2b9503/olist_sellers_dataset.csv) (exit 1) |

Producer naif (future `send()` tidak diperiksa) saat broker mati 48.4 s: 30,627 event hilang tanpa error dari 31,502.

Pemulihan stack setelah semua gangguan: OK; smoke 12/12.

## 6. Keputusan desain

| Keputusan | Alternatif yang ditolak | Alasan |
|---|---|---|
| Bronze content-addressed (`sha256` di kunci objek) + manifest append-only di bucket | kunci berbasis tanggal; manifest lokal | upload kedua di hari yang sama tidak menimpa; manifest ikut bersama bucket; versi lama tetap ada |
| Aturan kualitas satu klausa `WHERE` dari kontrak ODCS, dipakai Spark, datacontract, dan uji DuckDB | tiga implementasi aturan | tidak bisa melenceng antara kontrak, silver, dan uji |
| Aturan yang diketahui kotor dijadikan soft (simpan + flag) | karantina semua pelanggaran | temuan kotor yang sudah dipahami tidak boleh menghentikan pipeline atau menghapus data |
| `load` = satu transaksi TRUNCATE + COPY untuk semua tabel | INSERT per baris; transaksi per tabel | atomik dan cepat; gagal di tengah tidak meninggalkan tabel setengah terisi |
| `event_id` deterministik dan status hanya naik | dedup berdasar `order_id`; urutan tiba menentukan status | duplikat dan event terlambat aman tanpa state tambahan; `canceled` terminal |
| Commit urutan Postgres → DLQ → offset Kafka | auto-commit offset; offset dulu | crash di antaranya hanya menghasilkan duplikat yang diabaikan, tidak ada kehilangan |
| dw dan mart sebagai model dbt, PK/FK lewat `post_hook` pada Postgres sungguhan | constraint dbt yang tidak ditegakkan; dokumentasi saja | PK/FK benar-benar terpasang dan dibuktikan oleh uji dan kueri integritas |
| Mart dan dashboard memakai satu daftar kueri (`questions.py`) | kueri terpisah di Metabase dan laporan | angka laporan dan grafik tidak bisa berbeda; insight dihitung dari hasil kueri yang sama |
| `is_late` NULL untuk order tanpa tanggal tiba, dibandingkan per tanggal | `false`; timestamp vs tanggal | late rate tidak terlihat lebih bagus dari kenyataan |
| F pada RFM memakai pita tetap | kuintil mentah | hampir semua orang hanya belanja sekali, kuintil memecah nilai sama secara acak |
| Log Airflow di bind mount, bukan named volume | named volume | log task lengkap terbaca; named volume memotongnya (terbukti lewat uji A/B) |

## 7. Limitation

1. Satu broker Kafka tanpa replikasi dan satu proses consumer: lag tumbuh selama replay penuh; broker mati lama berarti replay berhenti sampai broker hidup lagi.
2. Spark berjalan lokal di dalam scheduler Airflow (`local[2]`), bukan di cluster: cukup untuk skala ini, tetapi memori scheduler menjadi batas.
3. `datacontract-cli` belum bisa memeriksa tipe pada Parquet; pemeriksaan tipe ditutup oleh pemetaan tipe Spark dan uji DuckDB, bukan oleh gate itu sendiri.
4. dw dan mart dibangun ulang penuh dari `raw` pada tiap `dbt build`: waktu build tumbuh linear dengan data, dan tidak ada refresh inkremental.
5. Atribusi seller memakai level order: order multi-seller dihitung ke tiap seller.
6. Kredensial dev ada di `.env` tanpa rotasi atau secret manager, dan layanan tanpa TLS.
7. Failure test memakai satu skenario per kasus; broker mati dengan durasi sangat panjang atau disk penuh belum diuji.

## 8. Next experiment

1. Tambah consumer satu per partisi dan ukur apakah p95 freshness pada replay penuh turun tanpa lag yang tumbuh.
2. Bandingkan durasi silver: Spark lokal vs DuckDB/PyArrow pada 126 MB dan pada data 10× (catat kapan Spark mulai layak).
3. Ubah `fact_order` dan mart harian menjadi inkremental dan bandingkan durasi dengan rebuild penuh.
4. Pindahkan tabel silver ke Apache Iceberg (time travel, evolusi skema) dan bandingkan dengan Parquet biasa.

## 9. Reproduksi (perintah persis)

```bash
cd olist-v2
make venv                          # venv lokal (dbt, pytest, psycopg2, kafka-python) — sekali
# letakkan kredensial Kaggle di .kaggle/kaggle.json (tidak ikut git)
make all YES=1                     # reset (down -v) lalu: up → download → bronze → silver → gate → load → stream → dbt-build → marts → dashboard
make inject                        # failure injection → outputs/failure_tests.md
make smoke                         # pemeriksaan kesehatan seluruh lapisan
make report                        # EVALUATION_EVIDENCE.md
make test                          # pytest tanpa Docker
```
