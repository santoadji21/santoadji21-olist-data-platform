# EVALUATION_EVIDENCE — Olist Data Platform

> Dibangkitkan oleh `python pipeline.py report` pada 2026-10-12T13:46:00+00:00 dari `outputs/*.json`. Jangan edit angka di sini dengan tangan.

## 1. Lingkungan & durasi tiap tahap

- Mesin: macOS-26.5.1-arm64-arm-64bit, arm64; Python 3.11.13; Docker version 29.4.0, build 9d7ad9f
- Stack: PostgreSQL 16, Kafka 3.9 (satu broker, KRaft), MinIO, Jupyter (satu docker compose)

Run bersih `python pipeline.py all` (dimulai 2026-10-12T13:38:34+00:00); `docker compose down -v` dilakukan sebelumnya: **108.8 detik total (1.8 menit)** untuk 10 perintah (batas tugas: ≤ 15 menit, ≤ 10 perintah).

| Tahap | Perintah | Durasi (detik) | Exit |
|---|---|---:|---:|
| init | `python pipeline.py init` | 0.0 | 0 |
| up | `python pipeline.py up` | 21.6 | 0 |
| check | `python pipeline.py check` | 1.2 | 0 |
| profile | `python pipeline.py profile` | 7.7 | 0 |
| bronze | `python pipeline.py bronze` | 2.1 | 0 |
| silver | `python pipeline.py silver` | 7.5 | 0 |
| load | `python pipeline.py load` | 6.0 | 0 |
| stream | `python pipeline.py stream run` | 43.3 | 0 |
| model | `python pipeline.py model` | 13.5 | 0 |
| marts | `python pipeline.py marts` | 5.8 | 0 |

## 2. Data & kualitas (T0–T2)

- Profil: 9 tabel, **18 temuan kotor** (≥ 10 diminta); keputusan: hard-rule = 1, keep-and-flag = 11, silent-wrong = 6.
- Bronze (run terakhir): unchanged = 9 dari 9 file; ingest 1.09 s.

| Tabel | Baris bronze | Ukuran (MB) | Versi |
|---|---:|---:|---:|
| customers | 99,441 | 9.0 | 3 |
| geolocation | 1,000,163 | 61.3 | 1 |
| order_items | 112,650 | 15.4 | 1 |
| order_payments | 103,886 | 5.8 | 1 |
| order_reviews | 99,224 | 14.3 | 1 |
| orders | 99,441 | 17.7 | 1 |
| products | 32,951 | 2.4 | 1 |
| sellers | 3,095 | 0.2 | 1 |
| product_category_translation | 71 | 0.0 | 1 |

Gate kualitas silver (hasil `outputs/dq.json`):

| Tabel | Sumber | Karantina | Silver | Verdict |
|---|---:|---:|---:|---|
| customers | 99,441 | 0 | 99,441 | PASS |
| sellers | 3,095 | 0 | 3,095 | PASS |
| products | 32,951 | 0 | 32,951 | PASS |
| product_category_translation | 71 | 0 | 71 | PASS |
| geolocation | 1,000,163 | 0 | 1,000,163 | PASS |
| orders | 99,441 | 0 | 99,441 | PASS |
| order_items | 112,650 | 0 | 112,650 | PASS |
| order_payments | 103,886 | 0 | 103,886 | PASS |
| order_reviews | 99,224 | 0 | 99,224 | PASS |

38 aturan hard dan 9 aturan soft (minimal 10 dan 8); 11,762 pelanggaran soft dicatat dan dipertahankan dengan flag.
- Load `raw.*`: customers 99,441, geolocation 1,000,163, order_items 112,650, order_payments 103,886, order_reviews 99,224, orders 99,441, products 32,951, sellers 3,095, product_category_translation 71; 5.6 s, semua cocok dengan sumber − karantina: True.

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
- Replay (producer): 22.1 s; ujung ke ujung 23.8 s (batas 5 menit).
- Freshness: p50 = 761.1 ms, p95 = 1958.3 ms, maks = 2423.4 ms.

## 4. Warehouse & mart (T4–T5)

| Tabel dw | Baris |
|---|---:|
| dim_date | 800 |
| dim_geo | 18,677 |
| dim_customer | 96,096 |
| dim_product | 32,951 |
| dim_seller | 3,095 |
| fact_order | 99,441 |
| fact_order_item | 112,650 |
| fact_payment | 103,886 |
| fact_review | 98,673 |

- Integritas: **28/28 pemeriksaan = 0 baris bermasalah**; build 11.6 s.
- Orang unik 96,096; order tanpa item 775, tanpa pembayaran 1; order delivered 96,478; late rate 6.77 % dari 96,470; bulan lengkap 20.

Jawaban ringkas (kalimat dibangkitkan dari mart, lihat `outputs/marts_report.md`):

- **Q1** — Pendapatan 2017-01 s.d. 2018-08 (bulan lengkap) 13,449,530 BRL dari 111,752 item; uang terbayar 15,686,469 BRL vs nilai pesanan 15,683,707 BRL (+0.02%); kategori terbesar health_beauty (9.3% pendapatan).
- **Q2** — Late rate 6.79% dari 96,203 order terkirim; tertinggi di AL (21.5%, n=396) dan, di antara negara bagian dengan ≥ 500 order, MA (17.5%, n=713); skor review turun dari 4.29 (tepat waktu) menjadi 2.27 (terlambat).
- **Q3** — 54 dari 634 seller yang dinilai (≥ 30 order, mencakup 83% order) perlu ditindak: 38 karena late rate ≥ 15%, 4 karena batal ≥ 5%, 25 karena skor ≤ 3,5 (seller bisa terkena lebih dari satu).
- **Q4** — 2,887 dari 94,983 orang (3.0%) belanja ulang; 14,010 orang bernilai tinggi sudah lama tidak kembali (rata-rata 444 hari, total 3,895,230 BRL).

Mart: sales_monthly 11,805, revenue_vs_paid_monthly 24, delivery_monthly 556, seller_scorecard 3,095, customer_rfm 94,983, reconciliation 99,441, stream_freshness 1, stream_freshness_batch 216; 17/17 pemeriksaan = 0; build 1.9 s.

Rekonsiliasi nilai pesanan vs dibayar (toleransi 0,01 BRL):

| Kelas | Order | % | Selisih absolut (BRL) |
|---|---:|---:|---:|
| cocok | 98,362 | 98.91 | 2.73 |
| tanpa_item | 775 | 0.78 | 162,591.95 |
| cicilan | 246 | 0.25 | 3,050.30 |
| tidak_dijelaskan | 50 | 0.05 | 201.44 |
| voucher | 7 | 0.01 | 17.48 |
| tanpa_pembayaran | 1 | 0.00 | 143.46 |

## 5. Failure injection (T6)

**6/6 kasus TERTANGKAP**; pulih: 6/6. Detail dan bukti angka: `outputs/failure_tests.md`.

| Kasus | Verdict | Symptom |
|---|---|---|
| (a) Kolom hilang di sumber | TERTANGKAP | silver berhenti (Postgres tidak disentuh): olist_customers_dataset.csv (customers): kolom hilang: customer_unique_id (exit 1) |
| (b) Producer dijalankan dua kali | TERTANGKAP | 40,614 pesan di topic, tetapi event tersimpan tetap 19,632 (= event unik); 20,802 duplikat diabaikan |
| (c) Consumer mati sebelum commit offset | TERTANGKAP | proses mati exit 137 dengan 5,794 event sudah di Postgres; setelah restart 2,000 pesan diproses ulang (batch ke-3 berisi 2,000 pesan) namun event tersimpan tetap 19,632 (= event unik) |
| (d) Broker Kafka mati beberapa detik di tengah replay | TERTANGKAP | outage pendek: producer exit 0, kegagalan 0, 31,502 event tiba. Outage panjang: producer naif exit 0 tanpa pesan error dan 28,113 event hilang; producer yang memeriksa hasil `send()` exit 1 dengan 29275 kegagalan terlapor |
| (e) Seluruh jalur batch dijalankan ulang tanpa perubahan | TERTANGKAP | bronze: semua file `unchanged`; 28 besaran (baris tiap tabel raw/dw/mart, Σ pendapatan, order terlambat) identik |
| (f) Objek bronze berubah diam-diam di bucket | TERTANGKAP | silver berhenti (Postgres tidak disentuh): sellers: isi objek bronze tidak cocok dengan sha256 di manifest (olist/sellers/ingest_date=2026-10-12/sha256=1f643d2b9503/olist_sellers_dataset.csv) (exit 1) |

Producer naif (future `send()` tidak diperiksa) saat broker mati None s: 28,113 event hilang tanpa error dari 31,502.

Pemulihan stack setelah semua gangguan: OK; smoke 11/11.

## 6. Keputusan desain

| Keputusan | Alternatif yang ditolak | Alasan |
|---|---|---|
| Bronze content-addressed (`sha256` di kunci objek) + manifest append-only di bucket | kunci berbasis tanggal; manifest lokal | upload kedua di hari yang sama tidak menimpa; manifest ikut bersama bucket; versi lama tetap ada |
| Satu klausa `WHERE` untuk aturan kualitas, dipakai kontrak ODCS dan gate (DuckDB) | dua implementasi aturan | tidak bisa melenceng antara kontrak dan gate |
| Aturan yang diketahui kotor dijadikan soft (simpan + flag) | karantina semua pelanggaran | temuan kotor yang sudah dipahami tidak boleh menghentikan pipeline atau menghapus data |
| `load` = satu transaksi TRUNCATE + COPY untuk semua tabel | INSERT per baris; transaksi per tabel | atomik dan cepat; gagal di tengah tidak meninggalkan tabel setengah terisi |
| `event_id` deterministik dan status hanya naik | dedup berdasar `order_id`; urutan tiba menentukan status | duplikat dan event terlambat aman tanpa state tambahan; `canceled` terminal |
| Commit urutan Postgres → DLQ → offset Kafka | auto-commit offset; offset dulu | crash di antaranya hanya menghasilkan duplikat yang diabaikan, tidak ada kehilangan |
| `dw` rebuild penuh dalam satu transaksi | update inkremental | murni turunan raw; gagal di tengah rollback; tidak ada logika inkremental yang bisa melenceng |
| `dim_customer` per orang (SCD tipe 1) | per `customer_id`; SCD tipe 2 | `customer_id` menghitung order, bukan orang; yang hilang hanya riwayat alamat |
| `is_late` NULL untuk order tanpa tanggal tiba, dibandingkan per tanggal | `false`; timestamp vs tanggal | late rate tidak terlihat lebih bagus dari kenyataan |
| F pada RFM memakai pita tetap | kuintil mentah | hampir semua orang hanya belanja sekali, kuintil memecah nilai sama secara acak |
| Dashboard notebook membaca `mart.*` | Power BI; logika di notebook | logika bisnis di SQL yang bisa diuji; laporan dan grafik memakai query yang sama |

## 7. Limitation

1. Satu broker Kafka tanpa replikasi (`replication.factor=1`) dan satu proses consumer: lag tumbuh selama replay (p95 = 1958.3 ms pada replay penuh); broker mati lama berarti replay berhenti.
2. Silver membaca satu CSV utuh ke memori (geolokasi ±1 juta baris) lalu menjalankan aturan lewat DuckDB: aman pada skala ini, tetapi memori tumbuh linear dengan ukuran file.
3. `dw` dan `mart` selalu rebuild penuh dari `raw`: waktu build tumbuh linear dengan data, dan tidak ada refresh per-hari.
4. Atribusi seller memakai order-level (`is_late`, review): order multi-seller dihitung ke tiap seller, jadi skor seller bercampur dengan seller lain di order yang sama.
5. Kredensial dev (`minio`, `olist`) ada di `.env` tanpa rotasi atau secret manager, dan MinIO tanpa web console atau TLS.
6. Failure test memakai satu skenario per kasus (broker mati ±6 detik, satu byte diubah, satu batch crash); outage yang lebih panjang dari timeout request producer belum diuji.

## 8. Next experiment

1. Tambah consumer satu per partisi (3 partisi) dan ukur apakah p95 freshness pada replay penuh turun di bawah 1 detik tanpa lag yang tumbuh.
2. Ganti pembaca silver dengan DuckDB/PyArrow yang membaca CSV per-chunk dan ukur memori puncak serta durasi pada data 10× (replikasi file).
3. Ubah `fact_order` dan mart harian menjadi inkremental (hanya partisi tanggal yang berubah) dan bandingkan durasi build dengan rebuild penuh.
4. Uji outage broker 60 detik (lebih lama dari `request_timeout_ms`) dan ukur berapa pesan yang menyerah serta apakah kirim ulang idempoten memulihkannya.

## 9. Reproduksi (perintah persis)

```bash
git clone <repo> && cd olist-data-platform
python -m venv .venv && .venv/bin/pip install -r requirements.txt   # atau: uv pip install -r requirements.txt
cp .env.example .env                       # atau: python pipeline.py init
python scripts/get_dataset.py              # unduh 9 CSV ke data/raw (Kaggle, cadangan mirror)
docker compose down -v                     # mulai dari nol
python pipeline.py all                     # init → up → check → profile → bronze → silver → load → stream → model → marts
python pipeline.py inject                  # failure injection → outputs/failure_tests.md
python pipeline.py report                  # EVALUATION_EVIDENCE.md
python pipeline.py smoke                   # pemeriksaan kesehatan seluruh lapisan
pytest -q
```
