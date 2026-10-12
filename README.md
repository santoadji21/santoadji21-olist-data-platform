# Starter — Tugas Besar Olist Data Platform

Isi starter ini **sengaja sedikit**: stack yang sama dengan Local Big Data Lab, pemeriksa dataset, kerangka `pipeline.py`,
dan tiga template dokumen. Sisanya kamu bangun mengikuti `TUGAS_BESAR.md`.

```
docker-compose.yml   .env.example   jupyter/Dockerfile      # stack (postgres, kafka, kafka-ui, minio, jupyter) — project name "olist"
pipeline.py                                                 # init/up/down sudah jadi; tahap lain = TODO kamu
scripts/check_dataset.py                                    # cek 9 file CSV Kaggle di data/raw
sql/00_schemas.sql                                          # hanya CREATE SCHEMA; tabel kamu yang rancang
tests/conftest.py                                           # fixture koneksi Postgres untuk test (skip kalau stack mati)
docs/DATA_CONTRACT_TEMPLATE.md  RUNBOOK_TEMPLATE.md  EVALUATION_EVIDENCE_TEMPLATE.md
```

Langkah pertama:

```
pip install -r requirements.txt
python pipeline.py init
python pipeline.py up --build        # pertama kali: build image jupyter (±2–3 menit)
# unduh dataset Kaggle → data/raw/
python scripts/check_dataset.py
```

Lalu buka `TUGAS_BESAR.md` → Tahap 0.


---

## T0 — Kontrak data & profil

Acuan semua tahap: [DATA_CONTRACT.md](DATA_CONTRACT.md) (dibangkitkan), [contracts/olist_raw.odcs.yaml](contracts/olist_raw.odcs.yaml) (ODCS), [outputs/profile.md](outputs/profile.md).

```bash
python pipeline.py check          # 9 file ada, kolom & jumlah baris sesuai
python pipeline.py profile        # outputs/profile.md + profile.json
.venv/bin/python pipeline.py contract      # tulis ODCS + datacontract lint + test
python pipeline.py contract-doc   # DATA_CONTRACT.md
```

**Sumber dataset.** Kesembilan CSV diambil dengan `scripts/get_dataset.py`. Sumbernya Kaggle (`olistbr/brazilian-ecommerce`), dengan kredensial di `.kaggle/kaggle.json` (tidak di-commit); skrip itu punya cadangan mirror GitHub bila Kaggle gagal. `check` lulus (jumlah baris dan md5 sesuai), jadi angka cek berlaku.

### Pertanyaan README: temuan yang membuat Q1–Q4 salah secara diam-diam

1. **Dua id pelanggan (temuan #1, Q4).** `customer_id` dibuat per order, `customer_unique_id` per orang: 99.441 vs 96.096, selisih 3.345 (3,36%). Kalau `dim_customer` dan RFM memakai `customer_id`, tiap order tampak dari "pelanggan baru", jadi frekuensi selalu 1 dan pelanggan yang belanja ulang (2.997 orang) tidak terdeteksi. Tidak ada error, hanya angka belanja ulang yang terlihat jauh lebih kecil.
2. **Order tanpa item dan tanpa pembayaran (temuan #7, Q1).** 775 order tanpa item (603 `unavailable`, 164 `canceled`), dan 1 tanpa pembayaran. Kalau pendapatan dihitung dari `payment_value` tanpa menyaring status, order yang tidak pernah terkirim ikut dihitung. Kalau memakai inner join ke item, 775 order hilang dari `fact_order` dan jumlah order ikut meleset.
3. **`delivered` tanpa tanggal tiba (temuan #9, Q2).** 8 order `delivered` tidak punya `order_delivered_customer_date`, dan 23 order punya tanggal tiba lebih awal dari tanggal diserahkan ke kurir. Perbandingan `NULL > estimasi` bernilai false di SQL, jadi `is_late = false` dan late rate terlihat lebih bagus. Karena itu `is_late` dibuat `NULL` dan penyebut late rate hanya order dengan `is_late IS NOT NULL`.
4. **Selisih item vs pembayaran (temuan #10, Q1).** 303 order selisih > 0,01 BRL (dihitung dalam sen; selisih tepat 0,01 ada 273 dan tidak dihitung), 249 di antaranya > 1,00, total selisih absolut 3.271,95 BRL. Penyebabnya berbeda: 264 dibayar lebih (249 di antaranya cicilan kartu kredit > 1), 39 kurang, 7 memakai voucher. Memakai `payment_value` sebagai pendapatan memasukkan bunga cicilan, memakai item mengabaikan voucher. Angka yang diambil menentukan "pendapatan" Q1, jadi selisihnya diklasifikasikan di mart rekonsiliasi.
5. **Ulasan ganda (temuan #2, Q2).** 547 order punya lebih dari satu ulasan dan 814 baris `review_id` berulang. Join langsung order ke review menggandakan baris order, sehingga skor rata-rata dan jumlah order terlambat ikut tertimbang ganda.

Keputusan penanganannya ada di DATA_CONTRACT.md §2, termasuk tahap yang menerapkannya.


---

## T1 — Bronze: ingest ke MinIO

```bash
make up && make bronze        # atau: python pipeline.py bronze
```

Hasil: [outputs/bronze_report.md](outputs/bronze_report.md) (tabel per file) dan `outputs/bronze.json`. Kode: [olist/bronze.py](olist/bronze.py), test tanpa Docker: [tests/test_bronze.py](tests/test_bronze.py).

**Tata letak.** Bucket `bronze`:
- `olist/<tabel>/ingest_date=YYYY-MM-DD/sha256=<12 hex>/<file>`: isi CSV apa adanya. Kunci memuat sha256, jadi isi berbeda tidak pernah menimpa objek lama, termasuk di hari yang sama.
- `olist/_manifest/<tabel>.json`: manifest per tabel (JSON biasa, di bucket, bukan di laptop). Isinya sumber, `versions[]` (versi, sha256, ukuran byte, jumlah baris, waktu ingest, kunci objek) yang hanya bertambah, dan `latest` yang menunjuk objek terbaru.

**Idempotensi.** sha256 isi dibandingkan dengan versi terakhir di manifest. Sama → `unchanged`, tanpa upload. Beda → `new_version`. Ukuran byte tidak pernah dipakai sebagai pembanding. Manifest ditulis setelah objek, sehingga kalau proses mati di antaranya, run berikutnya hanya menulis manifest (objeknya sudah ada).

**Bukti (dijalankan di MinIO lokal).**
- Ingest pertama 9 file (±126 MB): 1,9 detik (batas tugas 2 menit). Semua `uploaded`.
- Ingest kedua tanpa perubahan: 9/9 `unchanged`, tidak ada upload.
- Satu byte diubah di `sellers` (huruf pertama `campinas` jadi `Campinas`): `new_version` v2, objek v1 tetap ada di bucket dan manifest. Setelah file dikembalikan, v3 menunjuk objek v1 yang sama tanpa upload ulang.
- Jumlah baris dihitung dengan parser CSV dan cocok dengan kontrak untuk semua tabel.

### Pertanyaan README: kenapa bronze harus append-only?

Karena bronze adalah satu-satunya salinan sumber apa adanya. Silver dan semua lapisan di atasnya dibangun ulang darinya. Kalau bronze bisa ditimpa, kesalahan di ingest atau di sumber ikut menghapus kebenaran dan tidak ada yang bisa diaudit.

Skenario Olist: file `olist_order_reviews_dataset.csv` dikirim ulang dengan satu kolom bergeser atau komentar yang terpotong, dan sistem hulu tidak memberi tahu. Karena versi lama masih ada di bucket dan manifest (sha256-nya berbeda), kita bisa membandingkan v1 dan v2, melihat tepatnya apa yang berubah, dan membangun ulang silver dari v1 sementara menunggu perbaikan dari hulu. Pada skenario yang lebih ringan, angka di dashboard berubah setelah file diperbarui, dan kita bisa menjawab "angka itu dari versi file yang mana".


---

## T2 — Silver, gate kualitas, dan `raw.*`

```bash
make silver     # bronze → silver Parquet + gate + karantina  → outputs/dq_report.md
make load       # silver → raw.* (TRUNCATE+COPY, satu transaksi) → outputs/load_report.md
```

Kode: [olist/gate.py](olist/gate.py) (gate murni), [olist/silver.py](olist/silver.py), [olist/load.py](olist/load.py). Test: [tests/test_gate.py](tests/test_gate.py) (tanpa Docker), [tests/test_load_db.py](tests/test_load_db.py) (Docker).

**Satu sumber untuk aturan.** Tipe dan kolom dari [olist/schema.py](olist/schema.py); aturan, ambang, dan FK dari [olist/contract.py](olist/contract.py). Klausa `WHERE` yang sama dipakai kontrak ODCS (`datacontract test`) dan gate (DuckDB di atas DataFrame), jadi kalau aturan berubah hanya satu tempat yang diedit. DDL `raw.*` juga dibangkitkan dari `schema.py`.

**Alur.** `silver` mengambil versi terbaru dari manifest bronze dan memverifikasi sha256-nya, lalu memeriksa kolom semua tabel *sebelum* menulis apa pun. Setelah itu tiap tabel diproses dengan induk sebelum anak: konversi tipe → aturan hard (karantina) → FK (karantina) → aturan soft (hitung + flag). Baris lolos ditulis ke `silver/olist/<tabel>/<tabel>.parquet`, baris bermasalah ke `silver/olist/_quarantine/<tabel>.parquet` dengan kolom `_reasons`. Tabel berverdict `FAIL` tidak mendapat silver.

**Hasil (dari `outputs/dq_report.md` dan `outputs/load_report.md`).** Semua tabel `PASS`, 0 baris dikarantina: temuan kotor yang diketahui semuanya soft (simpan + flag). Jumlah soft cocok dengan profil T0, misalnya `lng_in_brazil` 11.682, `customer_after_carrier` 23, `delivered_has_date` 8. Flag `is_outside_brazil` = 11.687 titik dan `has_time_anomaly` = 31 order (8 + 23). `raw.*` berisi 9 tabel dan jumlah barisnya = sumber − karantina, dicek di dalam transaksi load sebelum commit.

**Perilaku yang dibuktikan test.**
- Kolom hilang atau berubah nama → `silver` berhenti sebelum menulis apa pun dan sebelum Postgres disentuh, dengan pesan yang menyebut kolom itu.
- `load` dua kali → jumlah baris sama. Gangguan di tengah load (tabel ketiga gagal) → rollback, `raw.customers` tidak berubah.
- Kode pos `1003` menjadi `01003` (teks); string kosong menjadi `NULL`, bukan `''`.

### Pertanyaan README: ambang karantina

| Tabel | Ambang | Alasan |
|---|---:|---|
| customers, order_items, sellers, product_category_translation | 0 % | kunci dan FK; satu baris rusak = file korup |
| order_payments, orders, products | 0,1 % | anomali yang diketahui sudah soft; lebih dari 1 per 1.000 berarti sumber berubah |
| order_reviews | 0,5 % | skor di luar 1–5 mengubah rata-rata Q2, toleransi kecil |
| geolocation | 2 % | 1 juta baris, ada ketidakrapian koordinat; di atas 2 % berarti sumber rusak |

Ambang 0 % membuat satu baris salah menghentikan pipeline, dan 50 % meloloskan file yang separuhnya sampah. Karena itu ambang diletakkan sedikit di atas tingkat gangguan yang terukur, dan hanya berlaku untuk pelanggaran hard (tipe, null, FK, aturan hard). Pelanggaran soft yang sudah diketahui tidak pernah mengarantina.

Pada data asli tidak ada tabel yang dikarantina, jadi kasus lolos dan gagal ditunjukkan oleh test `test_threshold_pass_and_fail_cases` dengan 100 baris pada ambang 2 %:
- **Lolos:** 2 baris melanggar = 2,0 % → `PASS+KARANTINA`, 2 baris di karantina, 98 di silver.
- **Gagal:** 3 baris melanggar = 3,0 % → `FAIL`, alasan "karantina 3.000% > ambang 2.0%", tabel tidak dimuat.


---

## T3 — Streaming replay di Kafka

```bash
make stream           # reset → consumer (proses terpisah) → producer → laporan; ±36 detik untuk 99.441 order
make stream-small     # 5.000 order, untuk uji cepat
make stream-report    # laporan dari kondisi sekarang
python pipeline.py stream produce|consume|reset [--limit N --speed N --seed N --crash-at-batch N]
```

Kode: [olist/stream.py](olist/stream.py) (logika murni), [olist/producer.py](olist/producer.py), [olist/consumer.py](olist/consumer.py), [olist/stream_report.py](olist/stream_report.py), [sql/10_stream.sql](sql/10_stream.sql). Test: [tests/test_stream.py](tests/test_stream.py) (tanpa Docker), [tests/test_consumer_db.py](tests/test_consumer_db.py) (Postgres). Hasil: [outputs/stream_report.md](outputs/stream_report.md), `outputs/stream.json`, kunci jawaban producer `outputs/stream_truth.json`.

**Desain.**
- **Event.** Tiap order dipecah dari header `raw.orders` menjadi event `created → approved → shipped → delivered` (hanya yang punya waktu nyata dan tidak melewati status akhir), ditambah satu event status akhir bila belum ada (`canceled`, `unavailable`, `invoiced`, dst.; waktunya diisi waktu terakhir yang diketahui dan ditandai `ts_imputed`). Key Kafka = `order_id` (3 partisi), sehingga urutan per order terjaga.
- **`event_id` deterministik** `"<order_id>:<status>"` dan menjadi PK `stream.order_events`: duplikat dikenali tanpa state tambahan. Dua replay berbeda menghasilkan id yang sama, jadi memutar ulang tidak menggandakan data.
- **Status hanya naik.** Peringkat `created 1 < approved 2 < invoiced 3 < processing 4 < shipped 5 < delivered 6 < canceled = unavailable 9`. `canceled` terminal: `delivered` yang datang terlambat tidak menimpanya. Upsert `stream.order_current` memakai `ON CONFLICT … DO UPDATE … WHERE status_rank < EXCLUDED.status_rank`.
- **Gangguan** (seed 42, dicatat ke `stream_truth.json`): 3 % event dikirim dua kali, 2 % ditahan 500–20.000 pesan lalu dikirim terlambat, 0,5 % salinan **tambahan** yang dirusak (field hilang, format waktu salah, bukan JSON). Rusak berupa tambahan, bukan pengganti, supaya event unik yang tersimpan harus tepat sama dengan event unik asli.
- **Consumer.** Group `olist-order-consumer`, `auto_offset_reset=earliest`, `enable_auto_commit=False`, batch sampai 2.000 pesan, satu transaksi per batch. Pesan rusak → tabel `stream.dlq` + topic `olist.order_events.dlq` dengan alasan. Metrik per batch di `stream.batch_metrics`.
- **Producer** memeriksa hasil tiap `send()` lewat errback dan gagal keras bila ada pesan yang tidak terkirim.

**Hasil replay penuh (dari `outputs/stream_report.md`).**

| Besaran | Producer | Consumer |
|---|---:|---:|
| Order | 99.441 | 99.441 (`order_current`) |
| Event unik | 394.713 | 394.713 |
| Duplikat | 11.812 | 11.812 |
| Rusak / DLQ | 1.964 (657 field hilang, 646 waktu salah, 661 bukan JSON) | 1.964 |
| Total pesan | 408.489 | 408.489 |
| Status terkini = `raw.orders.order_status` | | 99.441 dari 99.441 |

Event terlambat dikirim 7.988; consumer mencatat 6.490 event out-of-order. Angkanya wajar berbeda: event yang ditahan hanya out-of-order bila status berikutnya sudah tiba, dan data asli menghasilkan out-of-order alami (tanggal tiba lebih awal dari tanggal kurir). Replay selesai ±36 detik (batas 5 menit). Duplikat di sisi consumer dihitung sebagai total pesan di topic − event unik − DLQ, sehingga tetap tepat walau ada pesan yang dikirim ulang setelah crash.

**Consumer dimatikan paksa.** `--crash-at-batch N` memanggil `os._exit(137)` setelah transaksi DB batch N commit dan sebelum offset di-commit. Setelah dijalankan lagi tanpa reset, semua kriteria tetap terpenuhi (event unik, duplikat, DLQ, dan status 100 % cocok). Percobaan: 20.000 order, crash di batch 5, restart, laporan 4/4 cocok.

**Freshness.** Dengan consumer berjalan bersamaan dengan producer: p50 = 652,6 ms, p95 = 1.954 ms, maks = 2.540 ms. Lag **tumbuh** selama replay (rata-rata p50 per 50 batch: 210 → 405 → 833 → 1.277 → 1.685 ms), artinya consumer lebih lambat daripada producer yang tanpa batas kecepatan (producer 408.489 pesan dalam 22,0 s ≈ 18,6 ribu pesan/detik; consumer selesai 23,7 s sejak pesan pertama ≈ 17,2 ribu pesan/detik) dan antrean menumpuk. Ini dicatat sebagai limitation. Hipotesis yang belum diuji (next experiment): `--speed` di bawah kapasitas consumer menjaga lag tetap rendah, dan consumer ditambah (satu per partisi, 3 partisi) memangkas lag.

### Pertanyaan README: skenario crash consumer

Pesan ke-N sudah di Postgres (transaksi batch commit), offset belum di-commit, proses mati:

1. Offset terakhir yang tersimpan di Kafka masih sebelum batch itu. Setelah restart, group menugaskan ulang partisi dan consumer membaca ulang dari offset itu (`auto_offset_reset=earliest` hanya berlaku bila group belum punya offset sama sekali).
2. Batch yang sama diproses lagi. `INSERT … ON CONFLICT (event_id) DO NOTHING` membuat semua event yang sudah ada diabaikan dan dihitung duplikat; upsert `order_current` dengan `WHERE status_rank < EXCLUDED.status_rank` tidak mengubah status yang sudah sama atau lebih tinggi; baris DLQ ditolak oleh `UNIQUE (topic, partition, offset)`.
3. Hasil akhirnya sama dengan kalau crash tidak pernah terjadi. Yang menjamin: `process_batch` di [olist/consumer.py](olist/consumer.py) (transaksi + `ON CONFLICT`), urutan commit di `run_consumer` (Postgres → topic DLQ → offset), dan `enable_auto_commit=False`.

Yang tidak idempoten: **publikasi ke topic DLQ**. Kalau proses mati setelah DB commit tapi sebelum offset commit, pesan rusak yang sama dipublikasi lagi ke topic `olist.order_events.dlq`, jadi topic itu bisa berisi salinan ganda. Ini bisa diterima karena sumber kebenaran DLQ adalah tabel `stream.dlq` (idempoten lewat offset sumber), sedangkan topic DLQ hanya sarana pemberitahuan dan konsumennya wajib toleran terhadap duplikat. Producer dua kali juga menambah baris DLQ baru (offset berbeda), dan itu memang dua kejadian rusak yang berbeda.


---

## T4 — Star schema `dw`

```bash
make model        # raw.* → dw.* (±8 detik) → outputs/dw_report.md
```

SQL ada di [sql/dw/](sql/dw/) (dijalankan urut nama file) dan pemeriksaan integritas di [sql/checks/dw_integrity.sql](sql/checks/dw_integrity.sql); pelari Python hanya [olist/model.py](olist/model.py). Test: [tests/test_dw.py](tests/test_dw.py) (Postgres). Hasil: [outputs/dw_report.md](outputs/dw_report.md), `outputs/dw.json`.

**Tabel dan grain.**

| Tabel | Baris | Grain |
|---|---:|---|
| `dim_customer` | 96.096 | satu baris per orang (`customer_unique_id`) |
| `dim_geo` | 18.677 | satu baris per zip prefix + baris `unknown` |
| `dim_product` | 32.951 | satu baris per produk |
| `dim_seller` | 3.095 | satu baris per seller |
| `dim_date` | 800 | satu baris per tanggal kalender |
| `fact_order` | 99.441 | satu baris per order (header), termasuk tanpa item/pembayaran |
| `fact_order_item` | 112.650 | satu baris per unit barang, PK `(order_id, order_item_id)` |
| `fact_payment` | 103.886 | satu baris per pembayaran, PK `(order_id, payment_sequential)` |
| `fact_review` | 98.673 | satu baris per order yang punya ulasan |

PK dan FK dipasang sebagai constraint: `CREATE TABLE … AS SELECT` lalu `ADD PRIMARY KEY` (gagal bila grain salah), dan `ADD FOREIGN KEY` di [sql/dw/90_foreign_keys.sql](sql/dw/90_foreign_keys.sql) (gagal bila ada baris yatim). Pemeriksaan integritas 28 dari 28 mengembalikan 0 baris bermasalah (tabel lengkap di laporan).

**Keputusan desain.**

| Keputusan | Alternatif yang ditolak | Alasan |
|---|---|---|
| Rebuild penuh dalam satu transaksi (`DROP SCHEMA dw` → CTAS → PK → FK → cek) | Update inkremental | `dw` murni turunan `raw` (±8 detik); tanpa logika inkremental yang bisa melenceng. Gagal di tengah → rollback, `dw` lama utuh (dibuktikan test). |
| `dim_customer` per orang, atribut terbaru (SCD tipe 1) | per `customer_id`; SCD tipe 2 | `customer_id` menghitung 99.441 "pelanggan" padahal orangnya 96.096. Yang hilang pada SCD 1: riwayat pindah alamat. |
| `dim_geo` rata-rata koordinat + `mode()` kota; titik luar Brasil dikecualikan; baris `unknown` | membuang pelanggan tanpa prefix | 1.357 pelanggan dan 14 seller tidak punya prefix di geolocation; mereka menunjuk ke `unknown` sehingga FK tetap valid. `geo_key` bertipe `text` (bukan `char(5)`) supaya `'unknown'` muat. |
| `is_late` NULL untuk order yang tidak/belum terkirim, dibandingkan per **tanggal** | `false` untuk yang belum sampai; timestamp vs tanggal | `false` membuat late rate terlihat lebih bagus; estimasi hanya punya tanggal, jadi tiba pukul 10.00 pada hari estimasi tidak terlambat. |
| `dim_date.is_complete_month` = false bila order bulan itu < 10 % median bulanan | ambang 5 %; membuang bulan sparse | Ambang 5 % meloloskan 2016-10 (324 order). Dengan 10 %, bulan lengkap tepat Jan-2017 … Agu-2018 (20 bulan). Bulan sparse tidak dibuang, hanya diberi flag. |
| `dim_product` memperbaiki typo `lenght`; kategori: terjemahan → Portugis → `unknown` | menjatuhkan produk tanpa kategori | 610 produk tanpa kategori (`unknown`) dan 2.058 produk dengan kategori yang tidak punya terjemahan (nama Portugis dipakai). |
| Denormalisasi secukupnya ke fact (status, tanggal, negara bagian) | fact lebar 60 kolom | mart tidak perlu join 4 tabel; `fact_order` 23 kolom. |

**Angka kunci** (dari `dw_report.md`): late rate 6,77 % (6.534 dari 96.470 delivered dengan tanggal tiba; ada 8 delivered tanpa tanggal, `is_late` NULL), 547 order dengan > 1 ulasan, 775 order tanpa item, 1 order tanpa pembayaran.

### Pertanyaan README: kebijakan dedup `fact_review`

**Kebijakan.** Satu baris per order. Untuk order dengan lebih dari satu ulasan (547 order) dipilih ulasan dengan `review_answer_timestamp` **terbaru**; seri → `review_creation_date` terbaru → `review_id` terkecil. Jumlah ulasan asli disimpan di `review_count`, dan semua ulasan tetap ada di `raw.order_reviews`.

**Alternatif yang ditolak: rata-rata skor semua ulasan per order.** Skor tidak lagi bilangan bulat 1–5, dan perubahan pendapat pelanggan (ulasan pertama 1, ulasan berikutnya 5) hilang. Ulasan terbaru mewakili pendapat akhir.

**Efeknya ke Q2 (diukur, rata-rata skor tepat waktu vs terlambat):**

| Kebijakan | Tepat waktu | Terlambat | Observasi (tepat waktu / terlambat) |
|---|---:|---:|---:|
| Dipilih: answer terbaru | 4,290 | 2,270 | 89.443 / 6.381 |
| Ditolak: rata-rata semua ulasan | 4,291 | 2,272 | 89.443 / 6.381 |
| Ditolak: ulasan pertama | 4,291 | 2,273 | 89.443 / 6.381 |
| Ditolak: join langsung ke baris ulasan | 4,290 | 2,271 | 89.944 / 6.409 |

Jadi pilihan kebijakan hampir tidak mengubah **rata-rata** Q2 (selisih ≤ 0,003). Kesimpulan "terlambat menurunkan skor dari ±4,3 ke ±2,3" kuat terhadap pilihan ini. Yang berbeda adalah **jumlah observasi**: join langsung tanpa dedup menggandakan order dengan banyak ulasan (+501 tepat waktu, +28 terlambat), sehingga hitungan order, bobot rata-rata, dan jumlah order terlambat yang dilaporkan ikut meleset walaupun rata-ratanya tidak.
