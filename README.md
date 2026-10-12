# Olist Data Platform — Tugas Besar

Platform data lengkap di atas stack lokal (PostgreSQL, Kafka, MinIO, Jupyter): dari 9 CSV mentah Olist (99.441 order, ±1,5 juta baris) sampai
mart, dashboard, dan bukti bahwa semuanya benar, bisa diulang, dan tahan gangguan.

| Dokumen | Isi |
|---|---|
| [DATA_CONTRACT.md](DATA_CONTRACT.md) | tabel, tipe, kunci, 18 temuan kotor beserta angka dan keputusannya (T0) |
| [EVALUATION_EVIDENCE.md](EVALUATION_EVIDENCE.md) | durasi, baris per lapisan, hasil gate/stream/dw/failure test (dibangkitkan dari `outputs/*.json`) |
| [RUNBOOK.md](RUNBOOK.md) | menyalakan dari nol, memeriksa tiap lapisan, symptom → perbaikan, reset |
| [outputs/](outputs/) | laporan tiap tahap: `profile`, `bronze_report`, `dq_report`, `load_report`, `stream_report`, `dw_report`, `marts_report`, `failure_tests`, `dashboard/*.png` |

## Arsitektur

```
 data/raw/*.csv (9 file, Kaggle)
        │  T1 bronze: sha256 + manifest append-only
        ▼
 MinIO  bronze ──T2 silver (tipe + gate hard/soft + karantina, Parquet)──▶ silver ──T2 load (TRUNCATE+COPY, 1 transaksi)──┐
                                                                                                                         ▼
                                    PostgreSQL  raw.* ──T4 model (SQL, PK/FK, 28 cek)──▶ dw.* ──T5 marts (17 cek)──▶ mart.* ──▶ gold (Parquet+CSV)
                                                  │                                                                      │
                                                  │ T3 producer (replay + gangguan) ──▶ Kafka ──▶ consumer ──▶ stream.* ─┘ (freshness)
                                                  ▼                                                                      ▼
                                             raw.orders                                                       dashboard (notebook) + marts_report.md
```

| Lapisan | Tempat | Sifat |
|---|---|---|
| bronze | MinIO | file sumber apa adanya + manifest per tabel; bisa diaudit |
| silver | MinIO (Parquet) | bertipe, lolos gate kualitas, baris bermasalah di `_quarantine/` |
| raw | PostgreSQL | salinan silver; dimuat idempoten dan atomik |
| stream | PostgreSQL | log event unik + status terkini per order + DLQ + metrik batch |
| dw | PostgreSQL | star schema, grain tegas, PK/FK terpasang |
| mart / gold | PostgreSQL + MinIO | tabel siap dashboard + rekonsiliasi; diekspor Parquet/CSV |

## Cara menjalankan (≤ 10 perintah dari nol)

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt    # 1. sekali saja
python scripts/get_dataset.py                                         # 2. 9 CSV → data/raw (Kaggle; cadangan mirror)
make reset                                                            # 3. docker compose down -v (lewati bila belum pernah jalan)
make all                                                              # 4. init → up → check → profile → bronze → silver → load → stream → model → marts
make smoke                                                            # 5. semua [ok]
make inject                                                           # 6. failure injection → outputs/failure_tests.md (±8 menit)
make report                                                           # 7. EVALUATION_EVIDENCE.md
.venv/bin/python -m pytest -q                                         # 8. test (yang butuh Docker di-skip bila stack mati)
```

`make all` menjalankan sepuluh tahap sebagai satu perintah; durasi tiap tahap dicatat di `outputs/run_all.json`. Satu tahap saja: `make bronze|silver|load|stream|model|marts`.
Semua perintah memakai satu pintu `python pipeline.py <tahap>`; `make help` mencantumkan targetnya.

## Struktur repository

```
pipeline.py              satu pintu (init/up/down, check/profile/contract, bronze/silver/load, stream, model/marts, smoke/inject/report, all)
olist/                   schema.py (kontrak terpusat) · contract*.py · bronze · gate · silver · load · stream · producer · consumer ·
                         model · marts · dashboard · failures · smoke · evidence
sql/                     00_schemas · 10_stream · dw/ · mart/ · checks/  (SQL di file, bukan string Python)
tests/                   tanpa Docker: contract, gate, bronze, stream · dengan Docker: load, consumer, dw, marts
notebooks/dashboard.ipynb   dashboard (membaca mart.* dari Postgres)
contracts/               kontrak ODCS (dibangkitkan dari olist/schema.py)
outputs/                 laporan dan angka tiap tahap (dihasilkan kode)
```

Rincian tiap tahap, keputusan desain, dan jawaban pertanyaan README ada di bagian berikut (T0 sampai T6).

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
- Ingest pertama 9 file (±126 MB) pada run bersih: 2,1 detik untuk seluruh perintah `bronze`, batas tugas 2 menit (`outputs/run_all.json`). Semua `uploaded`.
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
make stream           # reset → consumer (proses terpisah) → producer → laporan; ±43,3 detik untuk 99.441 order
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

Event terlambat dikirim 7.988; consumer mencatat 6.490 event out-of-order. Angkanya wajar berbeda: event yang ditahan hanya out-of-order bila status berikutnya sudah tiba, dan data asli menghasilkan out-of-order alami (tanggal tiba lebih awal dari tanggal kurir). Replay selesai 43,3 detik termasuk reset dan laporan (producer saja 22,1 s; batas 5 menit). Duplikat di sisi consumer dihitung sebagai total pesan di topic − event unik − DLQ, sehingga tetap tepat walau ada pesan yang dikirim ulang setelah crash.

**Consumer dimatikan paksa.** `--crash-at-batch N` memanggil `os._exit(137)` setelah transaksi DB batch N commit dan sebelum offset di-commit. Setelah dijalankan lagi tanpa reset, semua kriteria tetap terpenuhi (event unik, duplikat, DLQ, dan status 100 % cocok). Diuji otomatis di failure test (c): crash di batch ke-3 dengan 5.794 event sudah di Postgres, setelah restart 2.000 pesan (satu batch penuh) diproses ulang, dan hasil akhir tetap 19.632 event unik dan status 5000/5000 cocok.

**Freshness.** Dengan consumer berjalan bersamaan dengan producer: p50 = 761,1 ms, p95 = 1.958 ms, maks = 2.423 ms. Lag **tumbuh** selama replay (rata-rata p50 per 50 batch: 231 → 439 → 968 → 1.383 → 1.738 ms), artinya consumer lebih lambat daripada producer yang tanpa batas kecepatan (producer 408.489 pesan dalam 22,1 s ≈ 18,5 ribu pesan/detik; consumer selesai 23,8 s sejak pesan pertama ≈ 17,2 ribu pesan/detik) dan antrean menumpuk. Ini dicatat sebagai limitation. Hipotesis yang belum diuji (next experiment): `--speed` di bawah kapasitas consumer menjaga lag tetap rendah, dan consumer ditambah (satu per partisi, 3 partisi) memangkas lag.

### Pertanyaan README: skenario crash consumer

Pesan ke-N sudah di Postgres (transaksi batch commit), offset belum di-commit, proses mati:

1. Offset terakhir yang tersimpan di Kafka masih sebelum batch itu. Setelah restart, group menugaskan ulang partisi dan consumer membaca ulang dari offset itu (`auto_offset_reset=earliest` hanya berlaku bila group belum punya offset sama sekali).
2. Batch yang sama diproses lagi. `INSERT … ON CONFLICT (event_id) DO NOTHING` membuat semua event yang sudah ada diabaikan dan dihitung duplikat; upsert `order_current` dengan `WHERE status_rank < EXCLUDED.status_rank` tidak mengubah status yang sudah sama atau lebih tinggi; baris DLQ ditolak oleh `UNIQUE (topic, partition, offset)`.
3. Hasil akhirnya sama dengan kalau crash tidak pernah terjadi. Yang menjamin: `process_batch` di [olist/consumer.py](olist/consumer.py) (transaksi + `ON CONFLICT`), urutan commit di `run_consumer` (Postgres → topic DLQ → offset), dan `enable_auto_commit=False`.

Yang tidak idempoten: **publikasi ke topic DLQ**. Kalau proses mati setelah DB commit tapi sebelum offset commit, pesan rusak yang sama dipublikasi lagi ke topic `olist.order_events.dlq`, jadi topic itu bisa berisi salinan ganda. Ini bisa diterima karena sumber kebenaran DLQ adalah tabel `stream.dlq` (idempoten lewat offset sumber), sedangkan topic DLQ hanya sarana pemberitahuan dan konsumennya wajib toleran terhadap duplikat. Producer dua kali juga menambah baris DLQ baru (offset berbeda), dan itu memang dua kejadian rusak yang berbeda.


---

## T4 — Star schema `dw`

```bash
make model        # raw.* → dw.* (±13,5 detik) → outputs/dw_report.md
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
| Rebuild penuh dalam satu transaksi (`DROP SCHEMA dw` → CTAS → PK → FK → cek) | Update inkremental | `dw` murni turunan `raw` (±13,5 detik); tanpa logika inkremental yang bisa melenceng. Gagal di tengah → rollback, `dw` lama utuh (dibuktikan test). |
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


---

## T5 — Mart & dashboard

```bash
make marts        # dw/stream → mart.* → ekspor gold (Parquet + CSV) → marts_report.md → 5 grafik PNG   (±5,8 detik)
```

Dashboard: notebook [notebooks/dashboard.ipynb](notebooks/dashboard.ipynb) di JupyterLab (`http://localhost:8888`, token di `.env`), membaca `mart.*` langsung dari Postgres; PNG-nya ada di `outputs/dashboard/`. Kode: [olist/marts.py](olist/marts.py), [olist/dashboard.py](olist/dashboard.py), SQL di [sql/mart/](sql/mart/), pemeriksaan di [sql/checks/mart_checks.sql](sql/checks/mart_checks.sql). Test: [tests/test_marts.py](tests/test_marts.py). Hasil: [outputs/marts_report.md](outputs/marts_report.md) (Q1–Q4 dalam tabel, dibangkitkan oleh query yang sama dengan grafik), `outputs/marts.json`.

**Mart** (semua dibangun dalam satu transaksi bersama 17 pemeriksaan yang harus 0; gagal → rollback, mart lama utuh):

| Mart | Grain | Menjawab |
|---|---|---|
| `sales_monthly` | bulan × kategori × negara bagian | Q1 |
| `revenue_vs_paid_monthly` | bulan | Q1 (cocok dengan uang dibayar?) |
| `delivery_monthly` | bulan × negara bagian | Q2 |
| `seller_scorecard` | seller | Q3 |
| `customer_rfm` | orang (`customer_unique_id`) | Q4 |
| `reconciliation` | order | rekonsiliasi nilai pesanan vs dibayar |
| `stream_freshness` / `stream_freshness_batch` | menit / batch consumer | freshness stream |

Semua mart juga diekspor ke bucket `gold` (`olist/mart/<mart>.parquet` dan `.csv`); lihat dengan `make minio-ls`.

**Definisi yang dipilih (dan alasannya).**

| Hal | Keputusan | Alasan |
|---|---|---|
| Pendapatan | Σ `price` item pada order berstatus bukan `canceled`/`unavailable`; ongkir terpisah | `payment_value` ikut menghitung bunga cicilan, voucher, dan order tanpa item (162.591,95 BRL dibayar di 775 order tanpa item) |
| Bulan | hanya bulan lengkap (Jan-2017 … Agu-2018) di dashboard; bulan sparse tetap ada di mart dengan flag | notebook punya saklar `COMPLETE_ONLY` |
| Late rate | terlambat ÷ order dengan `is_late IS NOT NULL` | order yang belum sampai tidak dihitung "tepat waktu" |
| Seller dinilai | minimal 30 order (634 dari 3.095 seller, mencakup 83 % pasangan seller-order) | seller dengan 2 order dan 1 terlambat bukan "50 % terlambat" yang bermakna |
| Seller perlu ditindak | late rate ≥ 15 % (≈ 2,2× rata-rata 6,7 %), batal ≥ 5 %, atau skor ≤ 3,5 | ambang tertulis di SQL; seller tak dinilai tidak pernah ditandai |
| RFM | acuan recency = tanggal beli terakhir di data; R dan M kuintil; **F pita tetap 1, 2, 3, 4, ≥ 5** | 92.102 dari 94.983 orang (97 %) hanya belanja sekali: `ntile(5)` atas F memecah ribuan nilai yang sama (F = 1) ke lima skor secara acak |
| Rekonsiliasi | toleransi 0,01 BRL; kelas berurutan: tanpa_item → tanpa_pembayaran → cocok → voucher → cicilan → tidak_dijelaskan | tiap order tepat satu kelas, tidak ada yang disembunyikan |

**Jawaban (dari `outputs/marts_report.md`; tiap kalimat dihitung dari data, bukan diketik).**
- **Q1.** Pendapatan Jan-2017 s.d. Agu-2018 13.449.530 BRL dari 111.752 item. Uang terbayar 15.686.469 BRL vs nilai pesanan 15.683.707 BRL (+0,02 %) pada order yang punya item dan pembayaran, jadi angkanya cocok. Kategori terbesar `health_beauty` (9,3 %).
- **Q2.** Late rate 6,79 % dari 96.203 order terkirim. Tertinggi AL (21,5 %, n=396) dan, di antara negara bagian ≥ 500 order, MA (17,5 %, n=713). Skor review 4,29 (tepat waktu) vs 2,27 (terlambat).
- **Q3.** 54 dari 634 seller yang dinilai perlu ditindak: 38 karena late rate, 4 karena batal, 25 karena skor (bisa lebih dari satu alasan).
- **Q4.** 2.887 dari 94.983 orang (3,0 %) belanja ulang (angka ini dari order bernilai saja, jadi sedikit di bawah 2.997 di profil T0 yang menghitung semua order). 14.010 orang bernilai tinggi sudah lama tidak kembali (rata-rata 444 hari, total 3.895.230 BRL).
- **Freshness.** Lihat bagian T3: p50 batch awal 237 ms menjadi 1.522 ms di batch akhir (lag tumbuh selama replay).

**Rekonsiliasi:** 98,91 % order cocok (≥ 98 % dipenuhi). Sisanya: 775 tanpa item, 1 tanpa pembayaran, 246 cicilan, 7 voucher, 50 tidak terjelaskan.

**Penelusuran satu angka.** Contoh: pendapatan Q1 = `sum(mart.sales_monthly.revenue)` = `sum(dw.fact_order_item.price)` untuk order bukan `canceled`/`unavailable`; pemeriksaan `sales_revenue_equals_fact_items` bernilai 0 di tiap build. Tabel mart → SQL → fakta ada di akhir `marts_report.md`.

### Pertanyaan README: selisih uang yang tidak bisa dijelaskan, dan pertanyaan ke finance

**Yang tidak terjelaskan oleh aturan apa pun:** 50 order, 201,44 BRL selisih absolut (netto −196,72 BRL: 39 order dibayar kurang total 199,08 BRL, hingga 51,62 BRL per order; 11 order dibayar lebih total 2,36 BRL). Semuanya berstatus `delivered`, tanpa voucher, dan 30 di antaranya bukan cicilan.

Dua angka lain bukan "tidak terjelaskan" tetapi sudah terklasifikasi dan lebih besar, jadi tetap perlu jawaban:
- **775 order tanpa item tetap punya pembayaran 162.591,95 BRL** (603 `unavailable` 124.339,02; 164 `canceled` 37.337,87; sisanya `created`/`invoiced`/`shipped`).
- **1 order `delivered` tanpa pembayaran** senilai 143,46 BRL.

**Yang saya tanyakan ke finance Olist:**
1. Apakah 162.591,95 BRL di order tanpa item sudah direfund? Kalau tidak ada tabel refund, apakah itu uang yang masih tertahan?
2. Untuk 39 order yang dibayar kurang (maks 51,62 BRL): apakah ada diskon, kupon, atau pengembalian parsial yang tidak tercatat di data pembayaran? Mengapa 30 di antaranya bukan cicilan?
3. Bagaimana order `delivered` senilai 143,46 BRL bisa tidak punya baris pembayaran (dibayar di luar sistem, atau baris hilang saat ekstrak)?
4. Apakah bunga cicilan (246 order, 3.050,30 BRL lebih dari nilai pesanan) memang pendapatan Olist atau pendapatan pihak pembayaran?


---

## T6 — Failure test, evidence, runbook

```bash
make reset && make all     # dari docker compose down -v sampai dashboard siap
make inject                # failure injection otomatis (6 kasus) + pemulihan + smoke
make report                # EVALUATION_EVIDENCE.md
make smoke                 # pemeriksaan kesehatan kapan saja
```

Kode: [olist/failures.py](olist/failures.py), [olist/smoke.py](olist/smoke.py), [olist/evidence.py](olist/evidence.py). Hasil: [outputs/failure_tests.md](outputs/failure_tests.md), [EVALUATION_EVIDENCE.md](EVALUATION_EVIDENCE.md) (dibangkitkan dari `outputs/*.json`), [RUNBOOK.md](RUNBOOK.md). Test: [tests/test_evidence.py](tests/test_evidence.py).

**Dari `docker compose down -v` sampai dashboard siap: 108,8 detik (1,8 menit)**, batas tugas 15 menit. `make all` adalah satu perintah untuk 10 tahap (`init`, `up`, `check`, `profile`, `bronze`, `silver`, `load`, `stream`, `model`, `marts`), jadi total ≤ 10 perintah. Durasi per tahap (detik): init 0,0, up 21,6, check 1,2, profile 7,7, bronze 2,1, silver 7,5, load 6,0, stream 43,3, model 13,5, marts 5,8 (`outputs/run_all.json`).

**Failure injection: 6/6 kasus TERTANGKAP, pemulihan stack OK, smoke 11/11.** Tiap kasus adalah fungsi yang mengembalikan verdict (bukan skrip yang mencetak); pelapor terpisah. Consumer benar-benar dimatikan (proses terpisah, exit 137), broker dihentikan dengan `docker stop`, dan file sumber dirusak di salinan sementara.

| # | Kasus | Verdict | Pulih | Symptom |
|---|---|---|:---:|---|
| a | Kolom hilang di sumber | TERTANGKAP | ya | silver berhenti (Postgres tidak disentuh): olist_customers_dataset.csv (customers): kolom hilang: customer_unique_id (exit 1) |
| b | Producer dijalankan dua kali | TERTANGKAP | ya | 40,614 pesan di topic, tetapi event tersimpan tetap 19,632 (= event unik); 20,802 duplikat diabaikan |
| c | Consumer mati sebelum commit offset | TERTANGKAP | ya | proses mati exit 137 dengan 5,794 event sudah di Postgres; setelah restart 2,000 pesan diproses ulang (batch ke-3 berisi 2,000 pesan) namun event tersimpan tetap 19,632 (= event unik) |
| d | Broker Kafka mati beberapa detik di tengah replay | TERTANGKAP | ya | outage pendek: producer exit 0, kegagalan 0, 31,502 event tiba. Outage panjang: producer naif exit 0 tanpa pesan error dan 28,113 event hilang; producer yang memeriksa hasil `send()` exit 1 dengan 29275 kegagalan terlapor |
| e | Seluruh jalur batch dijalankan ulang tanpa perubahan | TERTANGKAP | ya | bronze: semua file `unchanged`; 28 besaran (baris tiap tabel raw/dw/mart, Σ pendapatan, order terlambat) identik |
| f | Objek bronze berubah diam-diam di bucket | TERTANGKAP | ya | silver berhenti (Postgres tidak disentuh): sellers: isi objek bronze tidak cocok dengan sha256 di manifest (olist/sellers/ingest_date=2026-10-12/sha256=1f643d2b9503/olist_sellers_dataset.csv) (exit 1) |

**Pelajaran dari kasus (d).** Dengan outage pendek producer pun tidak kehilangan pesan (retry internal kafka-python). Pada outage 50,0 detik (melewati `request_timeout_ms` 30 s), producer yang future `send()`-nya tidak diperiksa **kehilangan 28.113 dari 31.502 event tanpa satu pun pesan error dan keluar dengan exit 0**. Producer yang memeriksa hasil `send()` (errback + `flush()` + `SendFailure`) gagal keras: exit 1 dengan 29.275 kegagalan terlapor; kirim ulang yang idempoten (`event_id`) memulihkan semua 31.502 event.

**Dua kriteria uji yang salah dan saya perbaiki selama pengerjaan** (bukan perubahan desain): (1) pada kasus (c) saya semula menuntut batch pertama setelah restart berisi duplikat, padahal urutan partisi berubah dan redelivery muncul di batch lain; ukuran yang benar dan eksak adalah Σ pesan diproses − pesan di topic. (2) Pada kasus (e) baseline mart freshness basi karena kasus stream sebelumnya mengubah `stream.batch_metrics`; baseline kini disegarkan dulu.

### Pertanyaan README: data 100× lebih besar dan SLA freshness 60 detik

Titik awalnya adalah yang sudah terukur pada skala sekarang: replay penuh memproses ±408.489 pesan, dan satu consumer Python tidak mengejar producer (p95 freshness 1.958 ms dan lag tumbuh selama replay). Pada 100× data, consumer akan jauh dari SLA 60 detik dan komponen berikut patah lebih dulu:

1. **Satu consumer Python + satu broker Kafka (3 partisi, replikasi 1).** Lag tumbuh sejak skala 1×; pada 100× antrean mencapai puluhan juta pesan dan p95 freshness jauh melewati 60 detik. Satu broker juga berarti satu titik gagal. *Ganti dengan:* topic dengan jumlah partisi sebanding paralelisme (consumer group satu consumer per partisi), broker ≥ 3 dengan `replication.factor=3` dan `min.insync.replicas=2`, atau engine stream (Flink / Spark Structured Streaming) yang menulis ke tabel dengan upsert berbatch.
2. **Postgres sebagai satu-satunya penulis + rebuild penuh `dw` dan `mart`.** Upsert per batch ke `order_current` dan indeks `order_events` menjadi bottleneck tulis; build `dw` dan `mart` (13,5 s dan 5,8 s hari ini) tumbuh linear dengan data sehingga tidak mungkin memenuhi SLA 60 detik. *Ganti dengan:* tabel stream dipartisi per tanggal, `dw`/`mart` inkremental (hanya partisi yang berubah, atau materialized view yang di-refresh bertahap), dan mart real-time di store analitik kolumnar (mis. ClickHouse/DuckDB di atas Parquet) alih-alih Postgres OLTP.
3. **Silver dan ingest memuat satu CSV utuh ke memori.** File geolokasi ±1 juta baris (±60 MB) tidak masalah; pada 100× menjadi ±6 GB dalam satu proses pandas, dan `COPY` satu transaksi untuk semua tabel menahan lock lama. *Ganti dengan:* pembacaan per-chunk atau Spark/DuckDB di atas Parquet bertingkat, ingest ke bronze dengan multipart upload paralel, dan `load` per partisi atau per tabel dengan swap tabel (bukan satu transaksi raksasa).

Yang tidak perlu berubah: `event_id` deterministik, aturan status naik-saja, dan urutan commit Postgres → offset. Merekalah yang membuat consumer yang di-scale-out tetap benar (duplikat dan event tidak urut tetap aman).

## Alat bantu

Kode, SQL, dan dokumen di repository ini ditulis dengan bantuan **Claude Code** (agen AI berbasis model Claude) yang bekerja langsung di repository ini. Pembagian kerja:

| Dibantu AI | Dikerjakan dan diputuskan pemilik |
|---|---|
| Menulis modul Python (`olist/*`), SQL (`sql/*`), test, Makefile, dan draf dokumen | Memilih tahap dan urutan kerja, meninjau setiap perubahan, menjalankan dan meng-commit sendiri, memberi tag checkpoint |
| Menjalankan profiling, mengusulkan angka dan ambang (mis. ambang karantina, 30 order minimum seller, 10 % bulan lengkap) | Menyetujui keputusan desain dan memutuskan arah (mis. dashboard notebook, bukan Power BI) |
| Menemukan dan memperbaiki bug lewat test (kode pos kosong di gate, derau float di profil, lock Postgres yang menggantung, kriteria test crash) | Memverifikasi hasil di mesin sendiri |

> **Catatan untuk pemilik repo:** kolom kanan menggambarkan pembagian kerja yang terjadi selama sesi. Sebelum dikumpulkan, tambahkan di sini bagian spesifik yang kamu ubah atau tulis sendiri; penilai akan menanyakan penjelasan baris demi baris, dan "apa yang kamu ubah" harus berasal darimu.
