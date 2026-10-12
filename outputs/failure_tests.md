# Failure Test (T6)

Dijalankan 2026-10-12T13:46:00+00:00 terhadap stack yang hidup (`python pipeline.py inject`). **6/6 kasus TERTANGKAP.**

| # | Kasus | Verdict | Pulih | Symptom |
|---|---|---|:---:|---|
| a | Kolom hilang di sumber | **TERTANGKAP** | ya | silver berhenti (Postgres tidak disentuh): olist_customers_dataset.csv (customers): kolom hilang: customer_unique_id (exit 1) |
| b | Producer dijalankan dua kali | **TERTANGKAP** | ya | 40,614 pesan di topic, tetapi event tersimpan tetap 19,632 (= event unik); 20,802 duplikat diabaikan |
| c | Consumer mati sebelum commit offset | **TERTANGKAP** | ya | proses mati exit 137 dengan 5,794 event sudah di Postgres; setelah restart 2,000 pesan diproses ulang (batch ke-3 berisi 2,000 pesan) namun event tersimpan tetap 19,632 (= event unik) |
| d | Broker Kafka mati beberapa detik di tengah replay | **TERTANGKAP** | ya | outage pendek: producer exit 0, kegagalan 0, 31,502 event tiba. Outage panjang: producer naif exit 0 tanpa pesan error dan 28,113 event hilang; producer yang memeriksa hasil `send()` exit 1 dengan 29275 kegagalan terlapor |
| e | Seluruh jalur batch dijalankan ulang tanpa perubahan | **TERTANGKAP** | ya | bronze: semua file `unchanged`; 28 besaran (baris tiap tabel raw/dw/mart, Σ pendapatan, order terlambat) identik |
| f | Objek bronze berubah diam-diam di bucket | **TERTANGKAP** | ya | silver berhenti (Postgres tidak disentuh): sellers: isi objek bronze tidak cocok dengan sha256 di manifest (olist/sellers/ingest_date=2026-10-12/sha256=1f643d2b9503/olist_sellers_dataset.csv) (exit 1) |

## (a) Kolom hilang di sumber — TERTANGKAP

- **Yang dirusak:** kolom `customer_unique_id` dihapus dari salinan CSV customers, lalu `bronze` dan `silver` dijalankan
- **Symptom:** silver berhenti (Postgres tidak disentuh): olist_customers_dataset.csv (customers): kolom hilang: customer_unique_id (exit 1)
- **Mekanisme yang menangkap:** `check_header` di olist/gate.py membandingkan header dengan kontrak SEBELUM menulis apa pun; `run_silver` berhenti sebelum Postgres disentuh
- **Cara pulih:** ingest ulang CSV asli (bronze v baru; versi rusak tetap tercatat di manifest), lalu `silver` dan `load`
- **Bukti (angka):** bronze_exit = 0; silver_exit = 1; raw_customers_before = 99441; raw_customers_after = 99441; silver_parquet_unchanged = True; dq_report_unchanged = True
- Durasi uji: 17.9 s

## (b) Producer dijalankan dua kali — TERTANGKAP

- **Yang dirusak:** `stream produce --limit 5000` dijalankan dua kali berturut-turut tanpa reset (semua event dikirim ulang)
- **Symptom:** 40,614 pesan di topic, tetapi event tersimpan tetap 19,632 (= event unik); 20,802 duplikat diabaikan
- **Mekanisme yang menangkap:** `event_id` deterministik `<order_id>:<status>` = PK `stream.order_events`; `INSERT … ON CONFLICT DO NOTHING` mengabaikan duplikat; upsert `order_current` hanya menaikkan status
- **Cara pulih:** tidak perlu pemulihan (idempoten). Baris DLQ ikut berlipat karena pesan rusak run ke-2 punya offset baru; `stream reset` membersihkannya
- **Bukti (angka):** runs = 2; events_unique = 19632; events_stored = 19632; messages_total = 40614; duplicates_ignored = 20802; dlq_rows = 180; status_match = 5000/5000; consumer_exit = 0
- Durasi uji: 17.6 s

## (c) Consumer mati sebelum commit offset — TERTANGKAP

- **Yang dirusak:** consumer dijalankan sebagai proses terpisah dan dimatikan (exit 137) tepat setelah transaksi Postgres batch ke-3 commit, sebelum offset di-commit
- **Symptom:** proses mati exit 137 dengan 5,794 event sudah di Postgres; setelah restart 2,000 pesan diproses ulang (batch ke-3 berisi 2,000 pesan) namun event tersimpan tetap 19,632 (= event unik)
- **Mekanisme yang menangkap:** urutan commit di `run_consumer`: Postgres → topic DLQ → offset (`enable_auto_commit=False`); pesan dikirim ulang lalu `ON CONFLICT DO NOTHING` dan upsert `WHERE status_rank <` membuat hasil akhir sama
- **Cara pulih:** jalankan consumer lagi tanpa reset; tidak ada langkah manual
- **Bukti (angka):** crash_exit_code = 137; events_stored_at_crash = 5794; messages_in_crashed_batch = 2000; messages_redelivered = 2000; events_stored_final = 19632; events_unique_sent = 19632; status_match = 5000/5000; checks_ok = 4
- Durasi uji: 25.7 s

## (d) Broker Kafka mati beberapa detik di tengah replay — TERTANGKAP

- **Yang dirusak:** `docker stop olist-kafka` ±2 s setelah producer mulai (replay 31,502 event unik, 3.000 pesan/detik), lalu `docker start` setelah 15.9 s (pendek) dan setelah 50.0 s (panjang, melewati `request_timeout_ms` 30 s)
- **Symptom:** outage pendek: producer exit 0, kegagalan 0, 31,502 event tiba. Outage panjang: producer naif exit 0 tanpa pesan error dan 28,113 event hilang; producer yang memeriksa hasil `send()` exit 1 dengan 29275 kegagalan terlapor
- **Mekanisme yang menangkap:** errback pada tiap future `send()` + `flush()` + `SendFailure` membuat run GAGAL keras bila ada pesan yang menyerah; retry internal (`acks=all`, `retries=5`) menyerap outage pendek
- **Cara pulih:** kirim ulang producer (idempoten lewat `event_id`) lalu jalankan consumer; tidak perlu reset
- **Bukti (angka):** expected_unique_events = 31502; short_outage_s = 15.9; short_exit = 0; short_send_failures = 0; short_events_stored = 31502; long_outage_s = 50.0; naive_exit = 0; naive_reported_failures = 0; naive_silent_loss = 28113; checked_exit = 1; checked_send_failures = 29275; checked_lost_before_resend = 28308; checked_resend_exit = 0; checked_events_stored_final = 31502; status_match = 8000/8000
- Durasi uji: 173.6 s

## (e) Seluruh jalur batch dijalankan ulang tanpa perubahan — TERTANGKAP

- **Yang dirusak:** `bronze → silver → load → model → marts` dijalankan lagi tanpa mengubah data sumber
- **Symptom:** bronze: semua file `unchanged`; 28 besaran (baris tiap tabel raw/dw/mart, Σ pendapatan, order terlambat) identik
- **Mekanisme yang menangkap:** bronze: perbandingan sha256 dengan manifest; silver/load: TRUNCATE+COPY satu transaksi; dw/mart: rebuild penuh dari raw dalam satu transaksi
- **Cara pulih:** tidak ada yang perlu dipulihkan
- **Bukti (angka):** stage_seconds = {'bronze': 1.3, 'silver': 8.0, 'load': 6.3, 'model': 12.2, 'marts': 5.6}; stage_exit = {'bronze': 0, 'silver': 0, 'load': 0, 'model': 0, 'marts': 0}; quantities_compared = 28; differences = 0; bronze_statuses = ['unchanged']
- Durasi uji: 39.4 s

## (f) Objek bronze berubah diam-diam di bucket — TERTANGKAP

- **Yang dirusak:** satu byte di `olist/sellers/ingest_date=2026-10-12/sha256=1f643d2b9503/olist_sellers_dataset.csv` diubah langsung di MinIO (manifest tidak diubah)
- **Symptom:** silver berhenti (Postgres tidak disentuh): sellers: isi objek bronze tidak cocok dengan sha256 di manifest (olist/sellers/ingest_date=2026-10-12/sha256=1f643d2b9503/olist_sellers_dataset.csv) (exit 1)
- **Mekanisme yang menangkap:** `fetch_latest` di olist/silver.py menghitung sha256 objek dan membandingkannya dengan manifest sebelum memproses
- **Cara pulih:** kembalikan objek asli dari sumber/riwayat bronze, lalu `silver`
- **Bukti (angka):** silver_exit = 1; raw_sellers_before = 3095; raw_sellers_after = 3095; silver_after_restore_exit = 0
- Durasi uji: 8.6 s

## Pemulihan stack setelah semua gangguan

Replay penuh + marts + smoke: **OK** (53.9 s).

| Pemeriksaan smoke | Hasil | Detail |
|---|:---:|---|
| service postgres healthy | ya | healthy |
| service kafka healthy | ya | healthy |
| service minio healthy | ya | healthy |
| service kafka-ui healthy | ya | healthy |
| service jupyter healthy | ya | healthy |
| bronze: 9 manifest ada | ya | 9/9 |
| silver: tidak ada tabel FAIL | ya | 9/9 lolos |
| raw: baris = sumber − karantina (9 tabel) | ya | 9/9 cocok |
| dw: pemeriksaan integritas = 0 | ya | 28 pemeriksaan |
| mart: pemeriksaan = 0 | ya | 17 pemeriksaan |
| stream: 4 pemeriksaan producer vs consumer | ya | 4/4 cocok |
