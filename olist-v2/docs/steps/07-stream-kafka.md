# Langkah 7 — Stream: Kafka

**Status: ditulis; WAJIB untuk tugas (T3, bobot 20 %, terbesar).** Service `kafka` (KRaft, tanpa Zookeeper) dan `kafka-ui` ada di profil `stream`; image sudah ada di mesin. Profil hanya memisahkan penyalaan demi RAM, **bukan** membuat Kafka opsional: tahap ini harus ikut `make all` dan failure test (c) dan (d). Nyalakan: `make up P="stream"`, Kafka UI `http://localhost:18081`.

## Desain (dibawa dari T3)
- `event_id` deterministik `<order_id>:<status>`; status hanya naik (`canceled`/`unavailable` terminal).
- Gangguan terkontrol di producer (dobel, terlambat, rusak); consumer idempoten; pesan rusak ke DLQ.
- Urutan commit: tulis ke Postgres dulu, baru commit offset.

## Keputusan terbuka
**Consumer: Python (seperti T3) atau Spark Structured Streaming?** Python sudah terbukti (394.713 event unik cocok persis, crash aman). Spark Structured Streaming lebih "pas" dengan stack, tetapi menambah beban RAM di laptop 7,8 GB. Saran: port consumer Python dulu, ukur, baru pertimbangkan Spark.

## Kriteria selesai (dari tugas T3)
Empat pemeriksaan producer vs consumer cocok (event unik, duplikat, DLQ, status 100 %), dan replay sampai freshness tercatat di `mart.stream_freshness`.

## Risiko
Kafka + Kafka UI memakan ±1,5 GB. Jangan menyalakan semua profil sekaligus tanpa mengukur dengan `docker stats`.

## Syarat T3 yang harus dipenuhi (tugas §5, T3)
- Producer membaca `raw.orders`, key = `order_id`, mencatat jumlah gangguan (±3 % dobel, ±2 % terlambat, ±0,5 % rusak) ke file kunci jawaban.
- Consumer: consumer group, `enable_auto_commit=False`, DLQ ke **topic dan tabel**, `stream.order_events` (satu baris per event unik) dan `stream.order_current` (satu baris per order).
- Metrik per batch: pesan, baru, duplikat diabaikan, out-of-order, DLQ, freshness p50/p95.
- `outputs/stream_report.md`: producer vs consumer, dan status terkini vs `raw.orders.order_status`.
- Consumer dimatikan paksa lalu dijalankan lagi tanpa reset: semua kriteria tetap terpenuhi. Replay 99.441 order < 5 menit.
