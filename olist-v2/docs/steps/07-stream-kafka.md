# Langkah 7 — Stream: Kafka (T3)

**Status: selesai dan terbukti** pada replay penuh 99.441 order (35 detik). Service `kafka` (KRaft) dan `kafka-ui` di profil `stream`: `make up P=stream`, Kafka UI `http://localhost:18081`. Profil hanya memisahkan penyalaan demi RAM; perilaku T3 utuh.

## Perintah
| Perintah | Melakukan |
|---|---|
| `make stream` | reset → consumer (proses terpisah) → producer → laporan; replay penuh |
| `make stream-small` | 5.000 order, cepat untuk uji |
| `make stream-report` | laporan producer vs consumer dari kondisi sekarang |
| `make stream-reset` | kosongkan topic, consumer group, dan tabel `stream.*` |

Semua lewat `python -m olist_v2.stream_cli` di container scheduler.

## Hasil (`outputs/stream_report.md`)
| Besaran | Producer | Consumer |
|---|---:|---:|
| Event unik | 394.713 | 394.713 |
| Duplikat | 11.812 | 11.812 diabaikan |
| Pesan rusak | 1.964 | 1.964 di DLQ |
| Order dengan status terkini | 99.441 | 99.441 |

Status terkini = `raw.orders.order_status` pada 99.441 dari 99.441 order. Freshness p50 ≈ 837 ms, p95 ≈ 2,5 s; lag tumbuh selama replay (satu consumer, satu broker).

## Desain (dibawa dari T3)
- `event_id` deterministik `<order_id>:<status>` = PK `stream.order_events`; status hanya naik (`canceled`/`unavailable` terminal).
- Producer memberi gangguan terkontrol (±3 % dobel, ±2 % terlambat, ±0,5 % rusak, key = `order_id`); jumlahnya dicatat ke `outputs/stream_truth.json` (kunci jawaban).
- Consumer: consumer group, satu transaksi Postgres per batch, DLQ ke topic **dan** tabel, `enable_auto_commit=False`; urutan commit Postgres → DLQ → offset.
- Producer memeriksa hasil `send()` (errback + `flush` + `SendFailure`); producer naif kehilangan event tanpa error saat broker mati lama (failure test d).
- Consumer di-port ke psycopg2 (`with conn` = satu transaksi); logika lain sama dengan T0–T6. Tabel `stream.*` dibuat oleh `olist_v2/sql/10_stream.sql` (idempoten), juga sebelum `dbt build`.

## Keputusan yang diambil
Consumer tetap **Python** (bukan Spark Structured Streaming): sudah terbukti benar dan crash-safe, dan hemat RAM di laptop 7,8 GB. Kafka ±420 MB, scheduler ±430 MB saat replay.

## Belum
Stream belum menjadi task di DAG `olist_v2_pipeline` (DAG hanya jalur batch). Stream dijalankan lewat `make stream` dan ikut `make all`.
