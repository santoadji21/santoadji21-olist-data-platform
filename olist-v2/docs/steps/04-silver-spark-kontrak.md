# Langkah 4 — Silver: Spark dan gate kontrak ODCS

**Status: rencana** (image Spark/Airflow sudah ditulis di `spark/` dan `airflow/`, belum dibangun). Tujuan: data bertipe sesuai kontrak, dengan aturan kualitas hard dan soft, baris bermasalah dikarantina, dan kontrak ODCS menjadi **gate** di DAG.

## Pembagian peran
| Alat | Tugas |
|---|---|
| Spark | baca `s3a://bronze/…`, ubah ke tipe kontrak, tulis Parquet ke `s3a://silver/…` dan baris bermasalah ke karantina |
| datacontract (venv Airflow) | `datacontract test` terhadap kontrak ODCS = **gate**: gagal → DAG berhenti |
| Postgres | menerima `raw.*` dari silver (langkah `load_raw`) |

## Kontrak ODCS
Mulai dari `contracts/olist_raw.odcs.yaml` milik T0–T6 (sudah lint-valid, 8 warning soft yang cocok dengan profil). Aturan hard = `severity: error`, soft = `warning`. Satu klausa `WHERE` yang sama dipakai di kontrak, Spark SQL, dan dbt test supaya tidak ada tiga versi aturan.

## Keputusan terbuka (penting, harus diputuskan sebelum menulis job)
Image Airflow tidak punya Java, jadi cara Airflow menjalankan Spark harus dipilih:
1. **`spark-submit` client mode dari Airflow** (yang saat ini ditulis di Dockerfile: JRE + pyspark 3.5): driver di container Airflow, executor di `spark-worker`. Butuh `spark.driver.host` yang bisa dijangkau worker dan versi pyspark = 3.5.3.
2. **Spark Connect**: klien ringan tanpa Java, server di container Spark. Lebih ringan untuk laptop 7,8 GB, fiturnya terbatas pada DataFrame/SQL.
3. **Spark lokal di dalam task** (tanpa cluster): paling sederhana, tapi tidak lagi "cluster".

Catatan jujur: data 126 MB terlalu kecil untuk membutuhkan Spark (di T0–T6, DuckDB mengerjakan 1 juta baris geolokasi dalam hitungan detik). Nilai langkah ini adalah belajar dan **membandingkan durasi Spark vs DuckDB**.

## Kriteria selesai
- 9 tabel silver bertipe (timestamp, numeric, kode pos teks 5 digit), baris silver + karantina = baris sumber.
- Kolom hilang di sumber menghentikan DAG **sebelum** menyentuh Postgres, dengan pesan yang menyebut kolomnya.
- `datacontract test` lolos (hanya warning soft) dan menjadi task `contract_gate` di DAG.

## Risiko
S3A butuh pasangan `hadoop-aws 3.3.4` + `aws-java-sdk-bundle 1.12.262` (sudah di `spark/Dockerfile`); tag image `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu` belum diverifikasi.
