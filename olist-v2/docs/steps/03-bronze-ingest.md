# Langkah 3 — Bronze: ingest CSV ke MinIO

**Status: rencana.** Tujuan: file sumber tersimpan apa adanya di MinIO dengan bukti isi (sha256), dan ingest aman diulang.

## Desain (dibawa dari T1)
- Task Airflow `ingest_bronze` (Python, `boto3` dari provider Amazon yang sudah ada di image) membaca `/opt/airflow/data/raw` (CSV Olist dari `../data/raw`, di-mount read-only).
- Kunci objek **berisi sha256**: `olist/<tabel>/ingest_date=YYYY-MM-DD/sha256=<12 hex>/<file>`. Isi berbeda tidak pernah menimpa objek lama.
- **Manifest per tabel di bucket** (`olist/_manifest/<tabel>.json`): sumber, `versions[]` (**sha256 dan md5**, ukuran byte, jumlah baris, **waktu ingest**), dan `latest`. Manifest ditulis **terakhir**. Tugas membolehkan sha256 atau md5; keduanya disimpan: sha256 menentukan `unchanged`, md5 dicocokkan dengan ETag S3 (berlaku untuk upload tunggal, bukan multipart).
- Status per file: `uploaded` / `unchanged` / `new_version`. Jumlah baris dihitung dengan parser CSV, bukan `wc -l`.

## Kriteria selesai
1. Run DAG dua kali tanpa perubahan data: run kedua semua `unchanged`, nol upload.
2. 9 manifest ada di `bronze`; `make minio-ls` menampilkan objek di bawah `olist/<tabel>/…`.
3. Mengubah satu byte pada salinan CSV menghasilkan `new_version`, objek lama tetap ada.

## Keputusan terbuka
- **Kode ingest: salin dari T0–T6 atau tulis ulang?** `olist/bronze.py` di repo induk sudah teruji (6 test). Menyalinnya ke `olist-v2/airflow/dags/lib/` menghemat waktu, tapi membuat dua salinan. Saran: salin dulu, tandai sumbernya di komentar.
- **Kafka sebagai sumber bronze** (event order) menyusul di langkah 7; bronze batch dulu.

## Jebakan dari T1
Ukuran byte sama bukan berarti isi sama; manifest harus di bucket (bukan lokal); jangan menamai objek hanya dengan tanggal.
