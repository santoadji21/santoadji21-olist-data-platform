# Langkah 3 — Bronze: unduh dari Kaggle dan ingest ke MinIO

## Sumber data (v2 berdiri sendiri)
v2 **tidak mengambil apa pun dari repo induk**. Alurnya:

```
Kaggle (olistbr/brazilian-ecommerce) ──download_kaggle──▶ olist-v2/data/raw/*.csv ──ingest_bronze──▶ MinIO bronze
```
- **Kredensial:** salin `~/.kaggle/kaggle.json` ke `olist-v2/.kaggle/kaggle.json` (`chmod 600`). Folder itu di-ignore git dan di-mount read-only ke container; kredensial tidak pernah masuk repo.
- **`download_kaggle`** (`olist_v2/download.py`): idempoten (9 file sudah ada dan cocok kontrak → `present`, tanpa unduh), ekstraksi hanya untuk nama file yang dikenal (tanpa path, menolak zip-slip) ke folder sementara lalu dipindah (unduhan gagal tidak meninggalkan `data/raw` setengah terisi), dan **jumlah baris tiap file diperiksa terhadap kontrak ODCS**: versi dataset berbeda menghentikan pipeline sebelum bronze.
- **Perintah:** `make download` (mandiri) lalu `make bronze`; di Airflow: DAG `olist_v2_pipeline` = `download_kaggle` → `ingest_bronze`.
- `data/raw/` (±126 MB) di-ignore oleh `.gitignore` milik v2 sendiri.


**Status: ditulis; 13 test lolos tanpa Docker; belum dijalankan terhadap MinIO.** Kode: `olist_v2/bronze.py`, daftar tabel dibaca dari kontrak ODCS (`olist_v2/contract.py`, `contracts/olist_raw.odcs.yaml`, salinan dari T0–T6), DAG `olist_v2_pipeline` (task `ingest_bronze`), perintah mandiri `make bronze` (jalan di container scheduler), `make test` (venv lokal). Tujuan: file sumber tersimpan apa adanya di MinIO dengan bukti isi (sha256), dan ingest aman diulang.

## Desain (dibawa dari T1)
- Task Airflow `ingest_bronze` (Python, `boto3` dari provider Amazon yang sudah ada di image) membaca `/opt/airflow/data/raw` (`olist-v2/data/raw`, diisi `download_kaggle`).
- Kunci objek **berisi sha256**: `olist/<tabel>/ingest_date=YYYY-MM-DD/sha256=<12 hex>/<file>`. Isi berbeda tidak pernah menimpa objek lama.
- **Manifest per tabel di bucket** (`olist/_manifest/<tabel>.json`): sumber, `versions[]` (**sha256 dan md5**, ukuran byte, jumlah baris, **waktu ingest**), dan `latest`. Manifest ditulis **terakhir**. Tugas membolehkan sha256 atau md5; keduanya disimpan: sha256 menentukan `unchanged`, md5 dicocokkan dengan ETag S3 (berlaku untuk upload tunggal, bukan multipart).
- Status per file: `uploaded` / `unchanged` / `new_version`. Jumlah baris dihitung dengan parser CSV, bukan `wc -l`.

## Kriteria selesai
0. `make download` mengunduh 9 file dan semua jumlah barisnya cocok kontrak; run kedua `present` tanpa unduh.
1. Run DAG dua kali tanpa perubahan data: run kedua semua `unchanged`, nol upload.
2. 9 manifest ada di `bronze`; `make minio-ls` menampilkan objek di bawah `olist/<tabel>/…`.
3. Mengubah satu byte pada salinan CSV menghasilkan `new_version`, objek lama tetap ada.

## Keputusan terbuka
- **Kode ingest: salin dari T0–T6 atau tulis ulang?** `olist/bronze.py` di repo induk sudah teruji (6 test). Menyalinnya ke `olist-v2/airflow/dags/lib/` menghemat waktu, tapi membuat dua salinan. Saran: salin dulu, tandai sumbernya di komentar.
- **Kafka sebagai sumber bronze** (event order) menyusul di langkah 7; bronze batch dulu.

## Jebakan dari T1
Ukuran byte sama bukan berarti isi sama; manifest harus di bucket (bukan lokal); jangan menamai objek hanya dengan tanggal.
