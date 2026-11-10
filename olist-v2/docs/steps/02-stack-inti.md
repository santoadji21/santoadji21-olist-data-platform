# Langkah 2: Stack inti (Postgres, MinIO, Airflow)

Status: selesai dan terbukti. Compose valid, ketiga image kustom terbangun, dan seluruh inti (Postgres, MinIO, Airflow) sehat. Bucket dan schema terbentuk, dan `olist_v2_healthcheck` hijau. Tabrakan volume dengan T0–T6 (langkah 1) sudah ditutup di konfigurasi.

## Isi inti
| Service | Peran | Catatan |
|---|---|---|
| `postgres` (16-alpine) | satu server, tiga database: `airflow`, `olist_dw`, `metabase`; schema `raw/staging/dw/mart` | dibuat oleh `initdb/01_databases.sh` hanya saat volume baru |
| `minio` (quay.io/minio/minio) | S3 dan web console di port 19001 | bucket `bronze`/`silver`/`gold` dibuat `minio-init` |
| `airflow-init` → `airflow-webserver`, `airflow-scheduler` | LocalExecutor, tanpa worker/redis | image kustom di `airflow/` (Java, pyspark klien, venv dbt dan datacontract) |

## Jalankan
```bash
cd olist-v2
make init && make build     # pertama kali: build image airflow (beberapa menit)
make up                     # inti saja
make ps                     # tunggu postgres, minio, airflow-webserver, airflow-scheduler = healthy
```
Buka: Airflow `http://localhost:18080` (admin / admin dari `.env`), MinIO `http://localhost:19001`.

## Hasil yang harus terlihat
1. `make minio-ls` menampilkan bucket `bronze`, `silver`, `gold` (kosong).
2. `make psql` lalu `\dn` menampilkan schema `raw`, `staging`, `dw`, `mart`.
3. DAG `olist_v2_healthcheck` di UI dijalankan manual dan kedua task-nya hijau: `check_minio` lalu `check_warehouse`.

## Risiko
- RAM Docker hanya 7,8 GB. Hasil ukur `docker stats` dengan profil `stream` dan `bi` menyala: webserver ±1,1 GB, scheduler ±0,4 GB (batas 3 GB karena Spark berjalan di dalamnya), Kafka ±0,4 GB, Metabase ±1 GB (batas 1 GB, mepet), Postgres ±0,2 GB, MinIO ±0,1 GB.
- Build `airflow/Dockerfile` mengunduh constraints Airflow dari GitHub dan memasang dbt/datacontract/kaggle di venv terpisah serta jar S3A ke pyspark, jadi membutuhkan internet. Build ini sudah berhasil (`make build-core`).
- Jangan memakai `up --wait` karena service sekali-jalan keluar dengan kode 0. Pakai `make up` lalu `make ps`.
