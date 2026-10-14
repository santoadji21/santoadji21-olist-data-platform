# Langkah 2 — Stack inti: Postgres, MinIO, Airflow

**Status: ditulis, belum terverifikasi penuh.** Compose valid (`make config`). Pada percobaan pertama, console MinIO menjawab (HTTP 200) dan ketiga bucket terbentuk, tetapi Airflow gagal terhubung ke Postgres karena tabrakan volume (lihat langkah 1). Penyebabnya sudah ditutup di konfigurasi, tetapi **belum dijalankan ulang**.

## Isi inti
| Service | Peran | Catatan |
|---|---|---|
| `postgres` (16-alpine) | satu server, tiga database: `airflow`, `olist_dw`, `metabase`; schema `raw/staging/dw/mart` | dibuat oleh `initdb/01_databases.sh` hanya saat volume baru |
| `minio` (quay.io/minio/minio) | S3 + **web console :19001** | bucket `bronze`/`silver`/`gold` dibuat `minio-init` |
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
3. DAG **`olist_v2_healthcheck`** di UI dijalankan manual dan kedua task-nya hijau: `check_minio` lalu `check_warehouse`.

## Risiko
- RAM Docker hanya **7,8 GB**. Inti kira-kira 3 GB (perkiraan, belum diukur). Ukur dengan `docker stats` sebelum menyalakan profil lain.
- Build `airflow/Dockerfile` mengunduh constraints Airflow dari GitHub dan memasang dbt/datacontract di venv terpisah: butuh internet dan bisa gagal pada pin versi (belum pernah dibangun).
