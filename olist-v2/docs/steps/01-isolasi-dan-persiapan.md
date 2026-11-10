# Langkah 1: Isolasi dan persiapan

Status: ada. Tujuan langkah ini adalah memastikan eksplorasi tidak mengganggu tugas yang dinilai (tahap T0–T6 pada tugas besar) maupun proyek lain di mesin ini.

## Yang sudah dikerjakan
- Branch `explore-v2` dipotong dari tag `final` (`cf8b911`). Tag `cp1`–`cp3` dan `final` tidak bergerak selama branch ini berjalan, dan sebaiknya tidak digeser dengan `git tag -f`. Branch ini sudah digabung ke `main` lewat PR #1 dan #2; tag penilaian tetap menunjuk ke commit semula.
- Semua berkas berada di `olist-v2/`, dan tidak ada satu pun berkas di root repo yang disentuh.
- Variabel `.env` berawalan `V2_` (`V2_PG_PASSWORD`, `V2_MINIO_ROOT_USER`, …).
- `make` memakai `docker compose -p olist-v2`, dan volume serta network diberi nama eksplisit (`olist-v2-pg-data`, `olist_v2_net`, …).
- venv lokal untuk dbt: `make venv` membuat `.venv` (Python 3.11) dengan `dbt-core 1.8.9` dan `dbt-postgres 1.8.2`, versi yang sama dengan image Airflow.

## Alasan isolasinya serinci ini (pelajaran dari mesin ini)
Shell di mesin ini mengekspor `COMPOSE_PROJECT_NAME=olist` serta variabel `MINIO_*` dan `POSTGRES_*` milik T0–T6. Di Docker Compose, variabel shell mengalahkan `.env` dan `name:` di compose. Akibatnya, pada percobaan pertama compose v2 menganggap `postgres` dan `minio` miliknya sebagai service T0–T6 yang sama, lalu menggantikan container `olist-postgres` dan `olist-minio` dan memakai volume mereka. Data di volume tetap utuh, tetapi `olist-minio` sempat `unhealthy` sesudahnya.

Tiga pengaman sekarang berlaku bersamaan: `-p olist-v2`, awalan `V2_`, dan volume bernama eksplisit. Jalankan selalu lewat `make`, jangan memanggil `docker compose` langsung.

## Periksa
```bash
cd olist-v2
make config                 # compose valid untuk semua profil, tanpa menyalakan apa pun
.venv/bin/dbt --version     # core 1.8.9, postgres 1.8.2
git branch --show-current   # explore-v2
```

## Jebakan
- `docker compose up --wait` menghentikan semua service karena service sekali-jalan (`airflow-init`, `minio-init`) keluar dengan kode 0. Pakai `make up` lalu `make ps`.
- Jangan menghapus `COMPOSE_PROJECT_NAME` dari shell: stack T0–T6 bergantung padanya.
