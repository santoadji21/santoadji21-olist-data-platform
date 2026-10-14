# Langkah 1 — Isolasi dan persiapan

**Status: ada.** Tujuan: eksplorasi tidak boleh mengganggu tugas yang dinilai (T0–T6) maupun proyek lain di mesinmu.

## Yang sudah dikerjakan
- **Branch `explore-v2`** dipotong dari tag `final` (`cf8b911`). Tag `cp1`–`cp3` dan `final` tidak bergerak selama branch ini berjalan; jangan `git tag -f` dan jangan merge ke `main` sebelum penilaian selesai.
- **Semua file di `olist-v2/`**, tidak ada satu pun file di root repo yang disentuh.
- **Variabel `.env` berawalan `V2_`** (`V2_PG_PASSWORD`, `V2_MINIO_ROOT_USER`, …).
- **`make` memakai `docker compose -p olist-v2`**, dan volume/network diberi nama eksplisit (`olist-v2-pg-data`, `olist_v2_net`, …).
- **venv lokal untuk dbt**: `make venv` membuat `.venv` (Python 3.11) dengan `dbt-core 1.8.9` dan `dbt-postgres 1.8.2`, versi yang sama dengan image Airflow.

## Kenapa isolasinya sedetail itu (pelajaran dari mesin ini)
Shell-mu mengekspor `COMPOSE_PROJECT_NAME=olist` dan variabel `MINIO_*`/`POSTGRES_*` milik T0–T6. Di Docker Compose, **variabel shell mengalahkan `.env` dan `name:` di compose**. Akibatnya, percobaan pertama compose v2 menganggap `postgres` dan `minio`-nya sebagai service T0–T6 yang sama, **menggantikan container `olist-postgres` dan `olist-minio`**, lalu memakai volume mereka. Data di volume tetap utuh, tetapi `olist-minio` sempat `unhealthy` sesudahnya.

Tiga pengaman sekarang berlaku bersamaan: `-p olist-v2`, awalan `V2_`, dan volume bernama eksplisit. **Selalu jalankan lewat `make`**, jangan `docker compose` langsung.

## Periksa
```bash
cd olist-v2
make config                 # compose valid untuk semua profil, tanpa menyalakan apa pun
.venv/bin/dbt --version     # core 1.8.9, postgres 1.8.2
git branch --show-current   # explore-v2
```

## Jebakan
- `docker compose up --wait` menghentikan semuanya karena service sekali-jalan (`airflow-init`, `minio-init`) keluar dengan kode 0. Pakai `make up` lalu `make ps`.
- Jangan menghapus `COMPOSE_PROJECT_NAME` dari shell: stack T0–T6 bergantung padanya.
