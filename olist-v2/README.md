# olist-v2 (eksplorasi)

Airflow + MinIO + Spark + dbt + Metabase + datacontract (ODCS) + Kafka di atas Postgres. Terisolasi dari stack T0–T6 (project `olist`).

## Profil dan port
| Profil | Service | Port host |
|---|---|---|
| (inti) | Postgres `15432`, MinIO API `19000` + **web console `19001`**, Airflow `18080` | |
| `spark` | spark-master (UI `18090`, RPC `17077`), spark-worker (UI `18091`) | |
| `stream` | Kafka `19092`, Kafka UI `18081` | |
| `bi` | Metabase `13000` (tema gelap: Settings → Appearance) | |
| `lab` | Jupyter `18888` (token di `.env`), notebook di `notebooks/` | |
| `contract` | datacontract editor `18082` | |

```bash
make init && make build      # sekali (build image airflow, spark, jupyter)
make up                      # inti;  make up P="spark stream";  make up-all
make ps                      # status (JANGAN pakai `up --wait`)
make reset                   # hapus volume olist-v2-* saja
```

## Reset (tiga tingkat; semuanya meminta konfirmasi, lewati dengan `YES=1`)

| Perintah | Menghapus | Tetap ada | Untuk apa |
|---|---|---|---|
| `make reset-layers` | objek di bucket `bronze`/`silver`/`gold` (termasuk manifest dan riwayat versi) dan isi schema `raw`/`staging`/`dw`/`mart` | container, volume, riwayat run Airflow, `data/raw`, kode | mengulang pipeline dari awal dengan cepat, tanpa membangun ulang stack |
| `make reset` | **semua volume olist-v2** (Postgres termasuk database `airflow`/`metabase`, MinIO, Kafka); log Airflow ada di `airflow/logs/` dan TIDAK ikut terhapus | CSV di `data/raw`, kode, `.env`, `outputs/` | mulai dari stack kosong; `make up` lagi sesudahnya |
| `make reset-all` | seperti `reset` **plus** CSV unduhan di `data/raw` | kode, `.env`, `.kaggle/`, `outputs/` | menguji unduhan Kaggle dari nol |

`make down` hanya mematikan container (volume tetap). Semua perintah memakai `-p olist-v2`, jadi proyek lain (T0–T6, v1) tidak tersentuh.

## Log task Airflow: pakai bind mount, bukan named volume
Log task disimpan di `olist-v2/airflow/logs/` (bind mount, di-ignore git). **Dengan named volume Docker, log task yang dijalankan scheduler hanya 8 baris** (tanpa keluaran kode dan tanpa penutup), padahal task yang sama dijalankan manual menghasilkan log lengkap. Dengan bind mount, log menjadi lengkap (18 baris pada task ringan). Terbukti dengan eksperimen A/B pada DAG yang sama; **mekanisme persisnya belum diketahui** (kemungkinan terkait cara Docker Desktop menangani append dari banyak proses ke named volume), jadi jangan kembali ke named volume.

Jalur debug yang tidak bergantung pada file log tetap tersedia: `make task-test T=<task_id>`, `make dag-test`, dan `make silver` / `make gate` / `make load`.

## Jebakan yang sudah terjadi di mesin ini
- Shell mengekspor `COMPOSE_PROJECT_NAME=olist`, `MINIO_*`, `POSTGRES_*`: itu **mengalahkan `.env` dan `name:` compose**. Akibatnya compose v2 pernah menggantikan container `olist-postgres`/`olist-minio` milik T0–T6 dan memakai volume mereka. Penangkalnya sudah ada: `make` memakai `-p olist-v2`, variabel berawalan `V2_`, volume/network bernama eksplisit (`olist-v2-*`). **Selalu jalankan lewat `make`.**
- `docker compose up --wait` menghentikan semuanya karena service sekali-jalan keluar dengan kode 0.

## Belum diverifikasi (ditulis, belum dijalankan)
Build ketiga image kustom, versi pin dbt (`dbt-core 1.8.9`, `dbt-postgres 1.8.2`), tag `apache/spark:3.5.3-scala2.12-java17-python3-ubuntu`, konektor S3A, dan driver Spark client-mode dari Airflow/Jupyter.
