# olist-v2 (eksplorasi)

Platform data Olist yang dirangkai dari Airflow, MinIO, Spark, dbt, Metabase, datacontract (ODCS), dan Kafka, dengan Postgres sebagai warehouse. Stack ini terpisah dari implementasi pertama (tahap T0–T6, project Docker `olist`), sehingga keduanya dapat berjalan berdampingan tanpa saling menimpa.

## Profil dan port
| Profil | Service | Port host |
|---|---|---|
| (inti) | Postgres `15432`, MinIO API `19000` dan web console `19001`, Airflow `18080` | |
| `spark` | spark-master (UI `18090`, RPC `17077`), spark-worker (UI `18091`) | |
| `stream` | Kafka `19092`, Kafka UI `18081` | |
| `bi` | Metabase `13000` (tema gelap: Settings → Appearance) | |
| `lab` | Jupyter `18888` (token di `.env`), notebook di `notebooks/` | |
| `contract` | datacontract editor `18082` | |

```bash
make init && make build      # sekali (build image airflow, spark, jupyter)
make up                      # inti;  make up P="spark stream";  make up-all
make ps                      # status (jangan pakai `up --wait`, lihat bagian Jebakan)
make reset                   # hapus hanya volume olist-v2-*
```

## Reset (tiga tingkat; semuanya meminta konfirmasi, lewati dengan `YES=1`)

| Perintah | Menghapus | Tetap ada | Untuk apa |
|---|---|---|---|
| `make reset-layers` | objek di bucket `bronze`/`silver`/`gold` (termasuk manifest dan riwayat versi) dan isi schema `raw`/`staging`/`dw`/`mart` | container, volume, riwayat run Airflow, `data/raw`, kode | mengulang pipeline dari awal dengan cepat, tanpa membangun ulang stack |
| `make reset` | semua volume olist-v2 (Postgres beserta database `airflow` dan `metabase`, MinIO, Kafka) dan isi `airflow/logs/`. Log disimpan di bind mount, bukan volume, sehingga harus dihapus terpisah. Riwayat run di database Airflow sudah hilang bersama volume, jadi log lama tidak lagi menunjuk ke run mana pun | CSV di `data/raw`, kode, `.env`, `outputs/` | mulai dari stack kosong; `make up` lagi sesudahnya |
| `make reset-all` | sama seperti `reset`, ditambah CSV unduhan di `data/raw` | kode, `.env`, `.kaggle/`, `outputs/` | menguji unduhan Kaggle dari nol |

`make down` hanya mematikan container dan volume tetap ada. Semua perintah memakai `-p olist-v2`, sehingga proyek lain (v1, T0–T6) tidak tersentuh.

## Log task Airflow: bind mount, bukan named volume
Log task disimpan di `olist-v2/airflow/logs/` (bind mount, di-ignore git). Dengan named volume Docker, log task yang dijalankan scheduler hanya 8 baris, tanpa keluaran kode dan tanpa penutup, padahal task yang sama menghasilkan log lengkap jika dijalankan manual. Dengan bind mount, log menjadi lengkap (18 baris pada task ringan). Perbedaan ini terlihat pada eksperimen A/B terhadap DAG yang sama. Mekanisme persisnya belum diketahui; dugaan sementara terkait cara Docker Desktop menangani penulisan bersamaan dari banyak proses ke named volume. Karena itu, jangan kembali ke named volume.

Jalur debug yang tidak bergantung pada file log tetap tersedia: `make task-test T=<task_id>`, `make dag-test`, dan `make silver` / `make gate` / `make load`.

## Jebakan yang sudah terjadi di mesin ini
- Shell di mesin ini mengekspor `COMPOSE_PROJECT_NAME=olist`, `MINIO_*`, dan `POSTGRES_*`. Variabel itu mengalahkan `.env` dan `name:` di compose, sehingga compose v2 pernah menggantikan container `olist-postgres` dan `olist-minio` milik T0–T6 lalu memakai volume mereka. Penangkalnya sudah ada: `make` memakai `-p olist-v2`, variabel berawalan `V2_`, serta volume dan network bernama eksplisit (`olist-v2-*`). Jalankan selalu lewat `make`.
- `docker compose up --wait` menghentikan semua service, karena service sekali-jalan keluar dengan kode 0.

## Alur sehari-hari (semua lewat `make`; `make help` untuk daftar)
```bash
make venv                          # sekali: venv lokal (dbt, pytest, kafka-python, dst.)
make all YES=1                     # dari nol: reset -> up -> download -> bronze -> silver -> gate -> load -> stream -> dbt-build -> marts -> dashboard
make smoke                         # pemeriksaan menyeluruh semua lapisan
make inject                        # failure injection T6 (6 kasus) -> outputs/failure_tests.md
make benchmark                     # Spark vs DuckDB -> outputs/benchmark_report.md
make report                        # EVALUATION_EVIDENCE.md dari outputs/*.json
make test                          # pytest tanpa Docker
```
Gejala dan cara pemulihan ada di [RUNBOOK.md](RUNBOOK.md). Rancangan dan langkah-langkahnya ada di [docs/README.md](docs/README.md).

## Belum diverifikasi / belum dikerjakan
- `make all YES=1` dari nol terukur 161,7 detik (batas tugas 15 menit), tetapi dengan image sudah terbangun dan CSV sudah ada. Build image dari nol dan unduhan Kaggle penuh belum diukur.
- Spark cluster (profil `spark`, image 3.5.3) belum dipakai dan versinya belum diselaraskan dengan pyspark klien 3.5.4. Silver masih berjalan di `local[2]`.
- Jupyter (profil `lab`) dan editor ODCS (profil `contract`) belum dijalankan.
- Stream belum menjadi task DAG (jalan lewat `make stream`).
- Iceberg, observability dan lineage, serta uji beban (k6) belum dikerjakan.
