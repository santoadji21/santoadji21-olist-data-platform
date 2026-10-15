# RUNBOOK — olist-v2

Format: **gejala → penyebab paling mungkin → cek → pulihkan**. Semua perintah dari folder `olist-v2/`. Daftar target: `make help`.

| # | Gejala | Lompat |
|---|---|---|
| 1 | `silver` berhenti: "kolom hilang" | [1](#1) |
| 2 | `silver` berhenti: "isi objek bronze tidak cocok dengan sha256" | [2](#2) |
| 3 | `contract_gate` merah | [3](#3) |
| 4 | `dbt_build` gagal koneksi (`Connection refused` port 15432) | [4](#4) |
| 5 | Jumlah event di `stream.order_events` ≠ event unik producer | [5](#5) |
| 6 | Producer selesai tanpa error tetapi event kurang | [6](#6) |
| 7 | Consumer mati di tengah jalan | [7](#7) |
| 8 | Log task Airflow terpotong | [8](#8) |
| 9 | Container saling bertabrakan dengan proyek T0–T6 | [9](#9) |
| 10 | Spark `spark_silver` mati (SIGKILL / tanpa S3A) | [10](#10) |
| 11 | Dashboard Metabase kosong atau kartu error | [11](#11) |

<a id="1"></a>
## 1. `silver` berhenti: "kolom hilang: …"
- **Penyebab:** CSV sumber kehilangan kolom yang ada di kontrak (`contracts/olist_raw.odcs.yaml`). Silver sengaja berhenti SEBELUM menulis silver atau menyentuh Postgres.
- **Cek:** `outputs/dq_report.md`; pesan menyebut nama kolom dan berkas. `make minio-ls` → versi bronze terbaru.
- **Pulihkan:** perbaiki/unduh ulang sumber (`make download`), `make bronze` (versi baru; versi rusak tetap di manifest), lalu `make silver gate load`.
- **Bukti:** failure test (a), `outputs/failure_tests.md`.

<a id="2"></a>
## 2. `silver` berhenti: "isi objek bronze tidak cocok dengan sha256 di manifest"
- **Penyebab:** objek di bucket bronze diubah tanpa lewat `make bronze` (manifest tidak ikut berubah).
- **Cek:** pesan menyebut `object_key`; bandingkan dengan `sha256` di manifest (`olist/_manifest/<tabel>.json` di bucket bronze).
- **Pulihkan:** unggah ulang CSV asli lewat `make bronze` (menghasilkan versi baru) atau kembalikan objek dari riwayat versi.
- **Bukti:** failure test (f).

<a id="3"></a>
## 3. `contract_gate` merah
- **Penyebab:** pemeriksaan ODCS berstatus `failed`/`error` (bukan peringatan soft atau "tipe belum didukung").
- **Cek:** `outputs/contract_gate.md` (alasan per pemeriksaan); log task menampilkan alasan gagalnya. Pesan `'s3' backend is not installed` → image Airflow lama: `make build-core && make up`.
- **Pulihkan:** perbaiki data atau aturan, ulangi `make silver gate`. Peringatan soft dan 52 pemeriksaan tipe pada Parquet BUKAN kegagalan (keterbatasan datacontract-cli).

<a id="4"></a>
## 4. `dbt_build` gagal: `Connection refused` ke `postgres:15432`
- **Penyebab:** container dibuat dari versi compose lama tanpa `DBT_PG_PORT`/`DBT_PG_DBNAME`, sehingga `dbt/profiles.yml` memakai bawaan laptop (15432), padahal di jaringan Docker Postgres ada di 5432.
- **Cek:** `docker compose -p olist-v2 exec airflow-scheduler env | grep DBT_PG`.
- **Pulihkan:** `make up` (container dibuat ulang dengan env terbaru), lalu jalankan ulang DAG. Dari laptop memakai `make dbt-build` (port 15432 sudah benar).

<a id="5"></a>
## 5. Event unik di Postgres ≠ event unik producer
- **Cek:** `make stream-report` (✅/❌ per besaran). Penyebab umum: run lama masih tersisa (tabel/topik tidak direset), atau producer dijalankan dengan seed/limit berbeda.
- **Pulihkan:** `make stream-reset` lalu `make stream`. Duplikat bukan penyebab: `event_id` = PK, duplikat diabaikan (failure test (b)).

<a id="6"></a>
## 6. Producer sukses tetapi event kurang
- **Penyebab:** broker mati lebih lama dari `request_timeout_ms`, dan producer tidak memeriksa hasil `send()`. Producer v2 memeriksa (errback + `flush` + `SendFailure`), jadi run seperti ini berakhir exit ≠ 0 dan `send_failures` > 0 di `outputs/stream_truth.json`.
- **Cek:** `make kafka-topics`, `make ps` (kafka healthy?), `outputs/stream_truth.json`.
- **Pulihkan:** nyalakan broker (`make up P=stream`), jalankan producer lagi (idempoten), lalu consumer. Tanpa reset.
- **Bukti:** failure test (d): producer naif kehilangan ribuan event tanpa error, producer yang memeriksa tidak.

<a id="7"></a>
## 7. Consumer mati di tengah jalan
- **Penyebab:** proses mati (OOM, `kill`, crash). Urutan commit Postgres → DLQ → offset menjamin tidak ada kehilangan; paling buruk beberapa pesan diproses dua kali.
- **Pulihkan:** jalankan consumer lagi tanpa reset (`docker compose -p olist-v2 exec -T airflow-scheduler python -m olist_v2.stream_cli consume`); hasil akhir sama (failure test (c)). Publikasi ke topic DLQ bersifat at-least-once: bisa dobel setelah crash, baris DLQ di Postgres tidak.

<a id="8"></a>
## 8. Log task Airflow terpotong (hanya beberapa baris)
- **Penyebab:** `/opt/airflow/logs` sebagai named volume memotong log. Compose memakai bind mount `./airflow/logs`.
- **Cek:** log lengkap ada di `airflow/logs/dag_id=…/run_id=…/task_id=…/attempt=N.log`. Cepat tanpa UI: `make task-test T=<task>` atau `make dag-test`.
- **Pulihkan:** pastikan `docker-compose.yml` masih memakai `./airflow/logs:/opt/airflow/logs`, lalu `make up`.

<a id="9"></a>
## 9. Tabrakan dengan proyek T0–T6 (container/volume `olist-*`)
- **Penyebab:** variabel shell `COMPOSE_PROJECT_NAME=olist`, `MINIO_*`, `POSTGRES_*` mengalahkan `.env`/`name:` compose.
- **Cek:** `env | grep -E 'COMPOSE|MINIO_|POSTGRES_'`.
- **Pulihkan:** selalu lewat `make` (memakai `-p olist-v2` dan variabel `V2_*`). Jangan menjalankan `docker compose` tanpa `-p olist-v2`.

<a id="10"></a>
## 10. `spark_silver` mati (SIGKILL / "No FileSystem for scheme s3a")
- **Penyebab:** image Airflow lama (tanpa jar S3A) atau memori scheduler kurang (Spark driver jalan di scheduler).
- **Cek:** `docker stats --no-stream | grep scheduler` (batas 3 GB), `docker compose -p olist-v2 exec airflow-scheduler ls /home/airflow/.local/lib/python3.11/site-packages/pyspark/jars | grep -E 'hadoop-aws|aws-java'`.
- **Pulihkan:** `make build-core && make up`; beri Docker Desktop RAM cukup (≥ 7,8 GB).

<a id="11"></a>
## 11. Metabase kosong atau kartu error
- **Penyebab:** mart belum ada (urutan: `make dbt-build` dulu), atau Metabase baru dibuat dari volume kosong.
- **Pulihkan:** `make up P=bi`, `make dbt-build`, lalu `make dashboard` (idempoten: kartu diperbarui, bukan digandakan). Metabase memakai ±1 GB; naikkan batas di compose bila sering restart.

## Pemulihan umum
- Ringan: `make reset-layers` (kosongkan bucket + schema, stack hidup).
- Sedang: `make reset` (hapus volume olist-v2; CSV tetap), lalu `make all`.
- Penuh: `make reset-all` (juga hapus CSV; unduh ulang dari Kaggle).
- Setelah gangguan: `make restore` (replay stream penuh + dbt + marts + smoke), lalu `make smoke`.
