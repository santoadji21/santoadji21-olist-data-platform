# RUNBOOK — Olist Data Platform

Ditulis untuk orang yang baru membuka repo ini, atau kamu sendiri jam 2 pagi. Urutan baca: **§3** kalau ada yang rusak, **§1** kalau mulai dari nol.
Semua perintah dijalankan dari root repo. `make <x>` hanyalah pembungkus `python pipeline.py <x>`.

## 1. Menyalakan dari nol

```bash
git clone <repo> && cd olist-data-platform
python -m venv .venv && .venv/bin/pip install -r requirements.txt   # sekali
python scripts/get_dataset.py        # 9 CSV → data/raw (Kaggle; cadangan: mirror GitHub). Lalu: make check
make reset                           # docker compose down -v  (lewati bila stack belum pernah jalan)
make all                             # init → up → check → profile → bronze → silver → load → stream → model → marts
make smoke                           # semua baris harus [ok]
```

`make all` mencatat durasi tiap tahap di `outputs/run_all.json` (tabelnya ada di `EVALUATION_EVIDENCE.md` §1). Pertama kali, `up` membangun image Jupyter (beberapa menit).
Dashboard: `http://localhost:8888` (token di `.env`, `JUPYTER_TOKEN`), buka `work/dashboard.ipynb`; PNG ada di `outputs/dashboard/`.

Satu tahap saja: `make bronze | silver | load | stream | model | marts`. Urutannya tetap; tiap tahap membaca hasil tahap sebelumnya.

## 2. Memeriksa tiap lapisan

| Lapisan | Perintah | Yang harus terlihat |
|---|---|---|
| semua | `make smoke` | `smoke: 11/11 lolos` |
| service | `docker compose ps` | 5 service `healthy` |
| bronze (MinIO tidak punya web console) | `make minio-ls` · `make minio-cat OBJ=bronze/olist/_manifest/orders.json` | objek di `bronze/olist/<tabel>/ingest_date=…/sha256=…/`, 9 manifest di `_manifest/` |
| silver | `cat outputs/dq_report.md` | semua tabel `PASS` (bukan `FAIL`); karantina di `silver/olist/_quarantine/` |
| raw | `docker exec olist-postgres psql -U olist -d olist -c "select count(*) from raw.orders"` | 99441 |
| stream | `make stream-report` · Kafka UI `http://localhost:8080` (topic `olist.order_events`, `…dlq`) | 4 baris `✅ cocok`, status 100 % cocok |
| dw | `.venv/bin/python -m pytest tests/test_dw.py -q` | semua lolos (28 pemeriksaan integritas = 0) |
| mart | `.venv/bin/python -m pytest tests/test_marts.py -q` | semua lolos (17 pemeriksaan = 0) |

## 3. Symptom → pemeriksaan termurah → perbaikan

| Symptom | Periksa dulu | Perbaikan |
|---|---|---|
| `NoBrokersAvailable`, producer/consumer menggantung | `docker compose ps kafka`; dari laptop alamat harus `localhost:29092` (`KAFKA_HOST_PORT`), dari container `kafka:9092` | `docker start olist-kafka` (atau `make up`); tunggu `healthy`. Replay yang terputus aman diulang: `make stream` |
| `password authentication failed` / koneksi ditolak ke Postgres | `.env` (`POSTGRES_*`, `POSTGRES_PORT`) vs `docker compose ps postgres`. Volume Postgres menyimpan password lama jika `.env` diubah setelah `up` pertama | kembalikan `.env`, atau `make reset && make all` (data turunan hilang, lihat §4) |
| Port bentrok saat `up` | `python pipeline.py init` mencetak tiap port `bebas` / `BENTROK` | ganti port di `.env`, atau matikan proses lain; jangan `down -v` hanya karena ini |
| `silver berhenti (Postgres tidak disentuh): … kolom hilang: <kolom>` | CSV di `data/raw` berubah bentuk (kolom hilang/berganti nama) | pulihkan CSV yang benar, `make check`, `make bronze` (versi baru), `make silver && make load`. `raw.*` tidak berubah selama silver berhenti |
| `silver berhenti: … isi objek bronze tidak cocok dengan sha256 di manifest` | objek di bucket rusak/berubah; `make minio-cat OBJ=…` membandingkan | unggah ulang objek asli ke kunci yang sama: `.venv/bin/python -c "import pipeline as p; from olist.bronze import make_s3; make_s3(p.read_env()).upload_file('data/raw/<file>.csv','bronze','<object_key dari manifest>')"`, lalu `make silver`. (`make bronze` tidak membantu: sha256 sumber sama dengan manifest, jadi `unchanged`) |
| `outputs/dq_report.md`: tabel `FAIL` (`karantina X% > ambang Y%`, `kunci … tidak unik`, `row_count`) | bagian "Alasan FAIL" di laporan; `silver/olist/_quarantine/<tabel>.parquet` kolom `_reasons` | perbaiki sumber, atau (dengan alasan tertulis) ubah ambang di `olist/contract.py`. `make load` melewati tabel `FAIL` dan keluar dengan kode ≠ 0 |
| `stream-report` ❌ (event unik / duplikat / DLQ / status tidak cocok) | apakah consumer sudah selesai? `docker exec olist-postgres psql -U olist -d olist -c "select count(*) from stream.order_events"` naik terus = belum selesai. Producer pernah dijalankan dengan `--seed`/`--limit` berbeda tanpa reset? | consumer berhenti terlalu cepat: `python pipeline.py stream consume --idle-exit 20`. Data campur dari run berbeda: `python pipeline.py stream reset` lalu `make stream` |
| Freshness p95 tinggi / lag tumbuh | `mart.stream_freshness_batch` atau grafik freshness: p50 naik selama replay | wajar saat producer tanpa batas kecepatan (consumer satu proses lebih lambat). Uji dengan `python pipeline.py stream run --speed 5000` |
| `model GAGAL, dw lama tidak berubah: pemeriksaan integritas gagal: <nama>=N` | nama pemeriksaan menunjuk tabel/FK (lihat `sql/checks/dw_integrity.sql`); `raw.*` basi? | jalankan `make load` lalu `make model`. `dw` lama utuh sampai build baru lolos |
| `make model`/`marts`/`pytest` **menggantung** | ada query lain memegang lock: `docker exec olist-postgres psql -U olist -d olist -c "select pid, state, wait_event_type, now()-query_start as lama, left(query,60) from pg_stat_activity where datname='olist'"` | batalkan yang lama: `select pg_terminate_backend(<pid>)`. Beri `set statement_timeout='60s'` pada query analisis manual |
| Notebook: `ModuleNotFoundError: olist` | `docker exec olist-jupyter ls /home/jovyan/olist` | `docker compose up -d jupyter` (compose me-mount `./olist` read-only) |
| `make marts`: grafik freshness kosong | `mart.stream_freshness_batch` kosong karena belum ada replay | `make stream` lalu `make marts` |

Kalau symptom tidak ada di tabel: baca error **pertama** (bukan terakhir) → `docker compose ps` → `docker compose logs --tail 50 <service>` → periksa `.env` → `make smoke`.

## 4. Reset

| Perintah | Efek | Yang hilang |
|---|---|---|
| `make down` | matikan container | tidak ada (volume tetap) |
| `make reset` (`down -v`) | matikan dan hapus volume | **semua data turunan**: objek bronze **beserta riwayat versinya**, silver, gold, seluruh `raw`/`stream`/`dw`/`mart`, topic Kafka dan offset consumer |
| `python pipeline.py stream reset` | hapus topic + consumer group, kosongkan `stream.*` dan `outputs/stream_truth.json` | hanya data stream (replay ulang dengan `make stream`) |
| `python pipeline.py model` / `marts` | bangun ulang `dw` / `mart` dalam satu transaksi | tidak ada (gagal = rollback, versi lama utuh) |

Yang **tidak** hilang pada reset: kode, `.env`, dan CSV di `data/raw` (di-`gitignore`, bukan di volume). Setelah `make reset` jalankan `make all` (≈ lihat durasi di `EVALUATION_EVIDENCE.md`) lalu `make smoke`.

Setelah `make inject` (failure injection) stack dipulihkan otomatis di akhir (replay penuh + marts + smoke); jika terhenti di tengah: `python pipeline.py bronze && make silver && make load && make stream && make marts && make smoke`.
