# Langkah 8 — Orkestrasi: DAG ujung ke ujung

**Status: ada dan terbukti per task.** Diagram: [olist-v2.workflow.html](../diagrams/olist-v2.workflow.html) (diagram ini masih berhenti di `load_raw`; belum menggambar `dbt_build` dan `marts_report`). Airflow hanya mengatur urutan; logika ada di Spark, dbt, dan datacontract, dan tiap tahap punya perintah mandiri (`make bronze`, …) yang jalan tanpa UI.

## Task dan urutan (DAG `olist_v2_pipeline`)
`download_kaggle → ingest_bronze → spark_silver → contract_gate → load_raw → dbt_build → marts_report`

| Task | Melakukan | Menulis ke |
|---|---|---|
| `download_kaggle` | Kaggle CLI → `data/raw` (idempoten) | `data/raw` |
| `ingest_bronze` | CSV → MinIO, sha256 + manifest | `bronze` |
| `spark_silver` | bronze → Parquet bertipe, karantina (Spark `local[2]` di scheduler) | `silver` |
| `contract_gate` | `datacontract test` terhadap silver; hanya `failed`/`error` menghentikan | DAG berhenti, **tabel FAIL tidak dimuat** |
| `load_raw` | silver → `raw.*` (TRUNCATE + COPY, satu transaksi) | Postgres `raw` |
| `dbt_build` | `dbt build` (model + 65 uji); membuat skema `stream.*` dulu | `staging`, `dw`, `mart` |
| `marts_report` | 17 pemeriksaan mart, ekspor gold, `marts_report.md` | `gold`, `outputs/` |

Plus DAG `olist_v2_healthcheck` (MinIO + warehouse).

## Pengaturan
- `retries=0` untuk task yang tidak boleh diulang otomatis (silver, gate, load, dbt, marts); `schedule=None` (manual).
- dbt dan datacontract lewat `$DBT_BIN` dan venv terpisah di image; target dan log dbt ke `/tmp`.
- **Log task di bind mount** `airflow/logs/` (named volume memotong log; lihat README).

## Yang belum
- Task stream, evidence, dan benchmark tidak ada di DAG (jalan lewat `make`).
- `marts_report` baru dibuktikan lewat `make marts`; jalankan DAG sekali lagi untuk melihatnya hijau di UI.
- Cosmos (`astronomer-cosmos`, satu task per model dbt) tidak dicoba; tetap satu task `dbt_build`.
