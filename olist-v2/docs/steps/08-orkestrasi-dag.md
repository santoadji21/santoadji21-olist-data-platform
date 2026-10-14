# Langkah 8 — Orkestrasi: DAG ujung ke ujung

**Status: rencana.** Diagram: [olist-v2.workflow.html](../diagrams/olist-v2.workflow.html). Airflow hanya mengatur urutan; logika ada di Spark, dbt, dan datacontract.

## Task dan urutan (DAG `olist_v2_pipeline`)
| Task | Melakukan | Menulis ke |
|---|---|---|
| `ingest_bronze` | CSV → MinIO, sha256 + manifest | `bronze` |
| `spark_silver` | bronze → Parquet bertipe, karantina | `silver` |
| `contract_gate` | `datacontract test` terhadap silver | gagal → DAG berhenti, **tabel FAIL tidak dimuat** |
| `load_raw` | silver → `raw.*` (TRUNCATE + COPY, satu transaksi) | Postgres `raw` |
| `dbt_build` | `dbt run` + `dbt test` | `dw`, `mart` |
| `publish_gold` | ekspor mart ke `gold` (Parquet/CSV), segarkan BI | `gold` |

## Pengaturan
- `retries` kecil (1–2) hanya untuk task yang aman diulang (semua idempoten); `contract_gate` **tidak** di-retry otomatis.
- `schedule=None` dulu (manual); jadwal harian setelah stabil.
- Perintah dbt dan datacontract lewat `$DBT_BIN` dan `$DATACONTRACT_BIN` (venv terpisah di image).

## Kriteria selesai
Satu run hijau dari nol; run kedua tanpa perubahan: bronze `unchanged`, hasil dw dan mart identik; gate yang dipaksa gagal (kolom dihapus di salinan CSV) menghentikan DAG sebelum `load_raw`.

## Keputusan terbuka
Cosmos (`astronomer-cosmos`) merender tiap model dbt sebagai task Airflow; menarik, tapi kompatibilitasnya dengan Airflow 2.10 perlu diuji. Mulai dengan satu task `dbt_build` biasa.
