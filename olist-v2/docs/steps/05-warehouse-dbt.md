# Langkah 5 — Warehouse: dbt

> v2 tidak dinilai, jadi dbt **dipakai penuh** (aturan §9 tugas tidak mengikat). Ketentuan T4 tetap berlaku: **PK dan FK terpasang** (di dbt lewat *model contracts* dengan `constraints`, bukan bawaan), `outputs/dw_report.md` yang dibangkitkan, test database, dan 9 tabel dw dengan nama yang diminta. Lihat [ALIGNMENT.md](../ALIGNMENT.md) dan [PRODUCTION-GRADE.md](../PRODUCTION-GRADE.md).

**Status: rencana.** venv dbt sudah ada (`make venv`; `dbt-core 1.8.9`, `dbt-postgres 1.8.2`); folder `dbt/` masih kosong. Tujuan: `raw → staging → dw → mart` sebagai model dbt dengan test dan dokumentasi, menggantikan `sql/dw`, `sql/mart`, dan `sql/checks` di T0–T6.

## Yang perlu dibuat
- `dbt/dbt_project.yml` dan `dbt/profiles.yml`. **Koneksi dari `env_var` dengan default**, supaya satu berkas dipakai di laptop dan di Airflow:
  - laptop (IDE/plugin): host `localhost`, port `15432`, database `olist_dw`
  - dalam container Airflow: host `postgres`, port `5432` (env `DBT_PG_*` sudah diset di compose)
- `models/staging/` (view atas `raw.*`), `models/dw/` (dim dan fact), `models/mart/`.
- `sources` untuk `raw.*` dengan `freshness` bila relevan.

## Logika yang dibawa dari T4/T5 (sudah terbukti)
| Aturan | Sumber |
|---|---|
| `dim_customer` per **orang** (`customer_unique_id`), atribut terbaru | T4 |
| `is_late` NULL bila belum terkirim; dibandingkan per **tanggal** | T4 |
| `dim_geo` + baris `unknown` agar FK valid | T4 |
| bulan lengkap = order ≥ 10 % median | T4 |
| `fact_review` satu per order (answer terbaru) + `review_count` | T4 |
| pendapatan = Σ price, order bukan `canceled`/`unavailable`; F pada RFM pita tetap | T5 |

## Test dbt = pemeriksaan integritas
`unique`, `not_null`, dan `relationships` menggantikan `sql/checks/dw_integrity.sql`; test `singular` untuk angka cek (96.096 orang, 112.650 item, 99.441 order).

## Kriteria selesai
`dbt build` hijau; angka cek sama dengan T0–T6 (96.096 / 112.650 / 99.441, late rate 6,77 %); `dbt docs generate` menghasilkan lineage.

## Jebakan
Nama berkas model = nama tabel yang tampil di Metabase. Hindari awalan seperti `ex_`; pisahkan lewat schema, bukan lewat nama.
