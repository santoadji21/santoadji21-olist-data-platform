# Langkah 5 — Warehouse: dbt

> **Status: selesai dan terbukti.** dbt 1.8.9 + dbt-postgres 1.8.2: 9 view staging, 9 model dw, 8 mart, **65/65 lulus** (≈10 s, idempoten). 9 PK + 13 FK nyata di Postgres; jumlah baris dan late rate 6,77 % sama dengan T0–T6; `dbt_build` hijau di Airflow. Mart: lihat [langkah 6](06-dashboard-metabase.md).
>
> v2 tidak dinilai, jadi dbt **dipakai penuh** (aturan §9 tugas tidak mengikat). Ketentuan T4 tetap: **PK dan FK terpasang**, test database, dan 9 tabel dw dengan nama yang diminta. Lihat [ALIGNMENT.md](../ALIGNMENT.md) dan [PRODUCTION-GRADE.md](../PRODUCTION-GRADE.md).

## Struktur (`dbt/`)
- `dbt_project.yml`, `profiles.yml`: koneksi dari `env_var` dengan default, satu berkas untuk laptop (`localhost:15432`) dan container (`postgres:5432`; env `DBT_PG_*` di compose).
- `macros/generate_schema_name.sql`: model mendarat persis di schema `staging`/`dw`/`mart`.
- `models/staging/` (view atas `raw.*`, `_sources.yml` juga mendeklarasikan `stream.batch_metrics`), `models/dw/` (dim + fact), `models/mart/` (8 mart).
- PK dan FK dipasang lewat `post_hook` (`alter table … add primary key/foreign key`), dengan komentar `-- depends_on` agar urutan build benar. **Selalu `dbt build` seluruh proyek**: menjalankan satu dim saja menghapus FK pada fact (cascade).
- Uji: `unique`/`not_null`/`relationships` di `_dw.yml`, plus uji singular `dw_integrity` (28 pemeriksaan), `dw_check_numbers` (96.096 / 112.650 / 99.441 / 775 / 1 …), `mart_checks` (17).
- Menjalankan: `make dbt-build` (laptop) atau task `dbt_build` di DAG.

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

## Kriteria selesai (tercapai)
`dbt build` hijau; angka cek sama dengan T0–T6; `make dbt-docs` menghasilkan lineage.

## Jebakan
Nama berkas model = nama tabel yang tampil di Metabase. Hindari awalan seperti `ex_`; pisahkan lewat schema, bukan lewat nama.
