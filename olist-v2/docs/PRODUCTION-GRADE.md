# Naik kelas: dari tugas ke production-grade

Ketentuan di [ALIGNMENT.md](ALIGNMENT.md) tetap sama. Dokumen ini berisi **apa yang ditambahkan** supaya stack-nya setara praktik produksi, diurutkan menurut nilai dibanding beban RAM. Semua angka RAM adalah **perkiraan kasar, belum diukur**; ukur dengan `docker stats` sebelum menambah komponen. Semua usulan ini **belum dibangun atau diuji**.

> **Keputusan:** jalur dasar dibuat **hijau dulu dengan Parquet biasa**; Iceberg (Tier 1 #1) baru masuk sesudahnya. Urutan lengkap ada di bagian paling bawah.

## Tier 1 — murah RAM, nilai tinggi (kerjakan lebih dulu)

| # | Peningkatan | Yang didapat dibanding versi tugas | Bukti selesai |
|---|---|---|---|
| 1 | **Table format Iceberg di MinIO** (silver dan gold), katalog **JDBC di Postgres yang sudah ada** (tanpa service baru) | transaksi ACID, evolusi skema, *time travel*, rollback snapshot; menggantikan "timpa file Parquet" | `INSERT` yang gagal di tengah tidak meninggalkan data setengah; `SELECT … VERSION AS OF` membaca versi lama |
| 2 | **dbt kelas produksi**: model contracts dengan `constraints` (PK/FK), `tests`, `exposures` (menautkan model ke dashboard Metabase), `source freshness`, `dbt docs` | skema dan integritas dipaksa saat build; lineage dan dokumentasi otomatis | `dbt build` hijau; PK/FK nyata di Postgres; halaman docs menampilkan lineage sampai exposure |
| 3 | **Airflow terdisiplin**: `retries` dengan backoff eksponensial, `execution_timeout`, `max_active_runs=1`, *pools*, `on_failure_callback` (alert), **backfill idempoten** lewat `data_interval`, assets/datasets | ketahanan operasional dan pemulihan tanpa campur tangan manual | DAG gagal di tengah dijalankan ulang tanpa duplikat; backfill satu bulan menghasilkan angka sama |
| 4 | **Kontrak ODCS di CI**: `datacontract lint` dan `datacontract test`, plus **`datacontract breaking`** antar versi | perubahan skema yang merusak tertangkap sebelum jalan | PR yang menghapus kolom gagal di CI |
| 5 | **CI GitHub Actions**: lint Python/SQL, unit test tanpa Docker, `dbt parse`, `docker compose config`, lint kontrak | tidak ada perubahan masuk tanpa pemeriksaan otomatis | pipeline hijau di PR |
| 6 | **Idempoten dan append-only dibuktikan otomatis**: failure injection (a–e) berjalan sebagai test, bukan manual | jaminan tetap terjaga saat stack berubah | `make inject` hijau, hasilnya dibangkitkan |

## Tier 2 — butuh RAM (pilih satu dulu, ukur)

| Peningkatan | Yang didapat | Beban perkiraan |
|---|---|---|
| **Lineage OpenLineage + Marquez** (integrasi Airflow, Spark, dbt) | jejak data lintas alat: dari CSV sampai kartu Metabase | +±1 GB (Marquez API, DB, UI) |
| **Observabilitas Prometheus + Grafana** (Airflow lewat statsd-exporter, `postgres-exporter`, exporter Kafka) | metrik, dashboard, dan alert untuk kesehatan platform | +±1 GB |
| **Schema Registry** (Avro) untuk event Kafka | kontrak skema event dan kompatibilitas versi | +±0,5–1 GB |
| **Trino** di atas Iceberg sebagai mesin query (Metabase membaca dari Trino) | memisahkan penyimpanan dan komputasi; melepas ketergantungan pada Postgres sebagai serving layer | +±2 GB |

## Tier 3 — di luar batas laptop ini
Kubernetes, CDC (Debezium), pengelola rahasia (Vault), multi-broker Kafka. Cukup dicatat sebagai langkah berikut, bukan dibangun.

## Catatan realistis
- **Data 126 MB tidak membutuhkan** Spark, Iceberg, atau Trino. Nilai eksplorasinya adalah **mempelajari mekanismenya dan mengukurnya**; untuk itu tiap komponen sebaiknya punya satu pengukuran (durasi, memori, atau jumlah baris) di laporan, bukan sekadar "terpasang".
- **Satu laptop, 7,8 GB:** jangan menyalakan semua profil sekaligus. Tentukan urutan: Tier 1 (1–6) dulu, lalu satu komponen Tier 2.
- Menambah Iceberg mengubah cara Spark dan dbt membaca data (dbt-postgres tidak membaca Iceberg langsung). Jika Iceberg dipilih, jalur silver/gold ada di Spark+Iceberg dan Postgres menerima `raw.*` untuk dbt; ini perlu dirancang di langkah 4 dan 5.

## Usulan urutan kerja
1. Fondasi (langkah 1–2) terverifikasi.
2. Bronze dengan manifest sha256 + md5 (langkah 3).
3. Silver di Spark, awalnya Parquet biasa; **ubah ke Iceberg** setelah jalur dasarnya hijau (langkah 4).
4. dbt dengan contracts dan tests (langkah 5), lalu Metabase (langkah 6).
5. Kafka dan stream (langkah 7), lalu DAG ujung ke ujung dengan disiplin Airflow (langkah 8).
6. Verifikasi, CI, dan failure injection (langkah 9); terakhir satu komponen Tier 2.
