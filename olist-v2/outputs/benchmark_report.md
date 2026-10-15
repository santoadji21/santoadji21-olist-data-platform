# Benchmark silver — Spark vs DuckDB

Pekerjaan sama: baca CSV teks → cast tipe → aturan hard/soft dari kontrak ODCS → FK ke induk → tulis Parquet bersih + karantina, 9 tabel. Disk lokal (tanpa MinIO), di container scheduler (8 CPU Docker, batas memori 3 GB); Spark `local[2]`, DuckDB 2 thread. Skala N = isi tiap CSV diulang N kali.

| Skala | Input (MB) | Mesin | Waktu (s), median | Rentang | Start sesi (s) | Puncak RSS (MB) | Output (MB) | Status |
|---|---:|---|---:|---|---:|---:|---:|---|
| 1× | 126.2 | duckdb 1.5.5 threads=2 | 2.1 | 2.0–2.2 (n=3) | 0.0 | 493 | 55.6 | ok |
| 1× | 126.2 | spark 3.5.4 local[2] | 15.5 | 15.0–16.0 (n=3) | 1.8 | 1,353 | 56.9 | ok |
| 5× | 630.9 | duckdb 1.5.5 threads=2 | 8.0 | 8.0–8.0 (n=1) | 0.0 | 1,837 | 277.5 | ok |
| 5× | 630.9 | spark 3.5.4 local[2] | 35.6 | 35.6–35.6 (n=1) | 1.8 | 1,664 | 208.0 | ok |
| 10× | 1,261.9 | duckdb 1.5.5 threads=2 | 19.1 | 19.1–19.1 (n=1) | 0.0 | 2,412 | 554.6 | ok |
| 10× | 1,261.9 | spark 3.5.4 local[2] | 72.2 | 72.2–72.2 (n=1) | 2.6 | 1,659 | 319.7 | ok |

## Paritas hasil (skala 1×)

**Identik**: 9 tabel, 41 hitungan aturan, jumlah baris sumber/karantina/bersih, dan verdict dibandingkan antara Spark dan DuckDB.

## Kesimpulan (dihitung dari tabel di atas)

- 1×: DuckDB 2.1 s vs Spark 15.5 s (+ start sesi 1.8 s) → DuckDB 7.3× lebih cepat; memori puncak DuckDB 493 MB vs Spark 1,353 MB.
- 5×: DuckDB 8.0 s vs Spark 35.6 s (+ start sesi 1.8 s) → DuckDB 4.5× lebih cepat; memori puncak DuckDB 1,837 MB vs Spark 1,664 MB.
- 10×: DuckDB 19.1 s vs Spark 72.2 s (+ start sesi 2.6 s) → DuckDB 3.8× lebih cepat; memori puncak DuckDB 2,412 MB vs Spark 1,659 MB.
