# Langkah 10 — Benchmark silver: Spark vs DuckDB

**Status: selesai.** Pekerjaan yang sama dijalankan di dua mesin: baca CSV teks → cast tipe → aturan hard/soft dari kontrak ODCS → FK ke induk → tulis Parquet bersih + karantina, 9 tabel. Disk lokal (tanpa MinIO) di container scheduler (batas memori 3 GB); Spark `local[2]`, DuckDB 2 thread.

`make benchmark` (`SCALES=1:3,5:1` untuk skala lain) → `outputs/benchmark_report.md`.

| Skala | Input | DuckDB | Spark | Rasio | RSS puncak DuckDB / Spark |
|---|---:|---:|---:|---:|---|
| 1× | 126 MB | 2,1 s | 15,5 s | 7,3× | 493 / 1.353 MB |
| 5× | 631 MB | 8,0 s | 35,6 s | 4,5× | 1.837 / 1.664 MB |
| 10× | 1,26 GB | 19,1 s | 72,2 s | 3,8× | 2.412 / 1.659 MB |

**Paritas (1×):** jumlah baris sumber/karantina/bersih, verdict, dan 41 hitungan aturan **identik** antara dua mesin. Aturan hanyalah klausa SQL dari kontrak, dipakai Spark dan DuckDB.

## Bacaan
- Di mesin satu node ini dan sampai 1,26 GB, Spark tidak pernah lebih cepat. Selisih mengecil seiring data membesar (7,3× → 3,8×).
- Memori DuckDB tumbuh hampir linear (2,4 GB pada 10×, dekat batas 3 GB); Spark stabil ±1,65 GB. Titik Spark mulai layak ada di sekitar 10× atau lebih, terutama sebagai cluster; itu belum diukur.
- Skala > 1× mengulang isi CSV, jadi aturan kunci melanggar: hanya waktu dan memori yang dibandingkan di sana. Ulangan: 3× pada 1×, 1× pada skala lain.
- Ukuran memori Spark = JVM + Python dalam container, bukan cluster.

Kode: `olist_v2/bench.py`. Kasus bug yang ditemukan: berkas CSV memakai CRLF; menyambung salinan dengan LF membuat DuckDB gagal membaca dialeknya (Spark tidak peduli).
