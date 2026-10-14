# Langkah 9 — Verifikasi dan bukti

**Status: rencana.** Pola dari T6: platform harus bisa **ditunjukkan** benar, bukan hanya diklaim.

## Yang perlu dibangun
| Bukti | Cara | Dari |
|---|---|---|
| Kesehatan semua lapisan | `make smoke`: service healthy, jumlah baris per lapisan, `dbt test`, `datacontract test`, DAG terakhir sukses | `olist/smoke.py` (T6) |
| Angka cek | 96.096 orang, 112.650 item, 99.441 order, late rate 6,77 %, rekonsiliasi 98,91 % | T4/T5 |
| Idempoten | jalankan seluruh DAG dua kali tanpa perubahan; semua jumlah baris dan jumlah uang identik | kasus (e) T6 |
| Gate tertangkap | hapus kolom di salinan CSV → `contract_gate` berhenti dengan pesan kolomnya | kasus (a) T6 |
| Integritas bronze | ubah satu byte objek bronze → terdeteksi lewat sha256 | kasus (f) T6 |
| Evidence | dokumen yang dibangkitkan dari file hasil, bukan diketik | `olist/evidence.py` (T6) |

## Pembanding yang menarik
- **Spark vs DuckDB** untuk silver pada tabel terbesar (geolokasi ±1 juta baris): durasi dan memori puncak.
- **dbt vs SQL mentah** (T0–T6): jumlah baris dan angka harus sama.

## Kriteria selesai
Satu perintah menghasilkan laporan yang menyatakan lulus/gagal per bukti di atas, dan hasilnya sama di mesin bersih (`make reset` lalu bangun ulang).
