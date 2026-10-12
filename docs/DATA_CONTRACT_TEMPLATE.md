# DATA_CONTRACT — Olist Data Platform (nama: …)

Sumber: Brazilian E-Commerce Public Dataset by Olist (Kaggle, versi …), diunduh …, `scripts/check_dataset.py` lulus pada ….
Semua stempel waktu: waktu lokal Brasil tanpa zona. Mata uang: BRL. Kode pos: **teks 5 digit** di semua tabel.

## 1. Tabel

Satu bagian per file. Contoh format:

### orders — `olist_orders_dataset.csv`
| Kolom | Tipe target | Null? | Catatan |
|---|---|---|---|
| order_id | text | tidak | kunci alami, unik (dibuktikan: …) |
| order_status | text | tidak | nilai: delivered, shipped, canceled, … |
| order_purchase_timestamp | timestamp | tidak | |
| … | | | |

Baris diharapkan: 99.441. Kunci alami: `order_id`.

### … (8 tabel lain)

## 2. Temuan "kotor" dan keputusannya

| # | Tabel | Temuan | Angka | Keputusan | Diterapkan di |
|---|---|---|---|---|---|
| 1 | customers | … | … | … | T4 `dim_customer` |
| 2 | … | | | | |

(minimal 10; tiap baris harus punya angka)

## 3. Aturan kualitas (gate silver)

| Tabel | Aturan | Jenis (hard/soft) | Tindakan | Ambang |
|---|---|---|---|---|
| … | | | | |

Ambang karantina per tabel: …% — alasan: …

## 4. Keputusan pemodelan yang mengikuti dari kontrak ini

- grain `dim_customer` = …
- kebijakan `fact_review` = …
- `is_late` = …
- bulan lengkap = …
