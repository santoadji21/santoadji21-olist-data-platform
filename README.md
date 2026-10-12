# Starter — Tugas Besar Olist Data Platform

Isi starter ini **sengaja sedikit**: stack yang sama dengan Local Big Data Lab, pemeriksa dataset, kerangka `pipeline.py`,
dan tiga template dokumen. Sisanya kamu bangun mengikuti `TUGAS_BESAR.md`.

```
docker-compose.yml   .env.example   jupyter/Dockerfile      # stack (postgres, kafka, kafka-ui, minio, jupyter) — project name "olist"
pipeline.py                                                 # init/up/down sudah jadi; tahap lain = TODO kamu
scripts/check_dataset.py                                    # cek 9 file CSV Kaggle di data/raw
sql/00_schemas.sql                                          # hanya CREATE SCHEMA; tabel kamu yang rancang
tests/conftest.py                                           # fixture koneksi Postgres untuk test (skip kalau stack mati)
docs/DATA_CONTRACT_TEMPLATE.md  RUNBOOK_TEMPLATE.md  EVALUATION_EVIDENCE_TEMPLATE.md
```

Langkah pertama:

```
pip install -r requirements.txt
python pipeline.py init
python pipeline.py up --build        # pertama kali: build image jupyter (±2–3 menit)
# unduh dataset Kaggle → data/raw/
python scripts/check_dataset.py
```

Lalu buka `TUGAS_BESAR.md` → Tahap 0.


---

## T0 — Kontrak data & profil

Acuan semua tahap: [DATA_CONTRACT.md](DATA_CONTRACT.md) (dibangkitkan), [contracts/olist_raw.odcs.yaml](contracts/olist_raw.odcs.yaml) (ODCS), [outputs/profile.md](outputs/profile.md).

```bash
python pipeline.py check          # 9 file ada, kolom & jumlah baris sesuai
python pipeline.py profile        # outputs/profile.md + profile.json
.venv/bin/python pipeline.py contract      # tulis ODCS + datacontract lint + test
python pipeline.py contract-doc   # DATA_CONTRACT.md
```

**Sumber dataset.** Kesembilan CSV diambil dengan `scripts/get_dataset.py`. Sumbernya Kaggle (`olistbr/brazilian-ecommerce`), dengan kredensial di `.kaggle/kaggle.json` (tidak di-commit); skrip itu punya cadangan mirror GitHub bila Kaggle gagal. `check` lulus (jumlah baris dan md5 sesuai), jadi angka cek berlaku.

### Pertanyaan README: temuan yang membuat Q1–Q4 salah secara diam-diam

1. **Dua id pelanggan (temuan #1, Q4).** `customer_id` dibuat per order, `customer_unique_id` per orang: 99.441 vs 96.096, selisih 3.345 (3,36%). Kalau `dim_customer` dan RFM memakai `customer_id`, tiap order tampak dari "pelanggan baru", jadi frekuensi selalu 1 dan pelanggan yang belanja ulang (2.997 orang) tidak terdeteksi. Tidak ada error, hanya angka belanja ulang yang terlihat jauh lebih kecil.
2. **Order tanpa item dan tanpa pembayaran (temuan #7, Q1).** 775 order tanpa item (603 `unavailable`, 164 `canceled`), dan 1 tanpa pembayaran. Kalau pendapatan dihitung dari `payment_value` tanpa menyaring status, order yang tidak pernah terkirim ikut dihitung. Kalau memakai inner join ke item, 775 order hilang dari `fact_order` dan jumlah order ikut meleset.
3. **`delivered` tanpa tanggal tiba (temuan #9, Q2).** 8 order `delivered` tidak punya `order_delivered_customer_date`, dan 23 order punya tanggal tiba lebih awal dari tanggal diserahkan ke kurir. Perbandingan `NULL > estimasi` bernilai false di SQL, jadi `is_late = false` dan late rate terlihat lebih bagus. Karena itu `is_late` dibuat `NULL` dan penyebut late rate hanya order dengan `is_late IS NOT NULL`.
4. **Selisih item vs pembayaran (temuan #10, Q1).** 378 order selisih > 0,01 BRL, 249 di antaranya > 1,00, total selisih absolut 3.271,95 BRL. Penyebabnya berbeda: 290 dibayar lebih (258 di antaranya cicilan kartu kredit > 1), 88 kurang, 10 memakai voucher. Memakai `payment_value` sebagai pendapatan memasukkan bunga cicilan, memakai item mengabaikan voucher. Angka yang diambil menentukan "pendapatan" Q1, jadi selisihnya diklasifikasikan di mart rekonsiliasi.
5. **Ulasan ganda (temuan #2, Q2).** 547 order punya lebih dari satu ulasan dan 814 baris `review_id` berulang. Join langsung order ke review menggandakan baris order, sehingga skor rata-rata dan jumlah order terlambat ikut tertimbang ganda.

Keputusan penanganannya ada di DATA_CONTRACT.md §2, termasuk tahap yang menerapkannya.


---

## T1 — Bronze: ingest ke MinIO

```bash
make up && make bronze        # atau: python pipeline.py bronze
```

Hasil: [outputs/bronze_report.md](outputs/bronze_report.md) (tabel per file) dan `outputs/bronze.json`. Kode: [olist/bronze.py](olist/bronze.py), test tanpa Docker: [tests/test_bronze.py](tests/test_bronze.py).

**Tata letak.** Bucket `bronze`:
- `olist/<tabel>/ingest_date=YYYY-MM-DD/sha256=<12 hex>/<file>`: isi CSV apa adanya. Kunci memuat sha256, jadi isi berbeda tidak pernah menimpa objek lama, termasuk di hari yang sama.
- `olist/_manifest/<tabel>.json`: manifest per tabel (JSON biasa, di bucket, bukan di laptop). Isinya sumber, `versions[]` (versi, sha256, ukuran byte, jumlah baris, waktu ingest, kunci objek) yang hanya bertambah, dan `latest` yang menunjuk objek terbaru.

**Idempotensi.** sha256 isi dibandingkan dengan versi terakhir di manifest. Sama → `unchanged`, tanpa upload. Beda → `new_version`. Ukuran byte tidak pernah dipakai sebagai pembanding. Manifest ditulis setelah objek, sehingga kalau proses mati di antaranya, run berikutnya hanya menulis manifest (objeknya sudah ada).

**Bukti (dijalankan di MinIO lokal).**
- Ingest pertama 9 file (±126 MB): 1,9 detik (batas tugas 2 menit). Semua `uploaded`.
- Ingest kedua tanpa perubahan: 9/9 `unchanged`, tidak ada upload.
- Satu byte diubah di `sellers` (huruf pertama `campinas` jadi `Campinas`): `new_version` v2, objek v1 tetap ada di bucket dan manifest. Setelah file dikembalikan, v3 menunjuk objek v1 yang sama tanpa upload ulang.
- Jumlah baris dihitung dengan parser CSV dan cocok dengan kontrak untuk semua tabel.

### Pertanyaan README: kenapa bronze harus append-only?

Karena bronze adalah satu-satunya salinan sumber apa adanya. Silver dan semua lapisan di atasnya dibangun ulang darinya. Kalau bronze bisa ditimpa, kesalahan di ingest atau di sumber ikut menghapus kebenaran dan tidak ada yang bisa diaudit.

Skenario Olist: file `olist_order_reviews_dataset.csv` dikirim ulang dengan satu kolom bergeser atau komentar yang terpotong, dan sistem hulu tidak memberi tahu. Karena versi lama masih ada di bucket dan manifest (sha256-nya berbeda), kita bisa membandingkan v1 dan v2, melihat tepatnya apa yang berubah, dan membangun ulang silver dari v1 sementara menunggu perbaikan dari hulu. Pada skenario yang lebih ringan, angka di dashboard berubah setelah file diperbarui, dan kita bisa menjawab "angka itu dari versi file yang mana".
