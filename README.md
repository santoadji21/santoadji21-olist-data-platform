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
