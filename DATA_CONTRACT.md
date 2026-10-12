# DATA_CONTRACT — Olist Data Platform (`olist-data-platform`)

Sumber: Brazilian E-Commerce Public Dataset by Olist (Kaggle; mirror GitHub), 9 file CSV. `scripts/check_dataset.py` lulus (9/9 sesuai). Dokumen dibangkitkan 2026-10-12 oleh `python pipeline.py contract-doc` dari `olist/schema.py`, `outputs/profile.json`, dan aturan di `olist/contract.py`.

Semua stempel waktu: waktu lokal Brasil tanpa zona. Mata uang: BRL. Kode pos: **teks 5 digit** di semua tabel.

**Kontrak mesin-terbaca:** [`contracts/olist_raw.odcs.yaml`](contracts/olist_raw.odcs.yaml) — [Open Data Contract Standard 3.x](https://datacontract.com), dites dengan [Data Contract CLI](https://cli.datacontract.com) (`python pipeline.py contract`). Dokumen ini dan YAML berasal dari sumber yang sama sehingga tidak bisa berbeda.

## 1. Tabel

### customers — `olist_customers_dataset.csv`

Satu baris per ORDER (bukan per orang). customer_id bersifat per-order; customer_unique_id adalah identitas orang yang sebenarnya.

| Kolom | Tipe target | Null? | Catatan |
|---|---|---|---|
| customer_id | text | tidak | ID pelanggan per order (join ke orders.customer_id); **kunci alami**, unik (dibuktikan: 0 duplikat) |
| customer_unique_id | text | tidak | ID orang sebenarnya; satu orang bisa punya banyak customer_id |
| customer_zip_code_prefix | char(5) | tidak | Prefix kode pos; wajib teks 5 digit, lpad '0' |
| customer_city | text | tidak | Kota |
| customer_state | char(2) | tidak | Kode negara bagian (UF) |

Baris diharapkan: 99.441. Kunci alami: `customer_id`.

### geolocation — `olist_geolocation_dataset.csv`

Koordinat per zip prefix. BANYAK baris per prefix (bukan kunci), banyak duplikat persis, dan ada titik di luar Brasil.

| Kolom | Tipe target | Null? | Catatan |
|---|---|---|---|
| geolocation_zip_code_prefix | char(5) | tidak | Prefix kode pos; wajib teks 5 digit, lpad '0' |
| geolocation_lat | double precision | tidak | Lintang; Brasil ≈ −34 … +5 |
| geolocation_lng | double precision | tidak | Bujur; Brasil ≈ −74 … −35 |
| geolocation_city | text | tidak | Kota (ejaan tidak seragam) |
| geolocation_state | char(2) | tidak | Kode negara bagian (UF) |

Baris diharapkan: 1.000.163. Kunci alami: **tidak ada** (tidak ada kombinasi kolom yang unik).

### order_items — `olist_order_items_dataset.csv`

Satu baris per unit barang dalam order.

| Kolom | Tipe target | Null? | Catatan |
|---|---|---|---|
| order_id | text | tidak | FK → orders.order_id; **kunci alami** (bagian dari kunci komposit) |
| order_item_id | integer | tidak | Nomor urut item dalam order (mulai 1); **kunci alami** (bagian dari kunci komposit) |
| product_id | text | tidak | FK → products.product_id |
| seller_id | text | tidak | FK → sellers.seller_id |
| shipping_limit_date | timestamp | tidak | Batas seller menyerahkan ke kurir (waktu lokal Brasil) |
| price | numeric(10,2) | tidak | Harga barang (BRL); ≥ 0 |
| freight_value | numeric(10,2) | tidak | Ongkir bagian item ini (BRL); ≥ 0 |

Baris diharapkan: 112.650. Kunci alami: `order_id`, `order_item_id`.

### order_payments — `olist_order_payments_dataset.csv`

Pembayaran; satu order bisa dibayar dengan beberapa metode/cicilan.

| Kolom | Tipe target | Null? | Catatan |
|---|---|---|---|
| order_id | text | tidak | FK → orders.order_id; **kunci alami** (bagian dari kunci komposit) |
| payment_sequential | integer | tidak | Urutan pembayaran dalam order (mulai 1); **kunci alami** (bagian dari kunci komposit) |
| payment_type | text | tidak | credit_card | boleto | voucher | debit_card | not_defined |
| payment_installments | integer | tidak | Jumlah cicilan; 0 muncul di sumber |
| payment_value | numeric(10,2) | tidak | Nilai pembayaran (BRL); ≥ 0; ada yang 0 |

Baris diharapkan: 103.886. Kunci alami: `order_id`, `payment_sequential`.

### order_reviews — `olist_order_reviews_dataset.csv`

Ulasan. review_id TIDAK unik, order_id TIDAK unik; komentar multi-baris (mengandung newline di dalam kutip).

| Kolom | Tipe target | Null? | Catatan |
|---|---|---|---|
| review_id | text | tidak | ID ulasan (tidak unik) |
| order_id | text | tidak | FK → orders.order_id (satu order bisa punya > 1 ulasan) |
| review_score | smallint | tidak | Skor 1–5; CHECK 1..5 |
| review_comment_title | text | ya | Judul komentar (kebanyakan kosong) |
| review_comment_message | text | ya | Isi komentar (kebanyakan kosong) |
| review_creation_date | timestamp | tidak | Waktu survei dikirim |
| review_answer_timestamp | timestamp | tidak | Waktu survei dijawab |

Baris diharapkan: 99.224. Kunci alami: **tidak ada** (tidak ada kombinasi kolom yang unik).

### orders — `olist_orders_dataset.csv`

Header order: status + stempel waktu. Tabel pusat; tiap order punya tepat satu baris.

| Kolom | Tipe target | Null? | Catatan |
|---|---|---|---|
| order_id | text | tidak | Kunci alami; **kunci alami**, unik (dibuktikan: 0 duplikat) |
| customer_id | text | tidak | FK → customers.customer_id (per order) |
| order_status | text | tidak | delivered | shipped | canceled | unavailable | invoiced | processing | created | approved |
| order_purchase_timestamp | timestamp | tidak | Waktu order dibuat |
| order_approved_at | timestamp | ya | Waktu pembayaran disetujui |
| order_delivered_carrier_date | timestamp | ya | Waktu diserahkan ke kurir |
| order_delivered_customer_date | timestamp | ya | Waktu tiba di pelanggan |
| order_estimated_delivery_date | timestamp | tidak | Estimasi tiba (presisi tanggal; jam selalu 00:00:00) |

Baris diharapkan: 99.441. Kunci alami: `order_id`.

### products — `olist_products_dataset.csv`

Katalog produk. Nama kolom sumber punya typo 'lenght'.

| Kolom | Tipe target | Null? | Catatan |
|---|---|---|---|
| product_id | text | tidak | Kunci alami; **kunci alami**, unik (dibuktikan: 0 duplikat) |
| product_category_name | text | ya | Kategori (Portugis) |
| product_name_lenght | integer | ya | Panjang nama; typo sumber; dw: product_name_length |
| product_description_lenght | integer | ya | Panjang deskripsi; typo sumber; dw: product_description_length |
| product_photos_qty | integer | ya | Jumlah foto |
| product_weight_g | integer | ya | Berat (gram) |
| product_length_cm | integer | ya | Panjang (cm) |
| product_height_cm | integer | ya | Tinggi (cm) |
| product_width_cm | integer | ya | Lebar (cm) |

Baris diharapkan: 32.951. Kunci alami: `product_id`.

### sellers — `olist_sellers_dataset.csv`

Seller + lokasi.

| Kolom | Tipe target | Null? | Catatan |
|---|---|---|---|
| seller_id | text | tidak | Kunci alami; **kunci alami**, unik (dibuktikan: 0 duplikat) |
| seller_zip_code_prefix | char(5) | tidak | Prefix kode pos; wajib teks 5 digit, lpad '0' |
| seller_city | text | tidak | Kota |
| seller_state | char(2) | tidak | Kode negara bagian (UF) |

Baris diharapkan: 3.095. Kunci alami: `seller_id`.

### product_category_translation — `product_category_name_translation.csv`

Terjemahan kategori Portugis → Inggris. File diawali BOM UTF-8 (EF BB BF).

| Kolom | Tipe target | Null? | Catatan |
|---|---|---|---|
| product_category_name | text | tidak | Kategori (Portugis); baca dengan utf-8-sig; **kunci alami**, unik (dibuktikan: 0 duplikat) |
| product_category_name_english | text | tidak | Kategori (Inggris) |

Baris diharapkan: 71. Kunci alami: `product_category_name`. Encoding: `utf-8-sig`.

## 2. Temuan "kotor" dan keputusannya

Angka dihitung kode (`python pipeline.py profile` → `outputs/profile.md`); tidak ada yang dibersihkan diam-diam.

| # | Tabel | Temuan | Angka | Keputusan | Diterapkan di |
|---|---|---|---|---|---|
| 1 | customers | Dua id pelanggan: `customer_id` per-ORDER, `customer_unique_id` = ORANG | customer_id unik=99,441; customer_unique_id unik=96,096; selisih=3,345 (3.36% jika salah pilih); orang belanja >1 kali=2,997 | Grain `dim_customer` = `customer_unique_id`; join orders→customers tetap lewat `customer_id`; atribut yang berubah antar order (zip/kota) ambil yang terbaru (SCD tipe 1), hilangnya riwayat dicatat | T4 `dim_customer` |
| 2 | order_reviews | `review_id` tidak unik dan satu `order_id` bisa punya >1 ulasan | baris dengan review_id berulang=814; review_id unik=98,410; order dengan >1 ulasan=547; order unik=98,673; pasangan (review_id, order_id) berulang=0 | `fact_review` satu baris per order: pilih ulasan dengan `review_answer_timestamp` terbaru (seri → `review_creation_date` terbaru → `review_id`); simpan `review_count` asli | T4 `fact_review` |
| 3 | customers, sellers, geolocation | Kode pos prefix berawalan `0` (`'01003'`) — kehilangan nol di depan bila dibaca sebagai angka | baris berawalan '0' (akan jadi 4 digit jika di-cast ke integer): customers=23,995 dari 99,441 (24.13%); sellers=1,027 dari 3,095 (33.18%); geolocation=245,733 dari 1,000,163 (24.57%); panjang string di file sumber=[5] | Baca sebagai teks (`dtype=str`); tipe target `char(5)`; `lpad(…, 5, '0')` sebagai pengaman di silver; join zip hanya antar kolom bertipe teks 5 digit | T2 silver (typing), T4 `dim_geo` |
| 4 | geolocation | Satu zip prefix punya banyak baris + duplikat persis (bukan kunci) | baris=1,000,163; prefix unik=19,015; maks baris/prefix=1,146; duplikat persis=261,831 (26.18%) | Agregasi ke satu baris per prefix: `avg(lat/lng)`, `mode(city/state)`; baris mentah dipertahankan di bronze/silver | T4 `dim_geo` |
| 5 | geolocation | Koordinat di luar Brasil (lat −34…+5, lng −74…−35) | titik di luar Brasil=11,687 (1.17%); lat [-36.61, 45.07]; lng [-101.47, 121.11] | Soft rule: baris tetap di silver dengan flag `is_outside_brazil` (tidak menggagalkan batch, tidak dibuang); dikecualikan dari agregasi `dim_geo` | T2 gate kualitas, T4 `dim_geo` |
| 6 | product_category_translation | File diawali BOM UTF-8 → nama kolom pertama tak terlihat berbeda, join kategori diam-diam gagal | 3 byte pertama=efbbbf; repr kolom pertama (utf-8)='\ufeffproduct_category_name'; repr dengan utf-8-sig='product_category_name' | Baca file ini dengan `encoding='utf-8-sig'`; kontrak mencatat encoding per file | T1 bronze (checksum apa adanya), T2 silver (decode) |
| 7 | orders | Order tanpa item dan order tanpa pembayaran | order tanpa item=775 (status: {'unavailable': 603, 'canceled': 164, 'created': 5, 'invoiced': 2, 'shipped': 1}); order tanpa pembayaran=1 | Tetap masuk `fact_order` (99.441 baris) dengan flag `has_items` / `has_payment`; tidak dibuang | T4 `fact_order` |
| 8 | order_payments | Pembayaran bernilai 0, `payment_type='not_defined'`, dan cicilan 0 | payment_value=0: 9; payment_type not_defined: 3; payment_installments=0: 2 | Biarkan & catat di silver; flag `is_zero_payment` di `fact_payment`; tidak dihitung sebagai pendapatan terbayar | T2 soft rule, T4 `fact_payment` |
| 9 | orders | Status `delivered` tanpa tanggal terkirim; tanggal terkirim lebih awal dari tanggal diserahkan ke kurir | delivered tanpa order_delivered_customer_date=8; delivered tanpa order_delivered_carrier_date=2; delivered_customer < delivered_carrier=23; approved_at < purchase=0 | Soft rule (simpan + flag `has_time_anomaly`, tidak dibuang; tanggal kosong maupun urutan terbalik); `is_late` = NULL (bukan false) bila tanggal terkirim kosong | T2 gate kualitas, T4 `is_late` |
| 10 | order_items vs order_payments | Σ(price+freight) item ≠ Σ(payment_value) — penyebabnya lebih dari satu | order dgn item & bayar=98,665; selisih >0,01=378; selisih >1,00=249; dari yang >0,01: dibayar LEBIH dari nilai item=290 (kartu kredit cicilan >1 di 258 di antaranya), dibayar KURANG=88, memakai voucher=10; total selisih absolut=3,271.95 BRL | Jangan dipaksa sama: mart pendapatan menampilkan dua angka (nilai pesanan vs terbayar) + tabel rekonsiliasi yang menjelaskan selisih per sebab | T5 mart rekonsiliasi |
| 11 | orders | Bulan pertama dan terakhir dataset hanya berisi segelintir order | rentang purchase=2016-09-04 21:15:19 … 2018-10-17 17:30:18; bulan pertama 2016-09=4; kedua 2016-10=324; bulan terakhir 2018-10=4; median per bulan=4,285; bulan <5% median={'2016-09': 4, '2016-12': 1, '2018-09': 16, '2018-10': 4} | Jangan dibuang; `dim_date.is_complete_month` = false untuk bulan tak lengkap agar mart/dashboard bisa memilih | T4 `dim_date`, T5 dashboard |
| 12 | products | Produk tanpa kategori, kategori tanpa terjemahan, dan typo nama kolom (`lenght`) | kategori null=610; produk dgn kategori tak terjemahkan=13 (kategori: ['pc_gamer', 'portateis_cozinha_e_preparadores_de_alimentos']); dimensi null=2; kolom typo: product_name_lenght, product_description_lenght | Kategori Inggris dengan fallback (`'unknown'` untuk null, nama Portugis untuk tanpa terjemahan); typo diperbaiki hanya di dw (`*_length`), silver tetap nama sumber | T4 `dim_product` |
| 13 | semua | Integritas referensial antar tabel (yatim/orphan) | order_items.order_id→orders=0; order_items.product_id→products=0; order_items.seller_id→sellers=0; order_payments.order_id→orders=0; order_reviews.order_id→orders=0; orders.customer_id→customers=0 | Semua FK sumber dinyatakan HARD di silver; bila >0 baris yatim → karantina, bukan dihapus diam-diam | T2 gate kualitas |
| 14 | customers, sellers → geolocation | Kode pos pelanggan/seller yang tidak ada di tabel geolocation | customers=278 baris; sellers=7 baris (setelah lpad 5 digit) | Jangan hapus FK: sediakan baris `unknown` di `dim_geo`; FK ke `dim_geo` tetap valid 100% | T4 `dim_geo` |
| 15 | order_reviews | Skor di luar 1–5 dan jawaban lebih awal dari pembuatan survei; komentar kosong dominan | skor di luar 1–5=0; answer<creation=0; judul kosong=88.34%; pesan kosong=58.70% | `review_score` CHECK 1..5 (hard); urutan waktu = soft; komentar kosong ≠ error (NULL, bukan '') | T2 gate kualitas |
| 16 | order_items, products | Harga ≤ 0, ongkir 0, dan berat produk 0 | price<=0=0; freight_value=0=383; freight_value<0=0; product_weight_g=0=4 | price/freight ≥ 0 sebagai hard rule; ongkir 0 sah (gratis ongkir); berat 0 → NULL di `dim_product` | T2 gate kualitas, T4 `dim_product` |
| 17 | orders | Status canceled/unavailable tetapi punya tanggal tiba di pelanggan | canceled/unavailable dengan delivered_customer_date=6; sebaran status={'delivered': 96478, 'shipped': 1107, 'canceled': 625, 'unavailable': 609, 'invoiced': 314, 'processing': 301, 'created': 5, 'approved': 2} | Biarkan & catat; `is_late` hanya dihitung untuk status `delivered` | T4 `is_late` |
| 18 | sellers, geolocation | Nama kota tidak seragam: satu zip prefix punya beberapa ejaan, karakter non-ASCII/simbol | seller_city berisi karakter non-huruf=24 dari 3,095; prefix geolocation dengan >1 ejaan kota=8,556 dari 19,015; baris geolocation_city non-ASCII=73,442 dari 1,000,163 | Kota bukan kunci join (kunci geografis = zip prefix). Di `dim_geo` kota = modus per prefix (trim + lower); ejaan lain dibiarkan di bronze/silver | T4 `dim_geo` |

Total: 18 temuan (minimal yang diminta: 10).

## 3. Aturan kualitas (gate silver)

`hard` = `severity: error` di YAML: pelanggaran → karantina/batch berhenti. `soft` = `severity: warning`: dicatat, baris dipertahankan + diberi flag.

| Tabel | Aturan | Jenis | Tindakan | Ambang karantina |
|---|---|---|---|---|
| customers | jumlah baris = 99.441 | hard | batch berhenti | — |
| customers | tipe & NOT NULL sesuai §1 (zip char(5) dibaca sebagai teks) | hard | baris → karantina | 0,0 % |
| customers | kunci `customer_id` unik | hard | batch berhenti | 0 % |
| customers | `state_len2`: customer_state harus 2 huruf (UF) | hard | baris → karantina | 0,0 % |
| geolocation | jumlah baris = 1.000.163 | hard | batch berhenti | — |
| geolocation | tipe & NOT NULL sesuai §1 (zip char(5) dibaca sebagai teks) | hard | baris → karantina | 2,0 % |
| geolocation | `lat_in_brazil`: lat dalam kotak Brasil (-34.0, 5.0) (temuan #5: titik di luar = soft) | soft | simpan + flag + catat di dq_report | tidak berlaku |
| geolocation | `lng_in_brazil`: lng dalam kotak Brasil (-74.0, -35.0) (temuan #5: titik di luar = soft) | soft | simpan + flag + catat di dq_report | tidak berlaku |
| order_items | jumlah baris = 112.650 | hard | batch berhenti | — |
| order_items | tipe & NOT NULL sesuai §1 | hard | baris → karantina | 0,0 % |
| order_items | kunci `order_id, order_item_id` unik | hard | batch berhenti | 0 % |
| order_items | `price_positive`: price > 0 | hard | baris → karantina | 0,0 % |
| order_items | `freight_nonneg`: freight_value >= 0 | hard | baris → karantina | 0,0 % |
| order_payments | jumlah baris = 103.886 | hard | batch berhenti | — |
| order_payments | tipe & NOT NULL sesuai §1 | hard | baris → karantina | 0,1 % |
| order_payments | kunci `order_id, payment_sequential` unik | hard | batch berhenti | 0 % |
| order_payments | `value_positive`: payment_value > 0 (temuan #8: 9 baris bernilai 0 = soft) | soft | simpan + flag + catat di dq_report | tidak berlaku |
| order_payments | `type_known`: payment_type bukan 'not_defined' (temuan #8: 3 baris = soft) | soft | simpan + flag + catat di dq_report | tidak berlaku |
| order_payments | `installments_ge1`: payment_installments >= 1 (temuan #8: 2 baris bernilai 0 = soft) | soft | simpan + flag + catat di dq_report | tidak berlaku |
| order_reviews | jumlah baris = 99.224 | hard | batch berhenti | — |
| order_reviews | tipe & NOT NULL sesuai §1 | hard | baris → karantina | 0,5 % |
| order_reviews | `score_1_5`: review_score di 1..5 | hard | baris → karantina | 0,5 % |
| order_reviews | `answer_after_creation`: answer >= creation (temuan #15: 0 baris saat ini; soft) | soft | simpan + flag + catat di dq_report | tidak berlaku |
| orders | jumlah baris = 99.441 | hard | batch berhenti | — |
| orders | tipe & NOT NULL sesuai §1 | hard | baris → karantina | 0,1 % |
| orders | kunci `order_id` unik | hard | batch berhenti | 0 % |
| orders | `status_known`: order_status salah satu dari 8 status | hard | baris → karantina | 0,1 % |
| orders | `approved_after_purchase`: approved_at >= purchase | hard | baris → karantina | 0,1 % |
| orders | `delivered_has_date`: status delivered wajib punya tanggal tiba (temuan #9: 8 baris = soft) | soft | simpan + flag + catat di dq_report | tidak berlaku |
| orders | `customer_after_carrier`: tiba >= diserahkan ke kurir (temuan #9: 23 baris = soft) | soft | simpan + flag + catat di dq_report | tidak berlaku |
| products | jumlah baris = 32.951 | hard | batch berhenti | — |
| products | tipe & NOT NULL sesuai §1 | hard | baris → karantina | 0,1 % |
| products | kunci `product_id` unik | hard | batch berhenti | 0 % |
| products | `weight_positive`: product_weight_g > 0 (temuan #16: 4 baris bernilai 0 = soft) | soft | simpan + flag + catat di dq_report | tidak berlaku |
| sellers | jumlah baris = 3.095 | hard | batch berhenti | — |
| sellers | tipe & NOT NULL sesuai §1 (zip char(5) dibaca sebagai teks) | hard | baris → karantina | 0,0 % |
| sellers | kunci `seller_id` unik | hard | batch berhenti | 0 % |
| sellers | `state_len2`: seller_state harus 2 huruf (UF) | hard | baris → karantina | 0,0 % |
| product_category_translation | jumlah baris = 71 | hard | batch berhenti | — |
| product_category_translation | tipe & NOT NULL sesuai §1 | hard | baris → karantina | 0,0 % |
| product_category_translation | kunci `product_category_name` unik | hard | batch berhenti | 0 % |

Aturan lintas tabel (FK, temuan #13): `order_items.{order_id,product_id,seller_id}`, `order_payments.order_id`, `order_reviews.order_id`, `orders.customer_id` → hard; baris yatim dikarantina, bukan dihapus.

Ambang karantina per tabel — jika persentase baris terkarantina melebihi ambang, batch dihentikan (bukan dilanjutkan):

| Tabel | Ambang | Alasan |
|---|---:|---|
| customers | 0,0 % | kunci & FK ke orders; satu baris rusak = indikasi file korup → batch berhenti |
| geolocation | 2,0 % | titik di luar Brasil (1,17%) soft: disimpan + flag, tidak dihitung; ambang untuk pelanggaran hard (tipe/null); >2% berarti sumber rusak |
| order_items | 0,0 % | sumber pendapatan Q1; baris tanpa order/produk/seller tak boleh lolos |
| order_payments | 0,1 % | 14 baris janggal (0,01%) bersifat soft (simpan + flag); ambang untuk pelanggaran hard; >0,1% berarti anomali baru |
| order_reviews | 0,5 % | skor di luar 1–5 mengubah rata-rata Q2; toleransi kecil |
| orders | 0,1 % | 31 baris waktu janggal (0,03%) bersifat soft (simpan + flag); ambang untuk pelanggaran hard; >0,1% = anomali baru |
| products | 0,1 % | kategori null (1,85%) bukan error — jadi 'unknown', bukan karantina |
| sellers | 0,0 % | hanya 3.095 baris; kunci & FK harus bersih |
| product_category_translation | 0,0 % | 71 baris; harus persis, BOM ditangani saat baca |

## 4. Keputusan pemodelan yang mengikuti dari kontrak ini

- **Grain `dim_customer` = `customer_unique_id`** (SCD tipe 1). `customer_id` hanyalah id per-order; memakainya sebagai
  grain menghitung 99.441 "pelanggan" padahal orang unik hanya 96.096 (temuan #1). `fact_order` menyimpan
  `customer_id` asli dan di-join ke `dim_customer` lewat `customer_unique_id`.
- **Kebijakan `fact_review` = satu baris per order.** Dari tiap order dengan >1 review dipilih review dengan
  `review_answer_timestamp` terbaru (seri → `review_creation_date` terbaru → `review_id` terkecil); jumlah review asli disimpan di
  `review_count`. `review_id` duplikat (temuan #2) dideduplikasi lebih dulu dengan aturan yang sama. Tidak ada review yang dibuang diam-diam:
  semuanya tetap di silver; yang tidak terpilih ada di tabel karantina `reviews_superseded`.
- **`is_late`** = `order_delivered_customer_date > order_estimated_delivery_date` hanya bila status `delivered` **dan** tanggal tiba terisi;
  selain itu **NULL** (bukan false) — order tak terkirim tidak boleh dihitung "tepat waktu" (temuan #9, #17).
- **Bulan lengkap**: `dim_date.is_complete_month = false` untuk bulan dengan jumlah order < 5% median bulanan
  (2016-09, 2016-12, 2018-09, 2018-10 pada data ini; temuan #11). Mart tren wajib menyaring / menandai bulan ini.
- **Kunci geografis = zip prefix teks 5 digit**; `dim_geo` satu baris per prefix + baris `unknown` (temuan #3, #4, #14).
- **Order tanpa item/pembayaran** tetap di `fact_order` dengan `has_items` / `has_payment` (temuan #7).
- **Rekonsiliasi**: mart menampilkan dua angka, nilai order (Σ price+freight) dan nilai dibayar (Σ payment_value), plus selisihnya (temuan #10).
- **Silver mempertahankan nama kolom sumber** (termasuk typo `lenght`); `dw` memperbaikinya jadi `length`.
