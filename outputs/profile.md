# Profil Dataset Olist (T0)

Dibangkitkan otomatis oleh `python pipeline.py profile` — 2026-10-12 17:57:09.
Semua kolom dibaca sebagai teks (`dtype=str`); angka di bawah adalah temuan, bukan konversi.

## 1. Ringkasan tabel

| Tabel | File | Baris | Diharapkan | OK | Kunci alami | Duplikat kunci | Duplikat persis |
|---|---|---:|---:|:-:|---|---:|---:|
| customers | `olist_customers_dataset.csv` | 99,441 | 99,441 | ✅ | customer_id | 0 | 0 |
| geolocation | `olist_geolocation_dataset.csv` | 1,000,163 | 1,000,163 | ✅ | — (tidak ada) | n/a | 261,831 |
| order_items | `olist_order_items_dataset.csv` | 112,650 | 112,650 | ✅ | order_id, order_item_id | 0 | 0 |
| order_payments | `olist_order_payments_dataset.csv` | 103,886 | 103,886 | ✅ | order_id, payment_sequential | 0 | 0 |
| order_reviews | `olist_order_reviews_dataset.csv` | 99,224 | 99,224 | ✅ | — (tidak ada) | n/a | 0 |
| orders | `olist_orders_dataset.csv` | 99,441 | 99,441 | ✅ | order_id | 0 | 0 |
| products | `olist_products_dataset.csv` | 32,951 | 32,951 | ✅ | product_id | 0 | 0 |
| sellers | `olist_sellers_dataset.csv` | 3,095 | 3,095 | ✅ | seller_id | 0 | 0 |
| product_category_translation | `product_category_name_translation.csv` | 71 | 71 | ✅ | product_category_name | 0 | 0 |

## 2. Profil kolom

### customers

| Kolom | Tipe target | Null | Null % | Distinct | Min | Max | Tak terparse |
|---|---|---:|---:|---:|---|---|---:|
| customer_id | text | 0 | 0.0 | 99,441 |  |  | 0 |
| customer_unique_id | text | 0 | 0.0 | 96,096 |  |  | 0 |
| customer_zip_code_prefix | char(5) | 0 | 0.0 | 14,994 | len=5 | len=5 | 0 |
| customer_city | text | 0 | 0.0 | 4,119 |  |  | 0 |
| customer_state | char(2) | 0 | 0.0 | 27 | len=2 | len=2 | 0 |

### geolocation

| Kolom | Tipe target | Null | Null % | Distinct | Min | Max | Tak terparse |
|---|---|---:|---:|---:|---|---|---:|
| geolocation_zip_code_prefix | char(5) | 0 | 0.0 | 19,015 | len=5 | len=5 | 0 |
| geolocation_lat | double precision | 0 | 0.0 | 717,372 | -36.6053744107061 | 45.06593318269697 | 0 |
| geolocation_lng | double precision | 0 | 0.0 | 717,615 | -101.46676644931476 | 121.10539381057764 | 0 |
| geolocation_city | text | 0 | 0.0 | 8,011 |  |  | 0 |
| geolocation_state | char(2) | 0 | 0.0 | 27 | len=2 | len=2 | 0 |

### order_items

| Kolom | Tipe target | Null | Null % | Distinct | Min | Max | Tak terparse |
|---|---|---:|---:|---:|---|---|---:|
| order_id | text | 0 | 0.0 | 98,666 |  |  | 0 |
| order_item_id | integer | 0 | 0.0 | 21 | 1.0 | 21.0 | 0 |
| product_id | text | 0 | 0.0 | 32,951 |  |  | 0 |
| seller_id | text | 0 | 0.0 | 3,095 |  |  | 0 |
| shipping_limit_date | timestamp | 0 | 0.0 | 93,318 | 2016-09-19 00:15:34 | 2020-04-09 22:35:08 | 0 |
| price | numeric(10,2) | 0 | 0.0 | 5,968 | 0.85 | 6735.0 | 0 |
| freight_value | numeric(10,2) | 0 | 0.0 | 6,999 | 0.0 | 409.68 | 0 |

### order_payments

| Kolom | Tipe target | Null | Null % | Distinct | Min | Max | Tak terparse |
|---|---|---:|---:|---:|---|---|---:|
| order_id | text | 0 | 0.0 | 99,440 |  |  | 0 |
| payment_sequential | integer | 0 | 0.0 | 29 | 1.0 | 29.0 | 0 |
| payment_type | text | 0 | 0.0 | 5 |  |  | 0 |
| payment_installments | integer | 0 | 0.0 | 24 | 0.0 | 24.0 | 0 |
| payment_value | numeric(10,2) | 0 | 0.0 | 29,077 | 0.0 | 13664.08 | 0 |

### order_reviews

| Kolom | Tipe target | Null | Null % | Distinct | Min | Max | Tak terparse |
|---|---|---:|---:|---:|---|---|---:|
| review_id | text | 0 | 0.0 | 98,410 |  |  | 0 |
| order_id | text | 0 | 0.0 | 98,673 |  |  | 0 |
| review_score | smallint | 0 | 0.0 | 5 | 1.0 | 5.0 | 0 |
| review_comment_title | text | 87,656 | 88.34 | 4,527 |  |  | 0 |
| review_comment_message | text | 58,247 | 58.7 | 36,159 |  |  | 0 |
| review_creation_date | timestamp | 0 | 0.0 | 636 | 2016-10-02 00:00:00 | 2018-08-31 00:00:00 | 0 |
| review_answer_timestamp | timestamp | 0 | 0.0 | 98,248 | 2016-10-07 18:32:28 | 2018-10-29 12:27:35 | 0 |

### orders

| Kolom | Tipe target | Null | Null % | Distinct | Min | Max | Tak terparse |
|---|---|---:|---:|---:|---|---|---:|
| order_id | text | 0 | 0.0 | 99,441 |  |  | 0 |
| customer_id | text | 0 | 0.0 | 99,441 |  |  | 0 |
| order_status | text | 0 | 0.0 | 8 |  |  | 0 |
| order_purchase_timestamp | timestamp | 0 | 0.0 | 98,875 | 2016-09-04 21:15:19 | 2018-10-17 17:30:18 | 0 |
| order_approved_at | timestamp | 160 | 0.16 | 90,733 | 2016-09-15 12:16:38 | 2018-09-03 17:40:06 | 0 |
| order_delivered_carrier_date | timestamp | 1,783 | 1.79 | 81,018 | 2016-10-08 10:34:01 | 2018-09-11 19:48:28 | 0 |
| order_delivered_customer_date | timestamp | 2,965 | 2.98 | 95,664 | 2016-10-11 13:46:32 | 2018-10-17 13:22:46 | 0 |
| order_estimated_delivery_date | timestamp | 0 | 0.0 | 459 | 2016-09-30 00:00:00 | 2018-11-12 00:00:00 | 0 |

### products

| Kolom | Tipe target | Null | Null % | Distinct | Min | Max | Tak terparse |
|---|---|---:|---:|---:|---|---|---:|
| product_id | text | 0 | 0.0 | 32,951 |  |  | 0 |
| product_category_name | text | 610 | 1.85 | 73 |  |  | 0 |
| product_name_lenght | integer | 610 | 1.85 | 66 | 5.0 | 76.0 | 0 |
| product_description_lenght | integer | 610 | 1.85 | 2,960 | 4.0 | 3992.0 | 0 |
| product_photos_qty | integer | 610 | 1.85 | 19 | 1.0 | 20.0 | 0 |
| product_weight_g | integer | 2 | 0.01 | 2,204 | 0.0 | 40425.0 | 0 |
| product_length_cm | integer | 2 | 0.01 | 99 | 7.0 | 105.0 | 0 |
| product_height_cm | integer | 2 | 0.01 | 102 | 2.0 | 105.0 | 0 |
| product_width_cm | integer | 2 | 0.01 | 95 | 6.0 | 118.0 | 0 |

### sellers

| Kolom | Tipe target | Null | Null % | Distinct | Min | Max | Tak terparse |
|---|---|---:|---:|---:|---|---|---:|
| seller_id | text | 0 | 0.0 | 3,095 |  |  | 0 |
| seller_zip_code_prefix | char(5) | 0 | 0.0 | 2,246 | len=5 | len=5 | 0 |
| seller_city | text | 0 | 0.0 | 611 |  |  | 0 |
| seller_state | char(2) | 0 | 0.0 | 23 | len=2 | len=2 | 0 |

### product_category_translation

| Kolom | Tipe target | Null | Null % | Distinct | Min | Max | Tak terparse |
|---|---|---:|---:|---:|---|---|---:|
| product_category_name | text | 0 | 0.0 | 71 |  |  | 0 |
| product_category_name_english | text | 0 | 0.0 | 71 |  |  | 0 |

## 3. Temuan "kotor" (dengan angka)

| # | Tabel | Temuan | Angka | Jenis |
|---:|---|---|---|---|
| 1 | customers | Dua id pelanggan: `customer_id` per-ORDER, `customer_unique_id` = ORANG | customer_id unik=99,441; customer_unique_id unik=96,096; selisih=3,345 (3.36% jika salah pilih); orang belanja >1 kali=2,997 | silent-wrong |
| 2 | order_reviews | `review_id` tidak unik dan satu `order_id` bisa punya >1 ulasan | baris dengan review_id berulang=814; review_id unik=98,410; order dengan >1 ulasan=547; order unik=98,673; pasangan (review_id, order_id) berulang=0 | silent-wrong |
| 3 | customers, sellers, geolocation | Kode pos prefix berawalan `0` (`'01003'`) — kehilangan nol di depan bila dibaca sebagai angka | baris berawalan '0' (akan jadi 4 digit jika di-cast ke integer): customers=23,995 dari 99,441 (24.13%); sellers=1,027 dari 3,095 (33.18%); geolocation=245,733 dari 1,000,163 (24.57%); panjang string di file sumber=[5] | silent-wrong |
| 4 | geolocation | Satu zip prefix punya banyak baris + duplikat persis (bukan kunci) | baris=1,000,163; prefix unik=19,015; maks baris/prefix=1,146; duplikat persis=261,831 (26.18%) | silent-wrong |
| 5 | geolocation | Koordinat di luar Brasil (lat −34…+5, lng −74…−35) | titik di luar Brasil=11,687 (1.17%); lat [-36.61, 45.07]; lng [-101.47, 121.11] | quarantine |
| 6 | product_category_translation | File diawali BOM UTF-8 → nama kolom pertama tak terlihat berbeda, join kategori diam-diam gagal | 3 byte pertama=efbbbf; repr kolom pertama (utf-8)='\ufeffproduct_category_name'; repr dengan utf-8-sig='product_category_name' | silent-wrong |
| 7 | orders | Order tanpa item dan order tanpa pembayaran | order tanpa item=775 (status: {'unavailable': 603, 'canceled': 164, 'created': 5, 'invoiced': 2, 'shipped': 1}); order tanpa pembayaran=1 | silent-wrong |
| 8 | order_payments | Pembayaran bernilai 0, `payment_type='not_defined'`, dan cicilan 0 | payment_value=0: 9; payment_type not_defined: 3; payment_installments=0: 2 | keep-and-flag |
| 9 | orders | Status `delivered` tanpa tanggal terkirim; tanggal terkirim lebih awal dari tanggal diserahkan ke kurir | delivered tanpa order_delivered_customer_date=8; delivered tanpa order_delivered_carrier_date=2; delivered_customer < delivered_carrier=23; approved_at < purchase=0 | quarantine |
| 10 | order_items vs order_payments | Σ(price+freight) item ≠ Σ(payment_value) — penyebabnya lebih dari satu | order dgn item & bayar=98,665; selisih >0,01=378; selisih >1,00=249; dari yang >0,01: dibayar LEBIH dari nilai item=290 (kartu kredit cicilan >1 di 258 di antaranya), dibayar KURANG=88, memakai voucher=10; total selisih absolut=3,271.95 BRL | keep-and-flag |
| 11 | orders | Bulan pertama dan terakhir dataset hanya berisi segelintir order | rentang purchase=2016-09-04 21:15:19 … 2018-10-17 17:30:18; bulan pertama 2016-09=4; kedua 2016-10=324; bulan terakhir 2018-10=4; median per bulan=4,285; bulan <5% median={'2016-09': 4, '2016-12': 1, '2018-09': 16, '2018-10': 4} | keep-and-flag |
| 12 | products | Produk tanpa kategori, kategori tanpa terjemahan, dan typo nama kolom (`lenght`) | kategori null=610; produk dgn kategori tak terjemahkan=13 (kategori: ['pc_gamer', 'portateis_cozinha_e_preparadores_de_alimentos']); dimensi null=2; kolom typo: product_name_lenght, product_description_lenght | keep-and-flag |
| 13 | semua | Integritas referensial antar tabel (yatim/orphan) | order_items.order_id→orders=0; order_items.product_id→products=0; order_items.seller_id→sellers=0; order_payments.order_id→orders=0; order_reviews.order_id→orders=0; orders.customer_id→customers=0 | hard-rule |
| 14 | customers, sellers → geolocation | Kode pos pelanggan/seller yang tidak ada di tabel geolocation | customers=278 baris; sellers=7 baris (setelah lpad 5 digit) | keep-and-flag |
| 15 | order_reviews | Skor di luar 1–5 dan jawaban lebih awal dari pembuatan survei; komentar kosong dominan | skor di luar 1–5=0; answer<creation=0; judul kosong=88.34%; pesan kosong=58.70% | keep-and-flag |
| 16 | order_items, products | Harga ≤ 0, ongkir 0, dan berat produk 0 | price<=0=0; freight_value=0=383; freight_value<0=0; product_weight_g=0=4 | keep-and-flag |
| 17 | orders | Status canceled/unavailable tetapi punya tanggal tiba di pelanggan | canceled/unavailable dengan delivered_customer_date=6; sebaran status={'delivered': 96478, 'shipped': 1107, 'canceled': 625, 'unavailable': 609, 'invoiced': 314, 'processing': 301, 'created': 5, 'approved': 2} | keep-and-flag |
| 18 | sellers, geolocation | Nama kota tidak seragam: satu zip prefix punya beberapa ejaan, karakter non-ASCII/simbol | seller_city berisi karakter non-huruf=24 dari 3,095; prefix geolocation dengan >1 ejaan kota=8,556 dari 19,015; baris geolocation_city non-ASCII=73,442 dari 1,000,163 | keep-and-flag |
