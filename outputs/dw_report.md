# Laporan Star Schema `dw` (T4)

Dibangun 2026-10-12T12:09:02+00:00 · 7.62 s · satu transaksi (DROP + CTAS + PK + FK + pemeriksaan)

## Jumlah baris dan grain

| Tabel | Baris | Grain |
|---|---:|---|
| dw.dim_date | 800 | satu baris per tanggal kalender |
| dw.dim_geo | 18,677 | satu baris per zip prefix (+ baris `unknown`) |
| dw.dim_customer | 96,096 | satu baris per ORANG (`customer_unique_id`) |
| dw.dim_product | 32,951 | satu baris per produk |
| dw.dim_seller | 3,095 | satu baris per seller |
| dw.fact_order | 99,441 | satu baris per order (header) |
| dw.fact_order_item | 112,650 | satu baris per unit barang |
| dw.fact_payment | 103,886 | satu baris per pembayaran |
| dw.fact_review | 98,673 | satu baris per order yang punya ulasan |

## Integritas (semua harus 0 baris bermasalah)

| Pemeriksaan | Baris bermasalah |
|---|---:|
| fk_dim_customer_geo | 0 |
| fk_dim_seller_geo | 0 |
| fk_fact_order_customer | 0 |
| fk_fact_order_purchase_date | 0 |
| fk_fact_order_delivered_date | 0 |
| fk_fact_order_estimated_date | 0 |
| fk_item_order | 0 |
| fk_item_product | 0 |
| fk_item_seller | 0 |
| fk_payment_order | 0 |
| fk_review_order | 0 |
| grain_dim_customer_vs_unique_people | 0 |
| grain_fact_order_vs_raw_orders | 0 |
| grain_fact_order_item_vs_raw | 0 |
| grain_fact_payment_vs_raw | 0 |
| grain_fact_review_vs_orders_with_review | 0 |
| grain_dim_geo_vs_distinct_zip | 0 |
| grain_dim_product_vs_raw | 0 |
| sum_item_price_matches_raw | 0 |
| sum_payment_matches_raw | 0 |
| fact_order_item_count_matches_header | 0 |
| is_late_not_null_for_non_delivered | 0 |
| is_late_null_for_delivered_with_date | 0 |
| has_items_flag_consistent | 0 |
| review_count_ge_1 | 0 |
| dim_product_no_null_category_en | 0 |
| dim_geo_no_point_outside_brazil | 0 |
| dim_geo_single_unknown_row | 0 |

**28/28 pemeriksaan lulus.** PK dan FK terpasang sebagai constraint (bukan hanya didokumentasikan).

## Angka kunci

| Besaran | Nilai |
|---|---:|
| orang unik (`dim_customer`) | 96,096 |
| `fact_order` | 99,441 |
| `fact_order_item` | 112,650 |
| order tanpa item | 775 |
| order tanpa pembayaran | 1 |
| order dengan ≥ 1 ulasan (`fact_review`) | 98,673 |
| order dengan > 1 ulasan | 547 |
| order `delivered` | 96,478 |
| delivered dengan tanggal tiba (`is_late` tidak NULL) | 96,470 |
| terlambat | 6,534 |
| late rate (% dari yang tidak NULL) | 6.77 |
| bulan lengkap (≤ Agu-2018) | 20 |
| pelanggan → `dim_geo` unknown | 1,357 |
| seller → `dim_geo` unknown | 14 |
| produk dengan kategori Portugis (tanpa terjemahan) | 2,058 |
| produk tanpa kategori (`unknown`) | 610 |

Bulan lengkap: 2017-01, 2017-02, 2017-03, 2017-04, 2017-05, 2017-06, 2017-07, 2017-08, 2017-09, 2017-10, 2017-11, 2017-12, 2018-01, 2018-02, 2018-03, 2018-04, 2018-05, 2018-06, 2018-07, 2018-08.

## Waktu per file SQL

| File | Detik |
|---|---:|
| 00_reset.sql | 0.0 |
| 05_dim_date.sql | 0.11 |
| 10_dim_geo.sql | 1.12 |
| 20_dim_customer.sql | 0.35 |
| 21_dim_seller.sql | 0.01 |
| 22_dim_product.sql | 0.08 |
| 30_fact_order.sql | 0.66 |
| 31_fact_order_item.sql | 0.27 |
| 32_fact_payment.sql | 0.23 |
| 33_fact_review.sql | 0.35 |
| 90_foreign_keys.sql | 0.3 |
