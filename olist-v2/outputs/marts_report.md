# Laporan Mart (T5)

Dibangun 2026-10-14T22:11:13+00:00 · 3.47 s · 17/17 pemeriksaan mart = 0 (`dbt/tests/mart_checks.sql`). Setiap bagian di bawah memakai query dan kalimat insight yang SAMA dengan kartu di dashboard Metabase (`olist_v2/questions.py`); hanya **bulan lengkap** yang ditampilkan.

## Q1 · Pendapatan vs uang terbayar per bulan

**Insight:** Pendapatan bulanan naik dari R$ 120.098 (2017-01) menjadi R$ 848.860 (2018-08); total 13.449.530 dari 111.752 item, dan uang terbayar R$ 15.686.469 melebihi nilai pesanan R$ 15.683.707 (bunga cicilan/voucher).

| year_month | revenue | items | orders_compared | ordered_value | paid_value | paid_minus_ordered |
|---|---|---|---|---|---|---|
| 2017-01 | 120.098,27 | 953 | 787 | 136.943,46 | 137.006,76 | 63,30 |
| 2017-02 | 244.959,35 | 1.936 | 1.718 | 283.561,69 | 283.621,94 | 60,25 |
| 2017-03 | 368.341,32 | 2.975 | 2.617 | 425.617,96 | 425.656,40 | 38,44 |
| 2017-04 | 353.842,98 | 2.660 | 2.377 | 405.848,61 | 405.988,38 | 139,77 |
| 2017-05 | 503.159,19 | 4.106 | 3.640 | 582.710,83 | 582.926,16 | 215,33 |
| 2017-06 | 429.916,61 | 3.571 | 3.205 | 499.652,24 | 499.827,47 | 175,23 |
| 2017-07 | 492.287,30 | 4.493 | 3.946 | 578.753,73 | 578.858,58 | 104,85 |
| 2017-08 | 568.245,79 | 4.882 | 4.272 | 661.903,52 | 662.071,77 | 168,25 |
| 2017-09 | 621.415,91 | 4.815 | 4.227 | 717.102,72 | 717.195,22 | 92,50 |
| 2017-10 | 660.179,62 | 5.300 | 4.547 | 764.756,03 | 764.785,28 | 29,25 |
| 2017-11 | 1.003.862,14 | 8.626 | 7.421 | 1.172.191,68 | 1.172.355,54 | 163,86 |
| 2017-12 | 742.183,79 | 6.300 | 5.618 | 861.526,77 | 861.647,65 | 120,88 |
| 2018-01 | 945.456,29 | 8.173 | 7.187 | 1.101.920,01 | 1.102.639,41 | 719,40 |
| 2018-02 | 837.895,43 | 7.597 | 6.624 | 979.486,16 | 979.828,63 | 342,47 |
| 2018-03 | 981.051,06 | 8.195 | 7.168 | 1.152.656,99 | 1.152.736,74 | 79,75 |

_5 baris lain tidak ditampilkan._

## Q1 · 10 kategori dengan pendapatan tertinggi

**Insight:** Kategori teratas health_beauty menyumbang R$ 1.251.146; 10 kategori teratas total R$ 8.393.782.

| category | revenue | items |
|---|---|---|
| health_beauty | 1.251.145,54 | 9.585 |
| watches_gifts | 1.194.824,97 | 5.965 |
| bed_bath_table | 1.035.485,07 | 11.089 |
| sports_leisure | 977.728,77 | 8.573 |
| computers_accessories | 902.922,70 | 7.760 |
| furniture_decor | 721.584,27 | 8.229 |
| housewares | 625.538,73 | 6.903 |
| cool_stuff | 619.724,39 | 3.772 |
| auto | 585.177,48 | 4.195 |
| garden_tools | 479.650,06 | 4.323 |

## Q2 · Tingkat keterlambatan per negara bagian

**Insight:** AL paling sering terlambat (21,46 %, 85 dari 396 order), sedangkan AM terbaik (2,76 %).

| customer_state | delivered | late | late_rate_pct | avg_delivery_days |
|---|---|---|---|---|
| AL | 396 | 85 | 21,46 | 24,50 |
| MA | 713 | 125 | 17,53 | 21,50 |
| SE | 332 | 51 | 15,36 | 21,40 |
| PI | 475 | 66 | 13,89 | 19,40 |
| CE | 1.273 | 176 | 13,83 | 21,20 |
| RR | 40 | 5 | 12,50 | 29,90 |
| BA | 3.253 | 396 | 12,17 | 19,30 |
| RJ | 12.310 | 1.495 | 12,14 | 15,20 |
| PA | 942 | 106 | 11,25 | 23,70 |
| ES | 1.992 | 214 | 10,74 | 15,70 |
| PB | 516 | 54 | 10,47 | 20,40 |
| TO | 274 | 27 | 9,85 | 17,60 |
| MS | 701 | 68 | 9,70 | 15,50 |
| PE | 1.587 | 153 | 9,64 | 18,40 |
| RN | 470 | 44 | 9,36 | 19,20 |

_12 baris lain tidak ditampilkan._

## Q2 · Skor ulasan: tepat waktu vs terlambat

**Insight:** Order tepat waktu rata-rata diberi skor 4,29 (89.182 ulasan), order terlambat 2,27 (6.378 ulasan); keterlambatan keseluruhan 6,79 %.

| n_on_time | n_late | avg_on_time | avg_late | delivered | late | late_rate_pct |
|---|---|---|---|---|---|---|
| 89.182 | 6.378 | 4,29 | 2,27 | 96.203 | 6.531 | 6,79 |

## Q3 · Seller yang perlu tindakan

**Insight:** 54 dari 634 seller yang dinilai (≥ 30 order) perlu tindakan (38 terlambat, 4 batal, 25 skor rendah); seller yang dinilai mencakup 83,3 % order.

| seller_id | state | orders | revenue | late_rate_pct | canceled_rate_pct | avg_review_score | flag_late | flag_canceled | flag_review |
|---|---|---|---|---|---|---|---|---|---|
| ad781527c93d00d89a11eecd9dcad7c1 | SP | 44 | 6.759,58 | 31,58 | 2,27 | 3,07 | True | False | True |
| 54965bbe3e4f07ae045b90b0b8541f52 | PR | 78 | 10.961,30 | 30,14 | 0 | 3 | True | False | True |
| 2a1348e9addc1af5aaa619b1a3679d6b | MG | 52 | 2.793,10 | 27,08 | 1,92 | 3,12 | True | False | True |
| 835f0f7810c76831d6c7d24c7a646d4d | SP | 44 | 5.060,10 | 26,19 | 2,27 | 3,34 | True | False | True |
| a49928bcdf77c55c6d6e05e09a9b4ca5 | SP | 98 | 8.816,70 | 21,88 | 0 | 3,01 | True | False | True |
| 712e6ed8aa4aa1fa65dab41fed5737e4 | SC | 79 | 39.815 | 20,78 | 0 | 3,38 | True | False | True |
| 850913d59ce317156b00f3705f1c3edb | SP | 32 | 2.648 | 17,24 | 3,13 | 3,44 | True | False | True |
| 88460e8ebdecbfecb5f9601833981930 | PR | 248 | 31.396,65 | 17,07 | 0,40 | 3,41 | True | False | True |
| 1ca7077d890b907f89be8c954a02686a | SP | 115 | 13.341,57 | 16,67 | 0 | 2,33 | True | False | True |
| 602044f2c16190c2c6e45eb35c2e21cb | SP | 50 | 3.739,16 | 16,67 | 0 | 3,14 | True | False | True |
| bbad7e518d7af88a0897397ffdca1979 | SP | 69 | 4.462,22 | 16,18 | 0 | 3,16 | True | False | True |
| 0b35c634521043bf4b47e21547b99ab5 | PR | 51 | 13.533,65 | 16 | 0 | 3,36 | True | False | True |
| c60b801f2d52c7f7f91de00870882a75 | SP | 41 | 12.913,50 | 15,38 | 0 | 3,32 | True | False | True |
| ede0c03645598cdfc63ca8237acbe73d | SP | 46 | 2.431,95 | 32,56 | 2,17 | 3,52 | True | False | False |
| beadbee30901a7f61d031b6b686095ad | SP | 64 | 4.373,98 | 23,44 | 0 | 3,91 | True | False | False |

_35 baris lain tidak ditampilkan._

## Q3 · Ringkasan scorecard seller

**Insight:** 54 dari 634 seller yang dinilai (≥ 30 order) perlu tindakan (38 terlambat, 4 batal, 25 skor rendah); seller yang dinilai mencakup 83,3 % order.

| sellers | rated | needs_action | flag_late | flag_canceled | flag_review | rated_orders | seller_orders |
|---|---|---|---|---|---|---|---|
| 3.095 | 634 | 54 | 38 | 4 | 25 | 83.317 | 100.010 |

## Q4 · Segmen pelanggan (RFM)

**Insight:** Dari 94.983 pembeli, segmen terbesar "Baru" berisi 36.757 orang (38,70 %); hanya 1.858 orang yang setia.

| segment | people | pct | monetary | avg_monetary | avg_recency_days |
|---|---|---|---|---|---|
| Baru | 36.757 | 38,70 | 5.170.005,76 | 141 | 139 |
| Sekali beli, tidak aktif | 34.257 | 36,07 | 1.909.062,31 | 56 | 387 |
| Bernilai tinggi, lama tidak kembali | 14.010 | 14,75 | 3.895.230,49 | 278 | 444 |
| Bernilai tinggi, belum ulang | 7.072 | 7,45 | 1.769.633,75 | 250 | 269 |
| Pelanggan setia | 1.858 | 1,96 | 495.536,18 | 267 | 181 |
| Langganan berisiko hilang | 1.029 | 1,08 | 254.932,25 | 248 | 432 |

## Q4 · Berapa kali orang membeli

**Insight:** 97,0 % pembeli (92.096 dari 94.983) hanya membeli satu kali.

| frequency | people |
|---|---|
| 1 | 92.096 |
| 2 | 2.651 |
| 3 | 188 |
| 4 | 29 |
| 5 | 9 |
| 6 | 5 |
| 7 | 3 |
| 9 | 1 |
| 16 | 1 |

## Rekonsiliasi · nilai pesanan vs uang terbayar

**Insight:** 98,91 % order (98.362) nilai pesanannya cocok dengan uang terbayar (syarat ≥ 98 %); sisanya dijelaskan: tanpa_item 775, cicilan 246, tidak_dijelaskan 50, voucher 7, tanpa_pembayaran 1.

| class | orders | pct | abs_diff_brl | net_diff_brl |
|---|---|---|---|---|
| cocok | 98.362 | 98,91 | 2,73 | -0,67 |
| tanpa_item | 775 | 0,78 | 162.591,95 | 162.591,95 |
| cicilan | 246 | 0,25 | 3.050,30 | 3.050,30 |
| tidak_dijelaskan | 50 | 0,05 | 201,44 | -196,72 |
| voucher | 7 | 0,01 | 17,48 | 17,48 |
| tanpa_pembayaran | 1 | 0 | 143,46 | -143,46 |

## Freshness stream · p50/p95 per batch consumer

**Insight:** Dari 216 batch (p95 tertinggi 3.147 ms), p50 batch awal 177 ms → batch akhir 1.774 ms: lag tumbuh selama replay.

| batch_no | consumed_at | n_messages | n_new | n_duplicate | n_out_of_order | n_dlq | fresh_p50_ms | fresh_p95_ms |
|---|---|---|---|---|---|---|---|---|
| 1 | 2026-10-14 22:10:21.720941+00:00 | 711 | 693 | 15 | 0 | 3 | 61,40 | 68,60 |
| 2 | 2026-10-14 22:10:21.830785+00:00 | 2.000 | 1.933 | 54 | 4 | 13 | 112,10 | 141,70 |
| 3 | 2026-10-14 22:10:21.916344+00:00 | 402 | 388 | 11 | 0 | 3 | 160,70 | 172,50 |
| 4 | 2026-10-14 22:10:21.950573+00:00 | 2.000 | 1.928 | 66 | 4 | 6 | 120,60 | 145,80 |
| 5 | 2026-10-14 22:10:22.054236+00:00 | 865 | 840 | 18 | 4 | 7 | 146,30 | 201,20 |
| 6 | 2026-10-14 22:10:22.120168+00:00 | 2.000 | 1.932 | 58 | 7 | 10 | 139,70 | 208,70 |
| 7 | 2026-10-14 22:10:22.210679+00:00 | 996 | 963 | 32 | 10 | 1 | 154,60 | 198 |
| 8 | 2026-10-14 22:10:22.265786+00:00 | 2.000 | 1.933 | 63 | 18 | 4 | 140,60 | 227,10 |
| 9 | 2026-10-14 22:10:22.351891+00:00 | 627 | 607 | 18 | 4 | 2 | 177,60 | 203,50 |
| 10 | 2026-10-14 22:10:22.391625+00:00 | 1.622 | 1.565 | 46 | 9 | 11 | 168,40 | 194 |
| 11 | 2026-10-14 22:10:22.463969+00:00 | 700 | 667 | 28 | 6 | 5 | 119,10 | 136,40 |
| 12 | 2026-10-14 22:10:22.508184+00:00 | 2.000 | 1.948 | 42 | 24 | 10 | 107,40 | 137,80 |
| 13 | 2026-10-14 22:10:22.591588+00:00 | 678 | 658 | 18 | 10 | 2 | 120,80 | 155,20 |
| 14 | 2026-10-14 22:10:22.634021+00:00 | 2.000 | 1.935 | 58 | 26 | 7 | 112,80 | 177,10 |
| 15 | 2026-10-14 22:10:22.724519+00:00 | 1.146 | 1.111 | 31 | 19 | 4 | 143,10 | 177 |

_201 baris lain tidak ditampilkan._

## Pemeriksaan mart

| Pemeriksaan | Baris bermasalah |
|---|---:|
| sales_revenue_equals_fact_items | 0 |
| sales_items_equals_fact_items | 0 |
| revenue_vs_paid_paid_equals_fact | 0 |
| delivery_denominator_equals_fact_not_null | 0 |
| delivery_late_equals_fact_late | 0 |
| delivery_no_rate_above_100 | 0 |
| seller_count_equals_sellers_with_items | 0 |
| seller_unrated_never_flagged | 0 |
| rfm_customers_equals_people_with_valid_orders | 0 |
| rfm_frequency_sums_to_valid_orders | 0 |
| rfm_scores_in_1_5 | 0 |
| rfm_every_customer_has_a_segment | 0 |
| reconciliation_one_row_per_order | 0 |
| reconciliation_every_order_has_a_class | 0 |
| reconciliation_matched_share_at_least_98pct | 0 |
| reconciliation_paid_equals_fact | 0 |
| stream_freshness_batches_equal_source | 0 |

## Ekspor gold

| Mart | Baris | Parquet (byte) |
|---|---:|---:|
| s3://gold/olist/mart/sales_monthly | 11,805 | 210,419 |
| s3://gold/olist/mart/revenue_vs_paid_monthly | 24 | 5,444 |
| s3://gold/olist/mart/delivery_monthly | 556 | 30,394 |
| s3://gold/olist/mart/seller_scorecard | 3,095 | 178,367 |
| s3://gold/olist/mart/customer_rfm | 94,983 | 3,496,852 |
| s3://gold/olist/mart/reconciliation | 99,441 | 4,042,120 |
| s3://gold/olist/mart/stream_freshness | 1 | 6,697 |
| s3://gold/olist/mart/stream_freshness_batch | 216 | 14,278 |

## Penelusuran angka (mart → model dbt → fakta)

| Mart | Model | Sumber |
|---|---|---|
| mart.sales_monthly | dbt/models/mart/sales_monthly.sql | dw.fact_order_item, dw.dim_product, dw.dim_date |
| mart.revenue_vs_paid_monthly | dbt/models/mart/revenue_vs_paid_monthly.sql | dw.fact_order, dw.dim_date |
| mart.delivery_monthly | dbt/models/mart/delivery_monthly.sql | dw.fact_order (is_late), dw.fact_review, dw.dim_date |
| mart.seller_scorecard | dbt/models/mart/seller_scorecard.sql | dw.fact_order_item, dw.fact_order, dw.fact_review, dw.dim_seller |
| mart.customer_rfm | dbt/models/mart/customer_rfm.sql | dw.fact_order, dw.dim_customer |
| mart.reconciliation | dbt/models/mart/reconciliation.sql | dw.fact_order, dw.fact_payment |
| mart.stream_freshness | dbt/models/mart/stream_freshness.sql | stream.batch_metrics |
| mart.stream_freshness_batch | dbt/models/mart/stream_freshness_batch.sql | stream.batch_metrics |

Contoh penelusuran: pendapatan Q1 = `sum(mart.sales_monthly.revenue)` = `sum(dw.fact_order_item.price)` untuk order bukan `canceled`/`unavailable` (pemeriksaan `sales_revenue_equals_fact_items` = 0).
