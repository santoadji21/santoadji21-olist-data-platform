# Laporan Mart (T5) — jawaban Q1–Q4

Dibangun 2026-10-12T13:15:04+00:00 · 1.96 s · 17/17 pemeriksaan mart = 0. Tabel di bawah dihasilkan oleh query yang sama dengan grafik dashboard (`olist.marts.queries`); hanya **bulan lengkap** (Jan-2017 … Agu-2018) yang ditampilkan.

## Q1 — Pendapatan per bulan dan kategori

Pendapatan = Σ `price` item pada order berstatus bukan `canceled`/`unavailable` (ongkir terpisah).

Pendapatan 2017-01 s.d. 2018-08 (bulan lengkap) 13,449,530 BRL dari 111,752 item; uang terbayar 15,686,469 BRL vs nilai pesanan 15,683,707 BRL (+0.02%); kategori terbesar health_beauty (9.3% pendapatan).

| year_month | revenue | items | orders_compared | ordered_value | paid_value | paid_minus_ordered |
|---|---|---|---|---|---|---|
| 2017-01 | 120,098.27 | 953 | 787 | 136,943.46 | 137,006.76 | 63.30 |
| 2017-02 | 244,959.35 | 1,936 | 1,718 | 283,561.69 | 283,621.94 | 60.25 |
| 2017-03 | 368,341.32 | 2,975 | 2,617 | 425,617.96 | 425,656.40 | 38.44 |
| 2017-04 | 353,842.98 | 2,660 | 2,377 | 405,848.61 | 405,988.38 | 139.77 |
| 2017-05 | 503,159.19 | 4,106 | 3,640 | 582,710.83 | 582,926.16 | 215.33 |
| 2017-06 | 429,916.61 | 3,571 | 3,205 | 499,652.24 | 499,827.47 | 175.23 |
| 2017-07 | 492,287.30 | 4,493 | 3,946 | 578,753.73 | 578,858.58 | 104.85 |
| 2017-08 | 568,245.79 | 4,882 | 4,272 | 661,903.52 | 662,071.77 | 168.25 |
| 2017-09 | 621,415.91 | 4,815 | 4,227 | 717,102.72 | 717,195.22 | 92.50 |
| 2017-10 | 660,179.62 | 5,300 | 4,547 | 764,756.03 | 764,785.28 | 29.25 |
| 2017-11 | 1,003,862.14 | 8,626 | 7,421 | 1,172,191.68 | 1,172,355.54 | 163.86 |
| 2017-12 | 742,183.79 | 6,300 | 5,618 | 861,526.77 | 861,647.65 | 120.88 |
| 2018-01 | 945,456.29 | 8,173 | 7,187 | 1,101,920.01 | 1,102,639.41 | 719.40 |
| 2018-02 | 837,895.43 | 7,597 | 6,624 | 979,486.16 | 979,828.63 | 342.47 |
| 2018-03 | 981,051.06 | 8,195 | 7,168 | 1,152,656.99 | 1,152,736.74 | 79.75 |
| 2018-04 | 993,592.98 | 7,957 | 6,919 | 1,156,248.89 | 1,156,303.91 | 55.02 |
| 2018-05 | 992,871.75 | 7,898 | 6,833 | 1,145,686.46 | 1,145,748.63 | 62.17 |
| 2018-06 | 863,265.53 | 7,060 | 6,145 | 1,020,381.90 | 1,020,494.29 | 112.39 |
| 2018-07 | 878,044.27 | 7,039 | 6,233 | 1,039,783.58 | 1,039,880.16 | 96.58 |
| 2018-08 | 848,860.10 | 7,216 | 6,421 | 996,973.51 | 996,896.15 | -77.36 |

Top 10 kategori:

| category | revenue | items |
|---|---|---|
| health_beauty | 1,251,145.54 | 9,585 |
| watches_gifts | 1,194,824.97 | 5,965 |
| bed_bath_table | 1,035,485.07 | 11,089 |
| sports_leisure | 977,728.77 | 8,573 |
| computers_accessories | 902,922.70 | 7,760 |
| furniture_decor | 721,584.27 | 8,229 |
| housewares | 625,538.73 | 6,903 |
| cool_stuff | 619,724.39 | 3,772 |
| auto | 585,177.48 | 4,195 |
| garden_tools | 479,650.06 | 4,323 |

## Q2 — Keterlambatan pengiriman dan dampaknya ke review

Late rate = terlambat ÷ order dengan `is_late IS NOT NULL` (delivered dan tanggal tiba terisi).

Late rate 6.79% dari 96,203 order terkirim; tertinggi di AL (21.5%, n=396) dan, di antara negara bagian dengan ≥ 500 order, MA (17.5%, n=713); skor review turun dari 4.29 (tepat waktu) menjadi 2.27 (terlambat).

| customer_state | delivered | late | late_rate_pct | avg_delivery_days |
|---|---|---|---|---|
| AL | 396 | 85 | 21.46 | 24.5 |
| MA | 713 | 125 | 17.53 | 21.5 |
| SE | 332 | 51 | 15.36 | 21.4 |
| PI | 475 | 66 | 13.89 | 19.4 |
| CE | 1,273 | 176 | 13.83 | 21.2 |
| RR | 40 | 5 | 12.50 | 29.9 |
| BA | 3,253 | 396 | 12.17 | 19.3 |
| RJ | 12,310 | 1,495 | 12.14 | 15.2 |
| PA | 942 | 106 | 11.25 | 23.7 |
| ES | 1,992 | 214 | 10.74 | 15.7 |
| PB | 516 | 54 | 10.47 | 20.4 |
| TO | 274 | 27 | 9.85 | 17.6 |
| MS | 701 | 68 | 9.70 | 15.5 |
| PE | 1,587 | 153 | 9.64 | 18.4 |
| RN | 470 | 44 | 9.36 | 19.2 |
| SC | 3,537 | 291 | 8.23 | 14.9 |
| GO | 1,950 | 128 | 6.56 | 15.5 |
| RS | 5,327 | 325 | 6.10 | 15.2 |
| MT | 885 | 53 | 5.99 | 18.0 |
| DF | 2,074 | 118 | 5.69 | 12.9 |
| MG | 11,319 | 519 | 4.59 | 11.9 |
| SP | 40,399 | 1,817 | 4.50 | 8.7 |
| PR | 4,903 | 199 | 4.06 | 11.9 |
| AC | 80 | 3 | 3.75 | 21.0 |
| AP | 67 | 2 | 2.99 | 27.2 |
| RO | 243 | 7 | 2.88 | 19.3 |
| AM | 145 | 4 | 2.76 | 26.4 |

## Q3 — Seller yang perlu ditindak

Hanya seller dengan ≥ 30 order yang dinilai. Ambang: late rate ≥ 15 %, batal ≥ 5 %, skor ≤ 3,5. Top 15 menurut jumlah flag lalu late rate:

54 dari 634 seller yang dinilai (≥ 30 order, mencakup 83% order) perlu ditindak: 38 karena late rate ≥ 15%, 4 karena batal ≥ 5%, 25 karena skor ≤ 3,5 (seller bisa terkena lebih dari satu).

| seller_id | state | orders | late_rate_pct | canceled_rate_pct | avg_review_score |
|---|---|---|---|---|---|
| ad781527c93d | SP | 44 | 31.58 | 2.27 | 3.068 |
| 54965bbe3e4f | PR | 78 | 30.14 | 0.00 | 3.000 |
| 2a1348e9addc | MG | 52 | 27.08 | 1.92 | 3.118 |
| 835f0f7810c7 | SP | 44 | 26.19 | 2.27 | 3.341 |
| a49928bcdf77 | SP | 98 | 21.88 | 0.00 | 3.010 |
| 712e6ed8aa4a | SC | 79 | 20.78 | 0.00 | 3.385 |
| 850913d59ce3 | SP | 32 | 17.24 | 3.13 | 3.438 |
| 88460e8ebdec | PR | 248 | 17.07 | 0.40 | 3.413 |
| 602044f2c161 | SP | 50 | 16.67 | 0.00 | 3.143 |
| 1ca7077d890b | SP | 115 | 16.67 | 0.00 | 2.333 |
| bbad7e518d7a | SP | 69 | 16.18 | 0.00 | 3.162 |
| 0b35c6345210 | PR | 51 | 16.00 | 0.00 | 3.360 |
| c60b801f2d52 | SP | 41 | 15.38 | 0.00 | 3.317 |
| ede0c0364559 | SP | 46 | 32.56 | 2.17 | 3.523 |
| beadbee30901 | SP | 64 | 23.44 | 0.00 | 3.906 |

## Q4 — Siapa pelanggan kita (RFM per orang)

Titik acuan recency = tanggal beli terakhir di data; F memakai pita tetap (bukan kuintil mentah).

2,887 dari 94,983 orang (3.0%) belanja ulang; 14,010 orang bernilai tinggi sudah lama tidak kembali (rata-rata 444 hari, total 3,895,230 BRL).

| segment | people | pct | avg_monetary | avg_recency_days | monetary |
|---|---|---|---|---|---|
| Baru | 36,757 | 38.70 | 141 | 139 | 5,170,006 |
| Sekali beli, tidak aktif | 34,257 | 36.07 | 56 | 387 | 1,909,062 |
| Bernilai tinggi, lama tidak kembali | 14,010 | 14.75 | 278 | 444 | 3,895,230 |
| Bernilai tinggi, belum ulang | 7,072 | 7.45 | 250 | 269 | 1,769,634 |
| Pelanggan setia | 1,858 | 1.96 | 267 | 181 | 495,536 |
| Langganan berisiko hilang | 1,029 | 1.08 | 248 | 432 | 254,932 |

Distribusi frekuensi (alasan F bukan kuintil):

| frequency | people |
|---|---|
| 1 | 92,096 |
| 2 | 2,651 |
| 3 | 188 |
| 4 | 29 |
| 5 | 9 |
| 6 | 5 |
| 7 | 3 |
| 9 | 1 |
| 16 | 1 |

## Freshness stream

Dari 217 batch (maks p95 2,514 ms), p50 batch awal 219 ms → batch akhir 1,461 ms: lag tumbuh selama replay.

| minute | batches | messages | new_events | duplicates | out_of_order | dlq | fresh_p50_ms | fresh_p95_ms_max |
|---|---|---|---|---|---|---|---|---|
| 2026-10-12 12:02:00+00:00 | 152 | 280803 | 271334 | 8122 | 3553 | 1347 | 523.3 | 1793.0 |
| 2026-10-12 12:03:00+00:00 | 65 | 127686 | 123379 | 3690 | 2937 | 617 | 1382.7 | 2514.3 |

## Rekonsiliasi (nilai pesanan vs dibayar)

98.91% order cocok (toleransi 0,01 BRL); selisih tidak terjelaskan 50 order = 201.44 BRL.

| class | orders | pct | abs_diff_brl | net_diff_brl |
|---|---|---|---|---|
| cocok | 98,362 | 98.91 | 2.73 | -0.67 |
| tanpa_item | 775 | 0.78 | 162591.95 | 162591.95 |
| cicilan | 246 | 0.25 | 3050.30 | 3050.30 |
| tidak_dijelaskan | 50 | 0.05 | 201.44 | -196.72 |
| voucher | 7 | 0.01 | 17.48 | 17.48 |
| tanpa_pembayaran | 1 | 0.00 | 143.46 | -143.46 |

## Penelusuran angka (mart → SQL → fakta)

| Mart | SQL | Sumber |
|---|---|---|
| mart.sales_monthly | sql/mart/10_sales_monthly.sql | dw.fact_order_item, dw.dim_product, dw.dim_date |
| mart.revenue_vs_paid_monthly | sql/mart/10_sales_monthly.sql | dw.fact_order, dw.dim_date |
| mart.delivery_monthly | sql/mart/20_delivery_monthly.sql | dw.fact_order (is_late), dw.fact_review, dw.dim_date |
| mart.seller_scorecard | sql/mart/30_seller_scorecard.sql | dw.fact_order_item, dw.fact_order, dw.fact_review, dw.dim_seller |
| mart.customer_rfm | sql/mart/40_customer_rfm.sql | dw.fact_order, dw.dim_customer |
| mart.reconciliation | sql/mart/50_reconciliation.sql | dw.fact_order, dw.fact_payment |
| mart.stream_freshness | sql/mart/60_stream_freshness.sql | stream.batch_metrics |
| mart.stream_freshness_batch | sql/mart/60_stream_freshness.sql | stream.batch_metrics |

Contoh penelusuran: pendapatan Q1 = `sum(mart.sales_monthly.revenue)` = `sum(dw.fact_order_item.price)` untuk order bukan `canceled`/`unavailable` (pemeriksaan `sales_revenue_equals_fact_items` = 0).
