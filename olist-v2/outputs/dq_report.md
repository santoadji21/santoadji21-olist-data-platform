# Laporan Kualitas Data (silver, Spark)

Dijalankan 2026-10-14T13:51:49+00:00 · 23.35 s · spark 3.5.4 (local[2])

Verdict: `PASS` · `PASS+KARANTINA` (ada baris dikarantina, di bawah ambang) · `FAIL` (di atas ambang / kunci tidak unik / jumlah baris menyimpang → silver tidak disediakan).

## Ringkasan per tabel

| Tabel | Baris sumber | Karantina | Lolos (silver) | Karantina % | Ambang % | Verdict |
|---|---:|---:|---:|---:|---:|---|
| customers | 99,441 | 0 | 99,441 | 0.000 | 0.0 | **PASS** |
| geolocation | 1,000,163 | 0 | 1,000,163 | 0.000 | 2.0 | **PASS** |
| products | 32,951 | 0 | 32,951 | 0.000 | 0.1 | **PASS** |
| sellers | 3,095 | 0 | 3,095 | 0.000 | 0.0 | **PASS** |
| product_category_translation | 71 | 0 | 71 | 0.000 | 0.0 | **PASS** |
| orders | 99,441 | 0 | 99,441 | 0.000 | 0.1 | **PASS** |
| order_items | 112,650 | 0 | 112,650 | 0.000 | 0.0 | **PASS** |
| order_payments | 103,886 | 0 | 103,886 | 0.000 | 0.1 | **PASS** |
| order_reviews | 99,224 | 0 | 99,224 | 0.000 | 0.5 | **PASS** |

## Aturan

| Tabel | Aturan | Jenis | Tindakan | Pelanggaran | % |
|---|---|---|---|---:|---:|
| customers | `row_count` | hard | batch berhenti | 0 | 0.000 |
| customers | `key_unique(customer_id)` | hard | batch berhenti | 0 | 0.000 |
| customers | `type_and_not_null` | hard | baris → karantina | 0 | 0.000 |
| customers | `state_len2` | hard | baris → karantina | 0 | 0.000 |
| geolocation | `row_count` | hard | batch berhenti | 0 | 0.000 |
| geolocation | `type_and_not_null` | hard | baris → karantina | 0 | 0.000 |
| geolocation | `lat_in_brazil` | soft | simpan + flag | 31 | 0.003 |
| geolocation | `lng_in_brazil` | soft | simpan + flag | 11,682 | 1.168 |
| products | `row_count` | hard | batch berhenti | 0 | 0.000 |
| products | `key_unique(product_id)` | hard | batch berhenti | 0 | 0.000 |
| products | `type_and_not_null` | hard | baris → karantina | 0 | 0.000 |
| products | `weight_positive` | soft | simpan + flag | 4 | 0.012 |
| sellers | `row_count` | hard | batch berhenti | 0 | 0.000 |
| sellers | `key_unique(seller_id)` | hard | batch berhenti | 0 | 0.000 |
| sellers | `type_and_not_null` | hard | baris → karantina | 0 | 0.000 |
| sellers | `state_len2` | hard | baris → karantina | 0 | 0.000 |
| product_category_translation | `row_count` | hard | batch berhenti | 0 | 0.000 |
| product_category_translation | `key_unique(product_category_name)` | hard | batch berhenti | 0 | 0.000 |
| product_category_translation | `type_and_not_null` | hard | baris → karantina | 0 | 0.000 |
| orders | `row_count` | hard | batch berhenti | 0 | 0.000 |
| orders | `key_unique(order_id)` | hard | batch berhenti | 0 | 0.000 |
| orders | `type_and_not_null` | hard | baris → karantina | 0 | 0.000 |
| orders | `status_known` | hard | baris → karantina | 0 | 0.000 |
| orders | `approved_after_purchase` | hard | baris → karantina | 0 | 0.000 |
| orders | `delivered_has_date` | soft | simpan + flag | 8 | 0.008 |
| orders | `customer_after_carrier` | soft | simpan + flag | 23 | 0.023 |
| order_items | `row_count` | hard | batch berhenti | 0 | 0.000 |
| order_items | `key_unique(order_id,order_item_id)` | hard | batch berhenti | 0 | 0.000 |
| order_items | `type_and_not_null` | hard | baris → karantina | 0 | 0.000 |
| order_items | `price_positive` | hard | baris → karantina | 0 | 0.000 |
| order_items | `freight_nonneg` | hard | baris → karantina | 0 | 0.000 |
| order_payments | `row_count` | hard | batch berhenti | 0 | 0.000 |
| order_payments | `key_unique(order_id,payment_sequential)` | hard | batch berhenti | 0 | 0.000 |
| order_payments | `type_and_not_null` | hard | baris → karantina | 0 | 0.000 |
| order_payments | `value_positive` | soft | simpan + flag | 9 | 0.009 |
| order_payments | `type_known` | soft | simpan + flag | 3 | 0.003 |
| order_payments | `installments_ge1` | soft | simpan + flag | 2 | 0.002 |
| order_reviews | `row_count` | hard | batch berhenti | 0 | 0.000 |
| order_reviews | `type_and_not_null` | hard | baris → karantina | 0 | 0.000 |
| order_reviews | `score_1_5` | hard | baris → karantina | 0 | 0.000 |
| order_reviews | `answer_after_creation` | soft | simpan + flag | 0 | 0.000 |

Total aturan: 32 hard, 9 soft (minimal tugas: 10 hard, 8 soft).

Karantina: `silver/olist/_quarantine/<tabel>/` (kolom sumber apa adanya + `_row` + `_reasons`).
