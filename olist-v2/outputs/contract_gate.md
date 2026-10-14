# Laporan Gate Kontrak (datacontract test)

Dijalankan 2026-10-14T12:49:46+00:00 · server `silver-minio` (silver di MinIO)

**Hasil: `warning`** — 189 pemeriksaan: 129 lolos, 0 gagal, 8 peringatan soft, 52 pemeriksaan tipe tidak didukung.

## Pelanggaran soft yang dikenal (dicatat, baris tetap lewat)

| Tabel | Aturan | Detail |
|---|---|---|
| geolocation | lat dalam kotak Brasil (-34.0, 5.0) (temuan #5: titik di luar = soft) | Actual custom_sql(geolocation) was 31, expected = 0 |
| geolocation | lng dalam kotak Brasil (-74.0, -35.0) (temuan #5: titik di luar = soft) | Actual custom_sql(geolocation) was 11682, expected = 0 |
| order_payments | payment_value > 0 (temuan #8: 9 baris bernilai 0 = soft) | Actual custom_sql(order_payments) was 9, expected = 0 |
| order_payments | payment_type bukan 'not_defined' (temuan #8: 3 baris = soft) | Actual custom_sql(order_payments) was 3, expected = 0 |
| order_payments | payment_installments >= 1 (temuan #8: 2 baris bernilai 0 = soft) | Actual custom_sql(order_payments) was 2, expected = 0 |
| orders | status delivered wajib punya tanggal tiba (temuan #9: 8 baris = soft) | Actual custom_sql(orders) was 8, expected = 0 |
| orders | tiba >= diserahkan ke kurir (temuan #9: 23 baris = soft) | Actual custom_sql(orders) was 23, expected = 0 |
| products | product_weight_g > 0 (temuan #16: 4 baris bernilai 0 = soft) | Actual custom_sql(products) was 4, expected = 0 |

## Keterbatasan

52 pemeriksaan tipe bertanda *not supported yet*: datacontract-cli belum dapat memeriksa tipe pada file Parquet. Tipe silver diverifikasi oleh gate Spark (`outputs/dq_report.md`) dan test-nya.
