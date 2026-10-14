# Laporan Stream (T3)

Producer: 1 run, 20.4 s · consumer: 219 batch · ujung ke ujung 23.4 s

## Producer (kunci jawaban) vs consumer

| Besaran | Producer | Consumer | Hasil |
|---|---:|---:|---|
| Event unik | 394,713 | 394,713 (distinct 394,713) | ✅ cocok |
| Duplikat diabaikan | 11,812 | 11,812 (total pesan − unik − DLQ) | ✅ cocok |
| Pesan rusak → DLQ | 1,964 | 1,964 | ✅ cocok |
| Order dengan status terkini | 99,441 | 99,441 | ✅ cocok |
| Total pesan di topic | 408,489 | 408,489 | ✅ cocok |
| Pesan gagal terkirim | 0 | — | ✅ cocok |

## Status terkini vs `raw.orders.order_status`

99,441 dari 99,441 order cocok → ✅ cocok

## Terlambat / tidak urut

- Event ditahan producer (terlambat): 7,988
- Event baru yang tiba di bawah status yang sudah tersimpan (out-of-order, dihitung consumer): 6,490. Angka ini tidak sama dengan jumlah event terlambat: event yang ditahan hanya out-of-order bila status berikutnya sudah tiba, dan data asli juga menghasilkan out-of-order alami (tanggal tiba lebih awal dari tanggal kurir).
- Event out-of-order tetap tercatat di `stream.order_events`, tetapi tidak menurunkan `stream.order_current` (status hanya naik).

## Freshness (`consumed_at − produced_at`, event baru)

p50 = 617.7 ms · p95 = 2868.9 ms · maks = 3544.6 ms
