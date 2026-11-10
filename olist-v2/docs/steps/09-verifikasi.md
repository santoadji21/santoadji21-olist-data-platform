# Langkah 9: Verifikasi dan bukti (T6)

Status: selesai. Kebenaran platform ditunjukkan lewat bukti, tidak hanya diklaim.

| Bukti | Perintah | Hasil |
|---|---|---|
| Kesehatan semua lapisan | `make smoke` | service healthy, bronze 9 manifest, raw = sumber − karantina, 28 pemeriksaan dw, angka cek tugas, 17 pemeriksaan mart, stream 4/4 |
| Failure injection (6 kasus) | `make inject` (±7 menit; `ONLY=a,c` sebagian) | 6/6 TERTANGKAP, stack pulih → `outputs/failure_tests.md` |
| Satu pintu dari nol | `make all YES=1` | 10 perintah, waktu per tahap → `outputs/run_all.json` |
| Laporan bukti | `make report` | `EVALUATION_EVIDENCE.md` dari `outputs/*.json`, tanpa angka ketikan tangan |
| Runbook | [RUNBOOK.md](../../RUNBOOK.md) | 11 gejala: penyebab, cek, pemulihan |
| Unit test tanpa Docker | `make test` | 80 tes: 66 lulus dan 14 dilewati bila stack mati (tes yang butuh Postgres hidup, serta tes Spark karena pyspark tidak ada di venv). Dengan stack hidup, hasil tercatat 79 lulus dan 1 dilewati (angka lama, belum diulang) |

## Kasus failure injection
| # | Kasus | Hasil |
|---|---|---|
| a | Kolom hilang di sumber | silver berhenti sebelum menulis; silver dan `raw` tidak berubah |
| b | Producer dijalankan dua kali | event tersimpan tetap = event unik |
| c | Consumer mati sebelum commit offset (exit 137) | pesan diproses ulang, hasil akhir sama |
| d | Broker Kafka mati | outage pendek terserap; outage panjang: producer naif kehilangan event tanpa error, producer yang memeriksa melaporkan kegagalan |
| e | Seluruh jalur batch dijalankan ulang | semua file `unchanged`, 26 besaran identik |
| f | Satu byte objek bronze diubah | sha256 tidak cocok dengan manifest, silver menolak |

Perusak sungguhan: proses dimatikan lewat exit code, broker lewat `docker stop`, sumber lewat salinan sementara (data asli tidak diubah). Dijalankan dari laptop (`olist_v2/failures.py`); tahap pipeline lewat `docker compose exec`.

## Batas 15 menit dan 10 perintah (T6)
`make all YES=1` (reset berupa `down -v`, lalu 10 perintah) memakan 161,7 detik (2,7 menit), dan semua perintah exit 0 (`outputs/run_all.json`). Syarat tugas: ≤ 15 menit dan ≤ 10 perintah.

Batasan pengukuran: image kustom sudah terbangun (tahap `up` 34 s tanpa build) dan CSV di `data/raw` dipertahankan (tahap `download` 2,4 s, bukan unduhan Kaggle). Build image dari nol dan unduhan penuh menambah waktu, dan keduanya belum pernah diukur. Untuk mengukurnya, jalankan `make reset-all` lalu `make build`.
