# Kesesuaian olist-v2 dengan Tugas Besar (T0–T6)

**Posisi v2:** eksplorasi pribadi, **tidak dinilai**. Karena itu aturan "alat" di tugas (mis. §9: dbt hanya untuk bonus, dashboard Power BI atau notebook) **tidak mengikat**. Yang dipertahankan **persis sama** adalah **perilaku dan ketentuan**: apa yang disimpan, bagaimana diberi nama, apa yang dijamin. Alatnya boleh berbeda dan dinaikkan ke kelas production-grade.

Sumber ketentuan: `Tugas_Besar_Olist_Data_Platform.md` dan `TUGAS_BESAR.md` (isinya sama). Rujukan `§` mengikuti nomor bagian di berkas itu.

## 1. Ketentuan yang HARUS sama (parity)

Ini daftar periksa untuk v2. Alat boleh berganti; hasil dan jaminannya tidak.

### T0 — kontrak dan profil
- `DATA_CONTRACT.md`: 9 tabel dengan tipe target eksplisit, kunci alami (dibuktikan unik atau dinyatakan tidak unik), jumlah baris, **≥ 10 temuan kotor dengan angka**, keputusan, dan tahap penerap.
- `outputs/profile.md` **dibangkitkan kode** (null %, kunci unik, rentang tanggal, duplikat persis).
- Angka cek: orang unik **96.096**, order tanpa item **775**, tanpa pembayaran **1**.

### T1 — bronze (tata letak dan manifest)
- Tata letak objek: `olist/<tabel>/ingest_date=YYYY-MM-DD/<file>` (di v2 ditambah `sha256=<12 hex>/` agar tidak menimpa di hari yang sama).
- **Satu manifest JSON per tabel, disimpan di bucket**, berisi minimal: **checksum isi, ukuran byte, jumlah baris, nama sumber, waktu ingest (tanggal/waktu update), riwayat versi**; dari manifest saja harus terbaca objek mana yang terbaru.
- **Checksum: simpan `sha256` (pembanding utama) dan `md5` (tambahan).** Tugas membolehkan keduanya. md5 sama dengan ETag S3 untuk upload tunggal (bukan multipart), jadi bisa dicocokkan langsung dengan MinIO; sha256 dipakai untuk keputusan `unchanged` karena md5 lemah terhadap tabrakan.
- Status per file: `uploaded` / `unchanged` / `new_version`; `outputs/bronze_report.md` dibangkitkan.
- Append-only: ingest dua kali tanpa perubahan = semua `unchanged`; ubah satu byte = versi baru, versi lama tetap; 9 file (±126 MB) < 2 menit.

### T2 — silver, gate, raw
- Parquet silver bertipe; **≥ 10 aturan hard dan ≥ 8 soft**; karantina (bukan dibuang) dengan kolom alasan; verdict `PASS` / `PASS+KARANTINA` / `FAIL` di `outputs/dq_report.md`.
- Ambang tertulis yang membedakan "kotoran biasa" dari "sumber rusak"; kolom hilang berhenti **sebelum** Postgres dengan pesan yang menyebut kolomnya.
- `raw.*` 9 tabel; baris = sumber − karantina; `load` idempoten dan atomik (gagal di tengah tidak setengah terisi); `outputs/load_report.md`.
- Kode pos teks 5 digit di tiga tabel; aturan kualitas punya unit test **tanpa Docker** (rancang aturan sebagai klausa SQL/fungsi yang bisa diuji tanpa cluster Spark).

### T3 — stream (Kafka wajib)
- Producer: ±3 % dobel, ±2 % terlambat, ±0,5 % rusak, key = `order_id`, jumlah gangguan dicatat ke kunci jawaban.
- Consumer: consumer group, DLQ ke topic **dan** tabel, `stream.order_events` (satu baris per event unik), `stream.order_current` (satu baris per order, status hanya naik), metrik per batch, freshness p50/p95.
- Event unik tersimpan = event unik dikirim; status terkini = `raw.orders.order_status` (100 %); consumer dimatikan paksa lalu jalan lagi tetap benar.

### T4 — star schema
- Sembilan tabel: `dim_date`, `dim_customer` (96.096), `dim_product`, `dim_seller`, `dim_geo` (+ baris `unknown`), `fact_order` (99.441), `fact_order_item` (112.650), `fact_payment`, `fact_review` (satu per order + jumlah asli).
- **PK dan FK terpasang** (bukan hanya didokumentasikan); `is_late` NULL bila belum terkirim, dibandingkan per tanggal; `outputs/dw_report.md` dibangkitkan; test ke database.

### T5 — mart dan dashboard
- Mart: penjualan bulanan, performa pengiriman, scorecard seller, RFM (F bukan kuintil mentah), freshness stream, rekonsiliasi (**≥ 98 % cocok**, sisanya diklasifikasi).
- Ekspor ke gold; dashboard minimal satu visual per pertanyaan + freshness, tiap visual punya **satu kalimat insight berangka**; `outputs/marts_report.md` dari query yang sama dengan dashboard; setiap angka bisa ditelusuri ke fakta.

### T6 — bukti
- Failure injection otomatis ≥ 5 kasus dengan verdict (a kolom hilang, b producer dua kali, c consumer mati sebelum commit offset, d broker Kafka mati, e jalur batch diulang); `EVALUATION_EVIDENCE.md` dan laporan **dibangkitkan dari file hasil**; `RUNBOOK.md` (≥ 6 symptom); `pytest` hijau; `down -v` sampai dashboard ≤ 15 menit dan ≤ 10 perintah.

### Prinsip lintas tahap
Satu pintu perintah per tahap; kontrak/skema dari **satu struktur data**; SQL di file; setiap angka di dokumen berasal dari `outputs/` yang dihasilkan kode; tidak ada "pembersihan diam-diam".

## 2. Alat yang BOLEH berbeda (dan dampaknya)

| Komponen | Tugas | v2 | Catatan |
|---|---|---|---|
| Dashboard | Power BI atau notebook | **Metabase** (notebook opsional untuk eksplorasi) | karena tidak dinilai, Metabase boleh. Syarat T5 tetap berlaku sebagai ketentuan isi: satu visual per pertanyaan, insight berangka, angka bisa ditelusuri. Agar `marts_report.md` dan dashboard memakai query yang sama, **tentukan query di dbt (mart), tampilkan di Metabase** |
| dbt | "tulis sendiri" untuk T0–T6 | **dipakai penuh** untuk dw dan mart | PK/FK dipasang lewat *model contracts* dengan `constraints` (bukan bawaan dbt); buktikan dengan query yang mengembalikan 0 |
| Airflow | bonus B3 | orkestrator utama | tiap tahap tetap punya perintah mandiri (`make bronze`, …) yang jalan tanpa UI Airflow |
| Spark | bonus B1 | silver (dan mungkin gold) | tetap sertakan perbandingan durasi vs DuckDB/pandas dan catatan kapan Spark tidak layak pada 126 MB |
| Kafka, MinIO, Postgres, Jupyter | stack wajib | sama | Kafka tidak boleh dianggap opsional: perilaku T3 harus utuh |

## 3. Risiko teknis yang tetap perlu dijaga
- **RAM Docker 7,8 GB:** nyalakan lewat profil, ukur dengan `docker stats`.
- **Batas 15 menit dari `down -v`:** build tiga image kustom (Airflow + Java, Spark, Jupyter) bisa memakan waktu; ukur termasuk build.
- **Unit test tanpa Docker:** rancang aturan kualitas sehingga tidak butuh Spark untuk diuji.
- **Versi pin belum terverifikasi:** `dbt-core 1.8.9`, `dbt-postgres 1.8.2`, tag image Spark, konektor S3A.

## 4. Naik kelas ke production-grade
Rincian dan prioritasnya ada di [PRODUCTION-GRADE.md](PRODUCTION-GRADE.md).
