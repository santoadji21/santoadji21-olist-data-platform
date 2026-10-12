"""T6 — EVALUATION_EVIDENCE.md dibangkitkan dari outputs/*.json. Tidak ada angka yang diketik tangan:
semua besaran dibaca dari file hasil tiap tahap; yang diketik hanya narasi keputusan/limitation yang tidak memuat angka.
"""
from __future__ import annotations

import json
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def _load(root: Path, name: str):
    p = root / "outputs" / name
    return json.loads(p.read_text(encoding="utf-8")) if p.exists() else None


def _n(x, d=0):
    return f"{float(x):,.{d}f}"


def _docker_version() -> str:
    try:
        return subprocess.run(["docker", "--version"], capture_output=True, text=True, timeout=10).stdout.strip()
    except Exception:
        return "docker: tidak terbaca"


def build(root: Path) -> str:
    J = {k: _load(root, f) for k, f in dict(
        run_all="run_all.json", profile="profile.json", bronze="bronze.json", dq="dq.json", load="load.json", stream="stream.json",
        truth="stream_truth.json", dw="dw.json", marts="marts.json", fail="failures.json").items()}
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    o = [f"# EVALUATION_EVIDENCE — Olist Data Platform\n",
         f"> Dibangkitkan oleh `python pipeline.py report` pada {now} dari `outputs/*.json`. Jangan edit angka di sini dengan tangan.\n"]

    # 1. lingkungan & durasi
    o += ["## 1. Lingkungan & durasi tiap tahap\n",
          f"- Mesin: {platform.platform()}, {platform.machine()}; Python {platform.python_version()}; {_docker_version()}",
          "- Stack: PostgreSQL 16, Kafka 3.9 (satu broker, KRaft), MinIO, Jupyter (satu docker compose)\n"]
    ra = J["run_all"]
    if ra:
        o += [f"Run bersih `python pipeline.py all` (dimulai {ra['started_at']}); `docker compose down -v` dilakukan sebelumnya: "
              f"**{_n(ra['total_seconds'], 1)} detik total ({_n(ra['total_seconds'] / 60, 1)} menit)** untuk {len(ra['stages'])} perintah "
              f"(batas tugas: ≤ 15 menit, ≤ 10 perintah).\n",
              "| Tahap | Perintah | Durasi (detik) | Exit |", "|---|---|---:|---:|"]
        o += [f"| {s['stage']} | `{s['command']}` | {_n(s['seconds'], 1)} | {s['exit']} |" for s in ra["stages"]]
    else:
        o.append("_Belum ada `outputs/run_all.json` — jalankan `python pipeline.py all` pada stack bersih._")

    # 2. data & kualitas
    o += ["\n## 2. Data & kualitas (T0–T2)\n"]
    if J["profile"]:
        f = J["profile"]["findings"]
        by = {}
        for x in f:
            by[x["severity"]] = by.get(x["severity"], 0) + 1
        o.append(f"- Profil: {len(J['profile']['tables'])} tabel, **{len(f)} temuan kotor** (≥ 10 diminta); keputusan: "
                 + ", ".join(f"{k} = {v}" for k, v in sorted(by.items())) + ".")
    if J["bronze"]:
        b = J["bronze"]
        o += [f"- Bronze (run terakhir): " + ", ".join(f"{s} = {sum(1 for x in b['files'] if x['status'] == s)}" for s in sorted({x['status'] for x in b['files']}))
              + f" dari {len(b['files'])} file; ingest {_n(b['seconds'], 2)} s.", "",
              "| Tabel | Baris bronze | Ukuran (MB) | Versi |", "|---|---:|---:|---:|"]
        o += [f"| {x['table']} | {_n(x['rows'])} | {_n(x['size_bytes'] / 1e6, 1)} | {x['version']} |" for x in b["files"]]
    if J["dq"]:
        o += ["", "Gate kualitas silver (hasil `outputs/dq.json`):", "",
              "| Tabel | Sumber | Karantina | Silver | Verdict |", "|---|---:|---:|---:|---|"]
        o += [f"| {t['table']} | {_n(t['source_rows'])} | {_n(t['quarantined'])} | {_n(t['clean'])} | {t['verdict']} |" for t in J["dq"]["tables"]]
        hard = sum(1 for t in J["dq"]["tables"] for r in t["rules"] if r["kind"] == "hard")
        soft = sum(1 for t in J["dq"]["tables"] for r in t["rules"] if r["kind"] == "soft")
        soft_hits = sum(r["violations"] for t in J["dq"]["tables"] for r in t["rules"] if r["kind"] == "soft")
        o.append(f"\n{hard} aturan hard dan {soft} aturan soft (minimal 10 dan 8); {_n(soft_hits)} pelanggaran soft dicatat dan dipertahankan dengan flag.")
    if J["load"]:
        o.append(f"- Load `raw.*`: " + ", ".join(f"{t['table']} {_n(t['loaded'])}" for t in J["load"]["tables"])
                 + f"; {_n(J['load']['seconds'], 1)} s, semua cocok dengan sumber − karantina: {all(t['match'] for t in J['load']['tables'])}.")

    # 3. stream
    o += ["\n## 3. Stream (T3)\n"]
    if J["stream"]:
        s, c, k = J["stream"]["sent"], J["stream"]["consumer"], J["stream"]["checks"]
        o += ["| Besaran | Producer | Consumer |", "|---|---:|---:|",
              f"| Order | {_n(s['orders'])} | {_n(c['order_current_rows'])} (`order_current`) |",
              f"| Event unik | {_n(s['events_unique'])} | {_n(c['events_stored'])} |",
              f"| Duplikat | {_n(s['duplicates'])} | {_n(c['duplicates_derived'])} diabaikan |",
              f"| Pesan rusak | {_n(s['corrupt'])} | {_n(c['dlq_rows'])} di DLQ |",
              f"| Event ditahan (terlambat) | {_n(s['late'])} | {_n(c['out_of_order'])} out-of-order tercatat |",
              f"| Pesan gagal terkirim | {_n(s['send_failures'])} | — |", "",
              f"- Status terkini = `raw.orders.order_status`: **{_n(c['status_match'])} dari {_n(c['status_total'])}**; pemeriksaan "
              f"producer vs consumer lulus {sum(k.values())}/{len(k)}.",
              f"- Replay (producer): {_n(s['producer_seconds'], 1)} s; ujung ke ujung {_n(c['end_to_end_seconds'], 1)} s (batas 5 menit).",
              f"- Freshness: p50 = {c['fresh_p50_ms']} ms, p95 = {c['fresh_p95_ms']} ms, maks = {c['fresh_max_ms']} ms."]

    # 4. dw & mart
    o += ["\n## 4. Warehouse & mart (T4–T5)\n"]
    if J["dw"]:
        d = J["dw"]
        o += ["| Tabel dw | Baris |", "|---|---:|"] + [f"| {t} | {_n(r)} |" for t, r in d["rows"].items()]
        ok = sum(1 for c in d["checks"] if c["bad_rows"] == 0)
        n = d["key_numbers"]
        o += [f"\n- Integritas: **{ok}/{len(d['checks'])} pemeriksaan = 0 baris bermasalah**; build {_n(d['seconds'], 1)} s.",
              f"- Orang unik {_n(n['unique_people'])}; order tanpa item {_n(n['orders_without_items'])}, tanpa pembayaran {_n(n['orders_without_payment'])}; "
              f"order delivered {_n(n['delivered'])}; late rate {n['late_rate_pct']} % dari {_n(n['delivered_with_date'])}; bulan lengkap {n['complete_months']}."]
    if J["marts"]:
        m = J["marts"]
        o += ["", "Jawaban ringkas (kalimat dibangkitkan dari mart, lihat `outputs/marts_report.md`):", ""]
        o += [f"- **{k.upper()}** — {v}" for k, v in m["insights"].items() if k in ("q1", "q2", "q3", "q4")]
        ok = sum(1 for c in m["checks"] if c["bad_rows"] == 0)
        o += [f"\nMart: " + ", ".join(f"{t} {_n(r)}" for t, r in m["rows"].items()) + f"; {ok}/{len(m['checks'])} pemeriksaan = 0; build {_n(m['seconds'], 1)} s.",
              "", "Rekonsiliasi nilai pesanan vs dibayar (toleransi 0,01 BRL):", "",
              "| Kelas | Order | % | Selisih absolut (BRL) |", "|---|---:|---:|---:|"]
        o += [f"| {r['class']} | {_n(r['orders'])} | {r['pct']} | {_n(r['abs_diff_brl'], 2)} |" for r in m["answers"]["recon"]]

    # 5. failure
    o += ["\n## 5. Failure injection (T6)\n"]
    if J["fail"]:
        cs = J["fail"]["cases"]
        o += [f"**{sum(c['verdict'] == 'TERTANGKAP' for c in cs)}/{len(cs)} kasus TERTANGKAP**; pulih: {sum(c['recovered'] for c in cs)}/{len(cs)}. "
              "Detail dan bukti angka: `outputs/failure_tests.md`.", "", "| Kasus | Verdict | Symptom |", "|---|---|---|"]
        o += [f"| ({c['id']}) {c['name']} | {c['verdict']} | {c['symptom']} |" for c in cs]
        d = next((c for c in cs if c["id"] == "d"), None)
        if d and d["evidence"]:
            e = d["evidence"]
            o.append(f"\nProducer naif (future `send()` tidak diperiksa) saat broker mati {e.get('naive_broker_down_s')} s: "
                     f"{_n(e.get('naive_silent_loss', 0))} event hilang tanpa error dari {_n(e.get('expected_unique_events', 0))}.")
        r = J["fail"].get("restore")
        if r:
            o.append(f"\nPemulihan stack setelah semua gangguan: {'OK' if r['ok'] else 'GAGAL'}; smoke {sum(x['ok'] for x in r['smoke'])}/{len(r['smoke'])}.")
    else:
        o.append("_Belum ada `outputs/failures.json` — jalankan `python pipeline.py inject`._")

    # 6. keputusan desain
    o += ["\n## 6. Keputusan desain\n", "| Keputusan | Alternatif yang ditolak | Alasan |", "|---|---|---|",
          "| Bronze content-addressed (`sha256` di kunci objek) + manifest append-only di bucket | kunci berbasis tanggal; manifest lokal | upload kedua di hari yang sama tidak menimpa; manifest ikut bersama bucket; versi lama tetap ada |",
          "| Satu klausa `WHERE` untuk aturan kualitas, dipakai kontrak ODCS dan gate (DuckDB) | dua implementasi aturan | tidak bisa melenceng antara kontrak dan gate |",
          "| Aturan yang diketahui kotor dijadikan soft (simpan + flag) | karantina semua pelanggaran | temuan kotor yang sudah dipahami tidak boleh menghentikan pipeline atau menghapus data |",
          "| `load` = satu transaksi TRUNCATE + COPY untuk semua tabel | INSERT per baris; transaksi per tabel | atomik dan cepat; gagal di tengah tidak meninggalkan tabel setengah terisi |",
          "| `event_id` deterministik dan status hanya naik | dedup berdasar `order_id`; urutan tiba menentukan status | duplikat dan event terlambat aman tanpa state tambahan; `canceled` terminal |",
          "| Commit urutan Postgres → DLQ → offset Kafka | auto-commit offset; offset dulu | crash di antaranya hanya menghasilkan duplikat yang diabaikan, tidak ada kehilangan |",
          "| `dw` rebuild penuh dalam satu transaksi | update inkremental | murni turunan raw; gagal di tengah rollback; tidak ada logika inkremental yang bisa melenceng |",
          "| `dim_customer` per orang (SCD tipe 1) | per `customer_id`; SCD tipe 2 | `customer_id` menghitung order, bukan orang; yang hilang hanya riwayat alamat |",
          "| `is_late` NULL untuk order tanpa tanggal tiba, dibandingkan per tanggal | `false`; timestamp vs tanggal | late rate tidak terlihat lebih bagus dari kenyataan |",
          "| F pada RFM memakai pita tetap | kuintil mentah | hampir semua orang hanya belanja sekali, kuintil memecah nilai sama secara acak |",
          "| Dashboard notebook membaca `mart.*` | Power BI; logika di notebook | logika bisnis di SQL yang bisa diuji; laporan dan grafik memakai query yang sama |"]

    # 7-8 limitation & experiments (angka dari file bila ada)
    lim = []
    if J["stream"]:
        c = J["stream"]["consumer"]
        lim.append(f"Satu broker Kafka tanpa replikasi (`replication.factor=1`) dan satu proses consumer: lag tumbuh selama replay "
                   f"(p95 = {c['fresh_p95_ms']} ms pada replay penuh); broker mati lama berarti replay berhenti.")
    lim += ["Silver membaca satu CSV utuh ke memori (geolokasi ±1 juta baris) lalu menjalankan aturan lewat DuckDB: aman pada skala ini, tetapi memori tumbuh linear dengan ukuran file.",
            "`dw` dan `mart` selalu rebuild penuh dari `raw`: waktu build tumbuh linear dengan data, dan tidak ada refresh per-hari.",
            "Atribusi seller memakai order-level (`is_late`, review): order multi-seller dihitung ke tiap seller, jadi skor seller bercampur dengan seller lain di order yang sama.",
            "Kredensial dev (`minio`, `olist`) ada di `.env` tanpa rotasi atau secret manager, dan MinIO tanpa web console atau TLS.",
            "Failure test memakai satu skenario per kasus (broker mati ±6 detik, satu byte diubah, satu batch crash); outage yang lebih panjang dari timeout request producer belum diuji."]
    o += ["\n## 7. Limitation\n"] + [f"{i}. {t}" for i, t in enumerate(lim, 1)]
    exp = ["Tambah consumer satu per partisi (3 partisi) dan ukur apakah p95 freshness pada replay penuh turun di bawah 1 detik tanpa lag yang tumbuh.",
           "Ganti pembaca silver dengan DuckDB/PyArrow yang membaca CSV per-chunk dan ukur memori puncak serta durasi pada data 10× (replikasi file).",
           "Ubah `fact_order` dan mart harian menjadi inkremental (hanya partisi tanggal yang berubah) dan bandingkan durasi build dengan rebuild penuh.",
           "Uji outage broker 60 detik (lebih lama dari `request_timeout_ms`) dan ukur berapa pesan yang menyerah serta apakah kirim ulang idempoten memulihkannya."]
    o += ["\n## 8. Next experiment\n"] + [f"{i}. {t}" for i, t in enumerate(exp, 1)]
    o += ["\n## 9. Reproduksi (perintah persis)\n", "```bash",
          "git clone <repo> && cd olist-data-platform",
          "python -m venv .venv && .venv/bin/pip install -r requirements.txt   # atau: uv pip install -r requirements.txt",
          "cp .env.example .env                       # atau: python pipeline.py init",
          "python scripts/get_dataset.py              # unduh 9 CSV ke data/raw (Kaggle, cadangan mirror)",
          "docker compose down -v                     # mulai dari nol",
          "python pipeline.py all                     # init → up → check → profile → bronze → silver → load → stream → model → marts",
          "python pipeline.py inject                  # failure injection → outputs/failure_tests.md",
          "python pipeline.py report                  # EVALUATION_EVIDENCE.md",
          "python pipeline.py smoke                   # pemeriksaan kesehatan seluruh lapisan",
          "pytest -q", "```"]
    return "\n".join(o) + "\n"


def write(root: Path) -> Path:
    p = root / "EVALUATION_EVIDENCE.md"
    p.write_text(build(root), encoding="utf-8")
    return p
