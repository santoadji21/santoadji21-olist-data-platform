"""T6 — EVALUATION_EVIDENCE.md dibangkitkan dari outputs/*.json (`make report`). Tidak ada angka yang diketik tangan:
semua besaran dibaca dari file hasil tiap tahap; yang diketik hanya narasi keputusan/limitation yang tidak memuat angka.
"""
from __future__ import annotations

import json
import platform
import subprocess
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FILES = dict(run_all="run_all.json", bronze="bronze.json", dq="dq.json", gate="contract_gate.json", load="load.json", stream="stream.json",
             dw="dw.json", marts="marts.json", fail="failures.json")


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


def build(root: Path = ROOT) -> str:
    J = {k: _load(root, f) for k, f in FILES.items()}
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")
    o = ["# EVALUATION_EVIDENCE — olist-v2\n",
         f"> Dibangkitkan oleh `make report` pada {now} dari `outputs/*.json`. Jangan edit angka di sini dengan tangan.\n"]

    # 1. lingkungan & durasi
    o += ["## 1. Lingkungan & durasi tiap tahap\n",
          f"- Mesin: {platform.platform()}, {platform.machine()}; Python {platform.python_version()}; {_docker_version()}",
          "- Stack: PostgreSQL 16, MinIO, Kafka 3.9 (satu broker, KRaft), Airflow 2.10 (LocalExecutor), Spark (lokal di scheduler), dbt, Metabase, datacontract (ODCS)\n"]
    ra = J["run_all"]
    if ra:
        mode = "setelah `make reset` (down -v; CSV di data/raw dipertahankan)" if ra["fresh"] else "tanpa reset (stack dibiarkan)"
        o += [f"Run `make all` dimulai {ra['started_at']}, {mode}: **{_n(ra['total_seconds'], 1)} detik ({_n(ra['total_seconds'] / 60, 1)} menit)** "
              f"untuk {ra['commands']} perintah (batas tugas: ≤ 15 menit, ≤ 10 perintah) — {'SELESAI' if ra['ok'] else 'GAGAL'}.\n",
              "| Tahap | Perintah | Durasi (detik) | Exit |", "|---|---|---:|---:|"]
        o += [f"| {s['stage']} | `{s['command']}` | {_n(s['seconds'], 1)} | {s['exit']} |" for s in ra["stages"]]
    else:
        o.append("_Belum ada `outputs/run_all.json` — jalankan `make all` (atau `make all YES=1` dari nol)._")

    # 2. data & kualitas
    o += ["\n## 2. Data & kualitas (T0–T2)\n"]
    if J["bronze"]:
        b = J["bronze"]
        o += ["- Bronze (run terakhir): " + ", ".join(f"{s} = {sum(1 for x in b['files'] if x['status'] == s)}" for s in sorted({x['status'] for x in b['files']}))
              + f" dari {len(b['files'])} file; ingest {_n(b['seconds'], 2)} s.", "",
              "| Tabel | Baris | Ukuran (MB) | Versi | sha256 (12) |", "|---|---:|---:|---:|---|"]
        o += [f"| {x['table']} | {_n(x['rows'])} | {_n(x['size_bytes'] / 1e6, 1)} | {x['version']} | {x['sha256'][:12]} |" for x in b["files"]]
    if J["dq"]:
        o += ["", f"Gate kualitas silver (`outputs/dq.json`, mesin {J['dq']['engine']}, {_n(J['dq']['seconds'], 1)} s):", "",
              "| Tabel | Sumber | Karantina | Silver | Verdict |", "|---|---:|---:|---:|---|"]
        o += [f"| {t['table']} | {_n(t['source_rows'])} | {_n(t['quarantined'])} | {_n(t['clean'])} | {t['verdict']} |" for t in J["dq"]["tables"]]
        rules = [r for t in J["dq"]["tables"] for r in t["rules"]]
        hard, soft = [r for r in rules if r["kind"] == "hard"], [r for r in rules if r["kind"] == "soft"]
        o.append(f"\n{len(hard)} aturan hard dan {len(soft)} aturan soft (minimal 10 dan 8); {_n(sum(r['violations'] for r in soft))} pelanggaran soft "
                 f"dicatat dan dipertahankan dengan flag; {_n(sum(r['violations'] for r in hard))} pelanggaran hard (karantina).")
    if J["gate"]:
        g = J["gate"]
        o.append(f"- Gate kontrak ODCS (`datacontract test`): **{g['result']}** — {_n(g['checks'])} pemeriksaan, {_n(g['passed'])} lolos, "
                 f"{len(g['failed'])} gagal, {len(g['soft_violations'])} peringatan soft, {_n(g['unsupported_type_checks'])} pemeriksaan tipe belum didukung "
                 "datacontract-cli untuk Parquet (keterbatasan alat, tidak menghentikan DAG).")
    if J["load"]:
        o.append("- Load `raw.*`: " + ", ".join(f"{t['table']} {_n(t['loaded'])}" for t in J["load"]["tables"])
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
              f"- Status terkini = `raw.orders.order_status`: **{_n(c['status_match'])} dari {_n(c['status_total'])}**; pemeriksaan producer vs consumer lulus {sum(k.values())}/{len(k)}.",
              f"- Ujung ke ujung {_n(c['end_to_end_seconds'], 1)} s (batas 5 menit).",
              f"- Freshness: p50 = {c['fresh_p50_ms']} ms, p95 = {c['fresh_p95_ms']} ms, maks = {c['fresh_max_ms']} ms."]
    else:
        o.append("_Belum ada `outputs/stream.json` — jalankan `make stream`._")

    # 4. dw & mart
    o += ["\n## 4. Warehouse & mart (T4–T5)\n"]
    if J["dw"]:
        d, n = J["dw"], J["dw"]["key_numbers"]
        o += ["| Tabel dw | Baris |", "|---|---:|"] + [f"| {t} | {_n(r)} |" for t, r in d["rows"].items()]
        ok = sum(1 for c in d["checks"] if c["bad_rows"] == 0)
        o += [f"\n- Integritas: **{ok}/{len(d['checks'])} pemeriksaan = 0 baris bermasalah**; constraint terpasang di database: {n['primary_keys']} PK dan {n['foreign_keys']} FK.",
              f"- Orang unik {_n(n['unique_people'])}; order tanpa item {_n(n['orders_without_items'])}, tanpa pembayaran {_n(n['orders_without_payment'])}; "
              f"order delivered {_n(n['delivered'])}; late rate {n['late_rate_pct']} % dari {_n(n['delivered_with_date'])}; bulan lengkap {n['complete_months']}."]
    if J["marts"]:
        m = J["marts"]
        o += ["", "Jawaban ringkas (kalimat dibangkitkan dari mart; sama dengan dashboard Metabase, lihat `outputs/marts_report.md`):", ""]
        o += [f"- **{k}** — {v}" for k, v in m["insights"].items() if v]
        ok = sum(1 for c in m["checks"] if c["bad_rows"] == 0)
        o += ["", "Mart di gold: " + ", ".join(f"{e['table']} {_n(e['rows'])}" for e in m["gold_export"]) + f"; {ok}/{len(m['checks'])} pemeriksaan mart = 0.",
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
            o.append(f"\nProducer naif (future `send()` tidak diperiksa) saat broker mati {e.get('long_outage_s')} s: "
                     f"{_n(e.get('naive_silent_loss', 0))} event hilang tanpa error dari {_n(e.get('expected_unique_events', 0))}.")
        r = J["fail"].get("restore")
        if r:
            o.append(f"\nPemulihan stack setelah semua gangguan: {'OK' if r['ok'] else 'GAGAL'}; smoke {sum(x['ok'] for x in r['smoke'])}/{len(r['smoke'])}.")
    else:
        o.append("_Belum ada `outputs/failures.json` — jalankan `make inject`._")

    # 6-8 narasi (tanpa angka)
    o += ["\n## 6. Keputusan desain\n", "| Keputusan | Alternatif yang ditolak | Alasan |", "|---|---|---|",
          "| Bronze content-addressed (`sha256` di kunci objek) + manifest append-only di bucket | kunci berbasis tanggal; manifest lokal | upload kedua di hari yang sama tidak menimpa; manifest ikut bersama bucket; versi lama tetap ada |",
          "| Aturan kualitas satu klausa `WHERE` dari kontrak ODCS, dipakai Spark, datacontract, dan uji DuckDB | tiga implementasi aturan | tidak bisa melenceng antara kontrak, silver, dan uji |",
          "| Aturan yang diketahui kotor dijadikan soft (simpan + flag) | karantina semua pelanggaran | temuan kotor yang sudah dipahami tidak boleh menghentikan pipeline atau menghapus data |",
          "| `load` = satu transaksi TRUNCATE + COPY untuk semua tabel | INSERT per baris; transaksi per tabel | atomik dan cepat; gagal di tengah tidak meninggalkan tabel setengah terisi |",
          "| `event_id` deterministik dan status hanya naik | dedup berdasar `order_id`; urutan tiba menentukan status | duplikat dan event terlambat aman tanpa state tambahan; `canceled` terminal |",
          "| Commit urutan Postgres → DLQ → offset Kafka | auto-commit offset; offset dulu | crash di antaranya hanya menghasilkan duplikat yang diabaikan, tidak ada kehilangan |",
          "| dw dan mart sebagai model dbt, PK/FK lewat `post_hook` pada Postgres sungguhan | constraint dbt yang tidak ditegakkan; dokumentasi saja | PK/FK benar-benar terpasang dan dibuktikan oleh uji dan kueri integritas |",
          "| Mart dan dashboard memakai satu daftar kueri (`questions.py`) | kueri terpisah di Metabase dan laporan | angka laporan dan grafik tidak bisa berbeda; insight dihitung dari hasil kueri yang sama |",
          "| `is_late` NULL untuk order tanpa tanggal tiba, dibandingkan per tanggal | `false`; timestamp vs tanggal | late rate tidak terlihat lebih bagus dari kenyataan |",
          "| F pada RFM memakai pita tetap | kuintil mentah | hampir semua orang hanya belanja sekali, kuintil memecah nilai sama secara acak |",
          "| Log Airflow di bind mount, bukan named volume | named volume | log task lengkap terbaca; named volume memotongnya (terbukti lewat uji A/B) |"]
    lim = ["Satu broker Kafka tanpa replikasi dan satu proses consumer: lag tumbuh selama replay penuh; broker mati lama berarti replay berhenti sampai broker hidup lagi.",
           "Spark berjalan lokal di dalam scheduler Airflow (`local[2]`), bukan di cluster: cukup untuk skala ini, tetapi memori scheduler menjadi batas.",
           "`datacontract-cli` belum bisa memeriksa tipe pada Parquet; pemeriksaan tipe ditutup oleh pemetaan tipe Spark dan uji DuckDB, bukan oleh gate itu sendiri.",
           "dw dan mart dibangun ulang penuh dari `raw` pada tiap `dbt build`: waktu build tumbuh linear dengan data, dan tidak ada refresh inkremental.",
           "Atribusi seller memakai level order: order multi-seller dihitung ke tiap seller.",
           "Kredensial dev ada di `.env` tanpa rotasi atau secret manager, dan layanan tanpa TLS.",
           "Failure test memakai satu skenario per kasus; broker mati dengan durasi sangat panjang atau disk penuh belum diuji."]
    exp = ["Tambah consumer satu per partisi dan ukur apakah p95 freshness pada replay penuh turun tanpa lag yang tumbuh.",
           "Bandingkan durasi silver: Spark lokal vs DuckDB/PyArrow pada 126 MB dan pada data 10× (catat kapan Spark mulai layak).",
           "Ubah `fact_order` dan mart harian menjadi inkremental dan bandingkan durasi dengan rebuild penuh.",
           "Pindahkan tabel silver ke Apache Iceberg (time travel, evolusi skema) dan bandingkan dengan Parquet biasa."]
    o += ["\n## 7. Limitation\n"] + [f"{i}. {t}" for i, t in enumerate(lim, 1)]
    o += ["\n## 8. Next experiment\n"] + [f"{i}. {t}" for i, t in enumerate(exp, 1)]
    o += ["\n## 9. Reproduksi (perintah persis)\n", "```bash",
          "cd olist-v2",
          "make venv                          # venv lokal (dbt, pytest, psycopg2, kafka-python) — sekali",
          "# letakkan kredensial Kaggle di .kaggle/kaggle.json (tidak ikut git)",
          "make all YES=1                     # reset (down -v) lalu: up → download → bronze → silver → gate → load → stream → dbt-build → marts → dashboard",
          "make inject                        # failure injection → outputs/failure_tests.md",
          "make smoke                         # pemeriksaan kesehatan seluruh lapisan",
          "make report                        # EVALUATION_EVIDENCE.md",
          "make test                          # pytest tanpa Docker", "```"]
    return "\n".join(o) + "\n"


def main() -> int:
    p = ROOT / "EVALUATION_EVIDENCE.md"
    p.write_text(build(ROOT), encoding="utf-8")
    print(f"ditulis {p.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
