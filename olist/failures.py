"""T6 — Failure injection otomatis. Tiap kasus = fungsi yang MENGEMBALIKAN hasil (dict), bukan mencetak.

Hasil: {id, name, broken, symptom, verdict ∈ TERTANGKAP|LOLOS|ERROR, mechanism, recovery, recovered, evidence, seconds}.
Pelapor (`report_md`) terpisah. Semua gangguan dijalankan terhadap stack yang sungguh-sungguh hidup:
proses dimatikan lewat exit code (bukan exception), broker Kafka dihentikan lewat `docker stop`, file sumber
dirusak lewat salinan sementara (data/raw asli tidak pernah diubah).
"""
from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import psycopg

from .bronze import make_s3, read_manifest
from .load import pg_dsn
from .schema import TABLES

STREAM_N = 5000          # jumlah order untuk kasus stream (cukup untuk banyak batch, cepat)
STREAM_IDLE = "8"


@dataclass
class Ctx:
    root: Path
    env: dict
    log: Callable = print

    def conn(self):
        return psycopg.connect(pg_dsn(self.env), connect_timeout=5)

    def s3(self):
        return make_s3(self.env)


def cli(ctx: Ctx, *args: str, env: dict | None = None, timeout: int = 1500) -> dict:
    t0 = time.time()
    p = subprocess.run([sys.executable, str(ctx.root / "pipeline.py"), *args], cwd=ctx.root, capture_output=True,
                       text=True, env={**os.environ, **(env or {})}, timeout=timeout)
    return {"rc": p.returncode, "out": (p.stdout + p.stderr).strip(), "seconds": round(time.time() - t0, 1)}


def line_with(out: str, needle: str) -> str:
    return next((ln.strip() for ln in out.splitlines() if needle in ln), out.splitlines()[-1].strip() if out else "")


def scalar(ctx: Ctx, sql: str):
    with ctx.conn() as c:
        return c.execute(sql).fetchone()[0]


def stream_state(ctx: Ctx) -> dict:
    from .stream_report import collect
    with ctx.conn() as c:
        return collect(ctx.env, c, ctx.root / "outputs" / "stream_truth.json")


def result(case_id, name, broken, symptom, caught: bool, mechanism, recovery, recovered: bool, evidence: dict, t0) -> dict:
    return {"id": case_id, "name": name, "broken": broken, "symptom": symptom,
            "verdict": "TERTANGKAP" if caught else "LOLOS", "mechanism": mechanism, "recovery": recovery,
            "recovered": bool(recovered), "evidence": evidence, "seconds": round(time.time() - t0, 1)}


# ---- (a) kolom hilang di sumber -------------------------------------------------------------------
def case_missing_column(ctx: Ctx) -> dict:
    t0 = time.time()
    s3 = ctx.s3()
    from .silver import DQ_KEY, silver_key
    raw = ctx.root / ctx.env.get("RAW_DIR", "data/raw")
    customers = next(t for t in TABLES if t.name == "customers")
    dropped = "customer_unique_id"

    def snap():
        return {"raw_rows": scalar(ctx, "SELECT count(*) FROM raw.customers"),
                "silver_mtime": s3.head_object(Bucket="silver", Key=silver_key("customers"))["LastModified"].isoformat(),
                "dq_etag": s3.head_object(Bucket="silver", Key=DQ_KEY)["ETag"]}

    before = snap()
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        for t in TABLES:
            if t.name != "customers":
                os.symlink(raw / t.file, tmp / t.file)
        with open(raw / customers.file, newline="", encoding="utf-8") as f, \
                open(tmp / customers.file, "w", newline="", encoding="utf-8") as g:
            rd, wr = csv.reader(f), csv.writer(g)
            header = next(rd)
            drop = header.index(dropped)
            wr.writerow([h for i, h in enumerate(header) if i != drop])
            for row in rd:
                wr.writerow([v for i, v in enumerate(row) if i != drop])
        b = cli(ctx, "bronze", env={"RAW_DIR": str(tmp)})      # bronze mengunggah apa adanya (tidak memvalidasi)
        s = cli(ctx, "silver")
    after = snap()
    caught = (s["rc"] != 0 and dropped in s["out"] and before == after)
    # pemulihan: ingest file asli lagi (versi baru, riwayat rusak tetap tercatat) → silver → load
    r1, r2, r3 = cli(ctx, "bronze"), cli(ctx, "silver"), cli(ctx, "load")
    recovered = r1["rc"] == 0 and r2["rc"] == 0 and r3["rc"] == 0 and scalar(ctx, "SELECT count(*) FROM raw.customers") == before["raw_rows"]
    return result(
        "a", "Kolom hilang di sumber",
        f"kolom `{dropped}` dihapus dari salinan CSV customers, lalu `bronze` dan `silver` dijalankan",
        line_with(s["out"], "silver berhenti") + f" (exit {s['rc']})",
        caught,
        "`check_header` di olist/gate.py membandingkan header dengan kontrak SEBELUM menulis apa pun; `run_silver` berhenti sebelum Postgres disentuh",
        "ingest ulang CSV asli (bronze v baru; versi rusak tetap tercatat di manifest), lalu `silver` dan `load`",
        recovered,
        {"bronze_exit": b["rc"], "silver_exit": s["rc"], "raw_customers_before": before["raw_rows"], "raw_customers_after": after["raw_rows"],
         "silver_parquet_unchanged": before["silver_mtime"] == after["silver_mtime"], "dq_report_unchanged": before["dq_etag"] == after["dq_etag"]}, t0)


# ---- (b) producer dijalankan dua kali ---------------------------------------------------------------
def case_producer_twice(ctx: Ctx) -> dict:
    t0 = time.time()
    cli(ctx, "stream", "reset")
    p1 = cli(ctx, "stream", "produce", "--limit", str(STREAM_N))
    p2 = cli(ctx, "stream", "produce", "--limit", str(STREAM_N))
    c = cli(ctx, "stream", "consume", "--idle-exit", STREAM_IDLE)
    st = stream_state(ctx)
    k, s = st["consumer"], st["sent"]
    caught = (p1["rc"] == 0 and p2["rc"] == 0 and all(st["checks"].values()) and s["runs"] == 2
              and k["events_stored"] == s["events_unique"])
    return result(
        "b", "Producer dijalankan dua kali",
        f"`stream produce --limit {STREAM_N}` dijalankan dua kali berturut-turut tanpa reset (semua event dikirim ulang)",
        f"{s['messages_total']:,} pesan di topic, tetapi event tersimpan tetap {k['events_stored']:,} (= event unik); "
        f"{k['duplicates_derived']:,} duplikat diabaikan",
        caught,
        "`event_id` deterministik `<order_id>:<status>` = PK `stream.order_events`; `INSERT … ON CONFLICT DO NOTHING` mengabaikan duplikat; "
        "upsert `order_current` hanya menaikkan status",
        "tidak perlu pemulihan (idempoten). Baris DLQ ikut berlipat karena pesan rusak run ke-2 punya offset baru; `stream reset` membersihkannya",
        caught,
        {"runs": s["runs"], "events_unique": s["events_unique"], "events_stored": k["events_stored"], "messages_total": s["messages_total"],
         "duplicates_ignored": k["duplicates_derived"], "dlq_rows": k["dlq_rows"], "status_match": f"{k['status_match']}/{k['status_total']}",
         "consumer_exit": c["rc"]}, t0)


# ---- (c) consumer mati sebelum commit offset ----------------------------------------------------------
def case_consumer_crash(ctx: Ctx) -> dict:
    t0 = time.time()
    cli(ctx, "stream", "reset")
    cli(ctx, "stream", "produce", "--limit", str(STREAM_N))
    crash = cli(ctx, "stream", "consume", "--crash-at-batch", "3", "--idle-exit", STREAM_IDLE)   # proses benar-benar mati (os._exit 137)
    stored_at_crash = scalar(ctx, "SELECT count(*) FROM stream.order_events")
    crashed_msgs = scalar(ctx, "SELECT n_messages FROM stream.batch_metrics WHERE batch_no = 3 ORDER BY consumed_at LIMIT 1")
    recov = cli(ctx, "stream", "consume", "--idle-exit", STREAM_IDLE)                              # restart tanpa reset
    st = stream_state(ctx)
    # Redelivery diukur eksak: pesan yang diproses kedua run dikurangi isi topic = pesan yang diproses dua kali.
    # (Urutan partisi setelah restart berbeda, jadi batch yang dikirim ulang tidak harus batch pertama.)
    processed = scalar(ctx, "SELECT sum(n_messages) FROM stream.batch_metrics")
    redelivered = int(processed) - st["consumer"]["messages_on_topic"]
    caught = crash["rc"] == 137 and recov["rc"] == 0 and all(st["checks"].values()) and redelivered >= crashed_msgs > 0
    k = st["consumer"]
    return result(
        "c", "Consumer mati sebelum commit offset",
        "consumer dijalankan sebagai proses terpisah dan dimatikan (exit 137) tepat setelah transaksi Postgres batch ke-3 commit, sebelum offset di-commit",
        f"proses mati exit {crash['rc']} dengan {stored_at_crash:,} event sudah di Postgres; setelah restart {redelivered:,} pesan diproses ulang "
        f"(batch ke-3 berisi {crashed_msgs:,} pesan) namun event tersimpan tetap {k['events_stored']:,} (= event unik)",
        caught,
        "urutan commit di `run_consumer`: Postgres → topic DLQ → offset (`enable_auto_commit=False`); pesan dikirim ulang lalu `ON CONFLICT DO NOTHING` dan upsert `WHERE status_rank <` membuat hasil akhir sama",
        "jalankan consumer lagi tanpa reset; tidak ada langkah manual",
        caught,
        {"crash_exit_code": crash["rc"], "events_stored_at_crash": stored_at_crash, "messages_in_crashed_batch": crashed_msgs,
         "messages_redelivered": redelivered, "events_stored_final": k["events_stored"], "events_unique_sent": st["sent"]["events_unique"],
         "status_match": f"{k['status_match']}/{k['status_total']}", "checks_ok": sum(st["checks"].values())}, t0)


# ---- (d) broker Kafka mati beberapa detik di tengah replay ----------------------------------------------
def _wait_kafka_healthy(timeout: int = 120) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = subprocess.run(["docker", "inspect", "olist-kafka", "--format", "{{.State.Health.Status}}"], capture_output=True, text=True)
        if r.stdout.strip() == "healthy":
            return True
        time.sleep(2)
    return False


def _outage(delay: float, down: float, box: dict):
    time.sleep(delay)
    box["stopped_at"] = time.time()
    subprocess.run(["docker", "stop", "-t", "2", "olist-kafka"], capture_output=True)
    time.sleep(down)
    subprocess.run(["docker", "start", "olist-kafka"], capture_output=True)
    box["healthy_again"] = _wait_kafka_healthy()
    box["down_seconds"] = round(time.time() - box["stopped_at"], 1)


def _producer_during_outage(ctx: Ctx, extra: list[str], delay=2.0, down=6.0) -> tuple[dict, dict]:
    box: dict = {}
    th = threading.Thread(target=_outage, args=(delay, down, box))
    th.start()
    r = cli(ctx, "stream", "produce", "--limit", "8000", "--speed", "3000", *extra)
    th.join()
    return r, box


def _outage_variant(ctx: Ctx, extra: list[str], down: float, expected: int) -> dict:
    """reset → producer saat broker mati `down` detik → (kirim ulang bila run dinyatakan gagal) → consumer → hitung event."""
    cli(ctx, "stream", "reset")
    run, box = _producer_during_outage(ctx, extra, down=down)
    truth_p = ctx.root / "outputs" / "stream_truth.json"
    truth = json.loads(truth_p.read_text()) if truth_p.exists() else {"runs": []}
    failures = truth["runs"][-1]["send_failures"] if truth["runs"] else None
    cli(ctx, "stream", "consume", "--idle-exit", STREAM_IDLE)
    stored_before_redo = scalar(ctx, "SELECT count(DISTINCT event_id) FROM stream.order_events")
    redo = None
    if run["rc"] != 0 or (failures or 0) > 0:   # kegagalan TERDETEKSI → pulihkan dengan mengirim ulang (idempoten)
        redo = cli(ctx, "stream", "produce", "--limit", "8000")
        cli(ctx, "stream", "consume", "--idle-exit", STREAM_IDLE)
    stored = scalar(ctx, "SELECT count(DISTINCT event_id) FROM stream.order_events")
    return {"exit": run["rc"], "send_failures": failures, "down_s": box.get("down_seconds"), "healthy_again": box.get("healthy_again"),
            "stored_before_redo": stored_before_redo, "lost_before_redo": expected - stored_before_redo,
            "redo_exit": None if redo is None else redo["rc"], "stored_final": stored, "out": run["out"]}


def case_broker_outage(ctx: Ctx) -> dict:
    from .producer import all_events, load_orders
    t0 = time.time()
    with ctx.conn() as c:
        expected = len(all_events(load_orders(c, 8000)))
    short = _outage_variant(ctx, [], 6.0, expected)                  # "beberapa detik": producer yang memeriksa hasil send()
    naive = _outage_variant(ctx, ["--unchecked"], 40.0, expected)    # outage > request_timeout (30 s): producer naif
    robust = _outage_variant(ctx, [], 40.0, expected)                # outage panjang: producer yang memeriksa hasil send()
    st = stream_state(ctx)
    k = st["consumer"]
    silent_loss = naive["lost_before_redo"] if naive["exit"] == 0 and not naive["send_failures"] else None
    robust_detected = robust["exit"] != 0 or (robust["send_failures"] or 0) > 0 or robust["lost_before_redo"] == 0
    caught = (short["healthy_again"] is True and short["stored_final"] == expected and robust["stored_final"] == expected
              and robust_detected and k["status_match"] == k["status_total"] == 8000)
    return result(
        "d", "Broker Kafka mati beberapa detik di tengah replay",
        f"`docker stop olist-kafka` ±2 s setelah producer mulai (replay {expected:,} event unik, 3.000 pesan/detik), lalu `docker start` setelah "
        f"{short['down_s']} s (pendek) dan setelah {robust['down_s']} s (panjang, melewati `request_timeout_ms` 30 s)",
        f"outage pendek: producer exit {short['exit']}, kegagalan {short['send_failures']}, {short['stored_final']:,} event tiba. "
        f"Outage panjang: producer naif exit {naive['exit']} tanpa pesan error dan {naive['lost_before_redo']:,} event hilang; "
        f"producer yang memeriksa hasil `send()` exit {robust['exit']} dengan {robust['send_failures']} kegagalan terlapor",
        caught,
        "errback pada tiap future `send()` + `flush()` + `SendFailure` membuat run GAGAL keras bila ada pesan yang menyerah; retry internal (`acks=all`, `retries=5`) menyerap outage pendek",
        "kirim ulang producer (idempoten lewat `event_id`) lalu jalankan consumer; tidak perlu reset",
        bool(caught),
        {"expected_unique_events": expected,
         "short_outage_s": short["down_s"], "short_exit": short["exit"], "short_send_failures": short["send_failures"], "short_events_stored": short["stored_final"],
         "long_outage_s": robust["down_s"],
         "naive_exit": naive["exit"], "naive_reported_failures": naive["send_failures"], "naive_silent_loss": naive["lost_before_redo"],
         "checked_exit": robust["exit"], "checked_send_failures": robust["send_failures"], "checked_lost_before_resend": robust["lost_before_redo"],
         "checked_resend_exit": robust["redo_exit"], "checked_events_stored_final": robust["stored_final"],
         "status_match": f"{k['status_match']}/{k['status_total']}"}, t0)


# ---- (e) seluruh jalur batch dijalankan ulang tanpa perubahan -----------------------------------------------
def _batch_snapshot(ctx: Ctx) -> dict:
    from .marts import MART_TABLES
    from .model import DW_TABLES
    snap = {}
    with ctx.conn() as c:
        for schema, tables in (("raw", [t.name for t in TABLES]), ("dw", DW_TABLES), ("mart", MART_TABLES)):
            for t in tables:
                snap[f"{schema}.{t}"] = c.execute(f"SELECT count(*) FROM {schema}.{t}").fetchone()[0]
        snap["mart.revenue_sum"] = str(c.execute("SELECT sum(revenue) FROM mart.sales_monthly").fetchone()[0])
        snap["dw.late_orders"] = c.execute("SELECT count(*) FROM dw.fact_order WHERE is_late").fetchone()[0]
    return snap


def case_batch_rerun(ctx: Ctx) -> dict:
    t0 = time.time()
    # mart.stream_freshness* mencerminkan stream.batch_metrics; kasus stream sebelumnya mengubahnya. Segarkan dulu
    # agar baseline = keadaan sekarang, lalu pembandingan murni "dijalankan ulang tanpa perubahan".
    cli(ctx, "marts")
    before = _batch_snapshot(ctx)
    runs = {st: cli(ctx, st) for st in ("bronze", "silver", "load", "model", "marts")}
    after = _batch_snapshot(ctx)
    bronze = json.loads((ctx.root / "outputs" / "bronze.json").read_text())
    statuses = sorted({f["status"] for f in bronze["files"]})
    diff = {k: (before[k], after[k]) for k in before if before[k] != after.get(k)}
    caught = all(r["rc"] == 0 for r in runs.values()) and statuses == ["unchanged"] and not diff
    return result(
        "e", "Seluruh jalur batch dijalankan ulang tanpa perubahan",
        "`bronze → silver → load → model → marts` dijalankan lagi tanpa mengubah data sumber",
        f"bronze: semua file `{', '.join(statuses)}`; {len(before)} besaran (baris tiap tabel raw/dw/mart, Σ pendapatan, order terlambat) "
        f"{'identik' if not diff else 'BERBEDA: ' + str(diff)}",
        caught,
        "bronze: perbandingan sha256 dengan manifest; silver/load: TRUNCATE+COPY satu transaksi; dw/mart: rebuild penuh dari raw dalam satu transaksi",
        "tidak ada yang perlu dipulihkan", caught,
        {"stage_seconds": {k: v["seconds"] for k, v in runs.items()}, "stage_exit": {k: v["rc"] for k, v in runs.items()},
         "quantities_compared": len(before), "differences": len(diff), "bronze_statuses": statuses}, t0)


# ---- (f) objek bronze diubah diam-diam di bucket ---------------------------------------------------------------
def case_bronze_tamper(ctx: Ctx) -> dict:
    t0 = time.time()
    s3 = ctx.s3()
    m = read_manifest(s3, "bronze", "sellers")
    key = m["latest"]["object_key"]
    orig = s3.get_object(Bucket="bronze", Key=key)["Body"].read()
    bad = bytearray(orig)
    bad[len(bad) // 2] ^= 0x20
    raw_before = scalar(ctx, "SELECT count(*) FROM raw.sellers")
    try:
        s3.put_object(Bucket="bronze", Key=key, Body=bytes(bad))
        s = cli(ctx, "silver")
    finally:
        s3.put_object(Bucket="bronze", Key=key, Body=orig)   # selalu kembalikan objek asli
    after = scalar(ctx, "SELECT count(*) FROM raw.sellers")
    r = cli(ctx, "silver")
    caught = s["rc"] != 0 and "sha256" in s["out"] and raw_before == after
    return result(
        "f", "Objek bronze berubah diam-diam di bucket",
        f"satu byte di `{key}` diubah langsung di MinIO (manifest tidak diubah)",
        line_with(s["out"], "sha256") + f" (exit {s['rc']})",
        caught,
        "`fetch_latest` di olist/silver.py menghitung sha256 objek dan membandingkannya dengan manifest sebelum memproses",
        "kembalikan objek asli dari sumber/riwayat bronze, lalu `silver`", r["rc"] == 0,
        {"silver_exit": s["rc"], "raw_sellers_before": raw_before, "raw_sellers_after": after, "silver_after_restore_exit": r["rc"]}, t0)


CASES: dict[str, tuple[str, Callable]] = {
    "a": ("kolom hilang di sumber", case_missing_column),
    "b": ("producer dua kali", case_producer_twice),
    "c": ("consumer mati sebelum commit offset", case_consumer_crash),
    "d": ("broker Kafka mati", case_broker_outage),
    "e": ("jalur batch dijalankan ulang", case_batch_rerun),
    "f": ("objek bronze diubah diam-diam", case_bronze_tamper),
}


def run_cases(ctx: Ctx, only: list[str] | None = None) -> list[dict]:
    out = []
    for cid, (name, fn) in CASES.items():
        if only and cid not in only:
            continue
        ctx.log(f"[inject:{cid}] {name} …")
        try:
            r = fn(ctx)
        except Exception as e:   # kegagalan infrastruktur uji ≠ LOLOS; dilaporkan sebagai ERROR
            r = {"id": cid, "name": name, "broken": "-", "symptom": f"{type(e).__name__}: {e}", "verdict": "ERROR",
                 "mechanism": "-", "recovery": "-", "recovered": False, "evidence": {}, "seconds": 0}
        ctx.log(f"[inject:{cid}] {r['verdict']} ({r['seconds']} s) — {r['symptom'][:110]}")
        out.append(r)
    return out


def restore(ctx: Ctx) -> dict:
    """Kembalikan stack ke keadaan bersih: replay penuh + mart + smoke (penilai menjalankan ini setelah inject)."""
    from .smoke import run_smoke
    t0 = time.time()
    s = cli(ctx, "stream", "run")
    m = cli(ctx, "marts")
    with ctx.conn() as c:
        smoke = run_smoke(ctx.env, c, ctx.s3(), ctx.root)
    return {"stream_exit": s["rc"], "marts_exit": m["rc"], "smoke": smoke, "ok": s["rc"] == 0 and m["rc"] == 0 and all(x["ok"] for x in smoke),
            "seconds": round(time.time() - t0, 1)}


def report_md(cases: list[dict], restored: dict | None, ran_at: str) -> str:
    n_ok = sum(c["verdict"] == "TERTANGKAP" for c in cases)
    o = ["# Failure Test (T6)\n",
         f"Dijalankan {ran_at} terhadap stack yang hidup (`python pipeline.py inject`). **{n_ok}/{len(cases)} kasus TERTANGKAP.**\n",
         "| # | Kasus | Verdict | Pulih | Symptom |", "|---|---|---|:---:|---|"]
    for c in cases:
        o.append(f"| {c['id']} | {c['name']} | **{c['verdict']}** | {'ya' if c['recovered'] else 'TIDAK'} | {c['symptom']} |")
    for c in cases:
        o += [f"\n## ({c['id']}) {c['name']} — {c['verdict']}\n",
              f"- **Yang dirusak:** {c['broken']}", f"- **Symptom:** {c['symptom']}",
              f"- **Mekanisme yang menangkap:** {c['mechanism']}", f"- **Cara pulih:** {c['recovery']}",
              f"- **Bukti (angka):** " + "; ".join(f"{k} = {v}" for k, v in c["evidence"].items()), f"- Durasi uji: {c['seconds']} s"]
    if restored:
        o += ["\n## Pemulihan stack setelah semua gangguan\n",
              f"Replay penuh + marts + smoke: **{'OK' if restored['ok'] else 'GAGAL'}** ({restored['seconds']} s).\n",
              "| Pemeriksaan smoke | Hasil | Detail |", "|---|:---:|---|"]
        o += [f"| {x['check']} | {'ya' if x['ok'] else 'TIDAK'} | {x['detail']} |" for x in restored["smoke"]]
    return "\n".join(o) + "\n"


def write_outputs(root: Path, cases: list[dict], restored: dict | None) -> Path:
    ran_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    out = root / "outputs"
    (out / "failures.json").write_text(json.dumps({"ran_at": ran_at, "cases": cases, "restore": restored}, indent=2, ensure_ascii=False))
    md = out / "failure_tests.md"
    md.write_text(report_md(cases, restored, ran_at), encoding="utf-8")
    return md
