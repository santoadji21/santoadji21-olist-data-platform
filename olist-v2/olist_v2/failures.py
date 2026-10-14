"""T6 — Failure injection otomatis terhadap stack v2 yang HIDUP. Tiap kasus = fungsi yang MENGEMBALIKAN hasil (dict).

Dijalankan dari laptop (venv): Postgres/MinIO/Kafka dibaca lewat port host; tahap pipeline (bronze, silver, gate, load, stream, dbt)
dijalankan di container scheduler lewat `docker compose exec` — proses sungguhan, exit code sungguhan. Broker Kafka dihentikan
lewat `docker stop`. Berkas sumber dirusak lewat salinan sementara di data/raw (data asli tidak pernah diubah).

Hasil: {id, name, broken, symptom, verdict ∈ TERTANGKAP|LOLOS|ERROR, mechanism, recovery, recovered, evidence, seconds}.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import shutil
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .bronze import make_s3, read_manifest
from .contract import load_models

STREAM_N = 5000          # jumlah order untuk kasus stream (cukup untuk banyak batch, cepat)
STREAM_IDLE = "8"
COMPOSE = ["docker", "compose", "-p", "olist-v2"]
KAFKA = "olist-v2-kafka"


def host_env(root: Path) -> dict:
    """Env untuk kode yang jalan di laptop: .env + alamat host (localhost:port)."""
    e = dict(os.environ)
    p = root / ".env"
    if p.exists():
        e.update(l.split("=", 1) for l in p.read_text().splitlines() if "=" in l and not l.lstrip().startswith("#"))
    e.update({"WAREHOUSE_DSN": f"postgresql://{e['V2_PG_USER']}:{e['V2_PG_PASSWORD']}@localhost:{e.get('V2_PG_PORT', '15432')}/olist_dw",
              "MINIO_ENDPOINT": f"http://localhost:{e.get('V2_MINIO_PORT', '19000')}",
              "MINIO_ROOT_USER": e["V2_MINIO_ROOT_USER"], "MINIO_ROOT_PASSWORD": e["V2_MINIO_ROOT_PASSWORD"],
              "KAFKA_BOOTSTRAP": f"localhost:{e.get('V2_KAFKA_PORT', '19092')}"})
    return e


@dataclass
class Ctx:
    root: Path
    env: dict
    log: Callable = print

    def conn(self):
        import psycopg2
        return psycopg2.connect(self.env["WAREHOUSE_DSN"], connect_timeout=5)

    def s3(self):
        return make_s3(self.env)


def run(ctx: Ctx, *cmd: str, env: dict | None = None, timeout: int = 1800) -> dict:
    """Jalankan perintah di container scheduler. `env` hanya diteruskan ke proses itu."""
    t0 = time.time()
    flags = [f for k, v in (env or {}).items() for f in ("-e", f"{k}={v}")]
    p = subprocess.run([*COMPOSE, "exec", "-T", *flags, "airflow-scheduler", *cmd], cwd=ctx.root, capture_output=True, text=True, timeout=timeout)
    return {"rc": p.returncode, "out": (p.stdout + p.stderr).strip(), "seconds": round(time.time() - t0, 1)}


def py(ctx: Ctx, module: str, *args: str, env: dict | None = None) -> dict:
    return run(ctx, "python", "-m", f"olist_v2.{module}", *args, env=env)


def stream(ctx: Ctx, *args: str) -> dict:
    return py(ctx, "stream_cli", *args)


def dbt_build(ctx: Ctx) -> dict:
    return run(ctx, "sh", "-c", 'python -c "from olist_v2.stream_cli import connect; connect().close()" && "$DBT_BIN" build '
               '--project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROFILES_DIR"',
               env={"DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs", "DBT_SEND_ANONYMOUS_USAGE_STATS": "False"})


def line_with(out: str, needle: str) -> str:
    return next((ln.strip() for ln in out.splitlines() if needle in ln), out.splitlines()[-1].strip() if out else "")


def scalar(ctx: Ctx, sql: str):
    c = ctx.conn()
    try:
        with c.cursor() as cur:
            cur.execute(sql)
            return cur.fetchone()[0]
    finally:
        c.close()


def stream_state(ctx: Ctx) -> dict:
    from .stream_report import collect
    c = ctx.conn()
    try:
        return collect(ctx.env, c, ctx.root / "outputs" / "stream_truth.json")
    finally:
        c.close()


def result(case_id, name, broken, symptom, caught: bool, mechanism, recovery, recovered: bool, evidence: dict, t0) -> dict:
    return {"id": case_id, "name": name, "broken": broken, "symptom": symptom,
            "verdict": "TERTANGKAP" if caught else "LOLOS", "mechanism": mechanism, "recovery": recovery,
            "recovered": bool(recovered), "evidence": evidence, "seconds": round(time.time() - t0, 1)}


# ---- (a) kolom hilang di sumber -------------------------------------------------------------------
def case_missing_column(ctx: Ctx) -> dict:
    from .silver import DQ_KEY, silver_prefix
    t0 = time.time()
    s3 = ctx.s3()
    raw = ctx.root / "data" / "raw"
    customers = next(m for m in load_models() if m.name == "customers")
    dropped = "customer_unique_id"

    def silver_fingerprint() -> str:
        h = hashlib.sha256()
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket="silver", Prefix=silver_prefix("customers")):
            for o in page.get("Contents", []):
                h.update(f"{o['Key']}{o['ETag']}".encode())
        return h.hexdigest()

    def snap():
        return {"raw_rows": scalar(ctx, 'SELECT count(*) FROM raw."customers"'), "silver": silver_fingerprint(),
                "dq_etag": s3.head_object(Bucket="silver", Key=DQ_KEY)["ETag"]}

    before = snap()
    tmp = raw / f".inject-{uuid.uuid4().hex[:8]}"          # di dalam data/raw agar ikut ter-mount ke container
    tmp.mkdir()
    try:
        for m in load_models():
            if m.name != "customers":
                os.symlink(Path("..") / m.file, tmp / m.file)
        with open(raw / customers.file, newline="", encoding="utf-8-sig") as f, \
                open(tmp / customers.file, "w", newline="", encoding="utf-8") as g:
            rd, wr = csv.reader(f), csv.writer(g)
            header = next(rd)
            drop = header.index(dropped)
            wr.writerow([h for i, h in enumerate(header) if i != drop])
            for row in rd:
                wr.writerow([v for i, v in enumerate(row) if i != drop])
        rel = f"/opt/airflow/data/raw/{tmp.name}"
        b = py(ctx, "bronze", env={"OLIST_RAW_DIR": rel})        # bronze mengunggah apa adanya (tidak memvalidasi)
        s = py(ctx, "silver")
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    after = snap()
    caught = s["rc"] != 0 and dropped in s["out"] and before == after
    r1, r2, r3, r4 = py(ctx, "bronze"), py(ctx, "silver"), py(ctx, "gate"), py(ctx, "load")
    recovered = all(r["rc"] == 0 for r in (r1, r2, r3, r4)) and scalar(ctx, 'SELECT count(*) FROM raw."customers"') == before["raw_rows"]
    return result(
        "a", "Kolom hilang di sumber",
        f"kolom `{dropped}` dihapus dari salinan CSV customers, lalu `bronze` dan `silver` dijalankan",
        line_with(s["out"], "silver berhenti") + f" (exit {s['rc']})",
        caught,
        "`check_header` (olist_v2/rules.py) membandingkan header dengan kontrak ODCS SEBELUM menulis apa pun; `run_silver` berhenti sebelum silver/Postgres disentuh",
        "ingest ulang CSV asli (bronze v baru; versi rusak tetap tercatat di manifest), lalu `silver`, `gate`, `load`",
        recovered,
        {"bronze_exit": b["rc"], "silver_exit": s["rc"], "raw_customers_before": before["raw_rows"], "raw_customers_after": after["raw_rows"],
         "silver_parquet_unchanged": before["silver"] == after["silver"], "dq_report_unchanged": before["dq_etag"] == after["dq_etag"]}, t0)


# ---- (b) producer dijalankan dua kali ---------------------------------------------------------------
def case_producer_twice(ctx: Ctx) -> dict:
    t0 = time.time()
    stream(ctx, "reset")
    p1 = stream(ctx, "produce", "--limit", str(STREAM_N))
    p2 = stream(ctx, "produce", "--limit", str(STREAM_N))
    c = stream(ctx, "consume", "--idle-exit", STREAM_IDLE)
    st = stream_state(ctx)
    k, s = st["consumer"], st["sent"]
    caught = (p1["rc"] == 0 and p2["rc"] == 0 and all(st["checks"].values()) and s["runs"] == 2 and k["events_stored"] == s["events_unique"])
    return result(
        "b", "Producer dijalankan dua kali",
        f"`stream produce --limit {STREAM_N}` dijalankan dua kali berturut-turut tanpa reset (semua event dikirim ulang)",
        f"{s['messages_total']:,} pesan di topic, tetapi event tersimpan tetap {k['events_stored']:,} (= event unik); {k['duplicates_derived']:,} duplikat diabaikan",
        caught,
        "`event_id` deterministik `<order_id>:<status>` = PK `stream.order_events`; `INSERT … ON CONFLICT DO NOTHING` mengabaikan duplikat; upsert `order_current` hanya menaikkan status",
        "tidak perlu pemulihan (idempoten). Baris DLQ ikut berlipat karena pesan rusak run ke-2 punya offset baru; `stream reset` membersihkannya",
        caught,
        {"runs": s["runs"], "events_unique": s["events_unique"], "events_stored": k["events_stored"], "messages_total": s["messages_total"],
         "duplicates_ignored": k["duplicates_derived"], "dlq_rows": k["dlq_rows"], "status_match": f"{k['status_match']}/{k['status_total']}",
         "consumer_exit": c["rc"]}, t0)


# ---- (c) consumer mati sebelum commit offset ----------------------------------------------------------
def case_consumer_crash(ctx: Ctx) -> dict:
    t0 = time.time()
    stream(ctx, "reset")
    stream(ctx, "produce", "--limit", str(STREAM_N))
    crash = stream(ctx, "consume", "--crash-at-batch", "3", "--idle-exit", STREAM_IDLE)   # proses benar-benar mati (os._exit 137)
    stored_at_crash = scalar(ctx, "SELECT count(*) FROM stream.order_events")
    crashed_msgs = scalar(ctx, "SELECT n_messages FROM stream.batch_metrics WHERE batch_no = 3 ORDER BY consumed_at LIMIT 1")
    recov = stream(ctx, "consume", "--idle-exit", STREAM_IDLE)                              # restart tanpa reset
    st = stream_state(ctx)
    # Redelivery diukur eksak: pesan yang diproses kedua run dikurangi isi topic = pesan yang diproses dua kali.
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
        "jalankan consumer lagi tanpa reset; tidak ada langkah manual", caught,
        {"crash_exit_code": crash["rc"], "events_stored_at_crash": stored_at_crash, "messages_in_crashed_batch": crashed_msgs,
         "messages_redelivered": redelivered, "events_stored_final": k["events_stored"], "events_unique_sent": st["sent"]["events_unique"],
         "status_match": f"{k['status_match']}/{k['status_total']}", "checks_ok": sum(st["checks"].values())}, t0)


# ---- (d) broker Kafka mati di tengah replay ----------------------------------------------------------------
def _wait_kafka_healthy(timeout: int = 120) -> bool:
    t0 = time.time()
    while time.time() - t0 < timeout:
        r = subprocess.run(["docker", "inspect", KAFKA, "--format", "{{.State.Health.Status}}"], capture_output=True, text=True)
        if r.stdout.strip() == "healthy":
            return True
        time.sleep(2)
    return False


def _outage(delay: float, down: float, box: dict):
    time.sleep(delay)
    box["stopped_at"] = time.time()
    subprocess.run(["docker", "stop", "-t", "2", KAFKA], capture_output=True)
    time.sleep(down)
    subprocess.run(["docker", "start", KAFKA], capture_output=True)
    box["healthy_again"] = _wait_kafka_healthy()
    box["down_seconds"] = round(time.time() - box["stopped_at"], 1)


def _outage_variant(ctx: Ctx, extra: list[str], down: float, expected: int) -> dict:
    """reset → producer saat broker mati `down` detik → (kirim ulang bila run dinyatakan gagal) → consumer → hitung event."""
    stream(ctx, "reset")
    box: dict = {}
    th = threading.Thread(target=_outage, args=(2.0, down, box))
    th.start()
    r = stream(ctx, "produce", "--limit", "8000", "--speed", "3000", *extra)
    th.join()
    truth_p = ctx.root / "outputs" / "stream_truth.json"
    truth = json.loads(truth_p.read_text()) if truth_p.exists() else {"runs": []}
    failures = truth["runs"][-1]["send_failures"] if truth["runs"] else None
    stream(ctx, "consume", "--idle-exit", STREAM_IDLE)
    stored_before_redo = scalar(ctx, "SELECT count(DISTINCT event_id) FROM stream.order_events")
    redo = None
    if r["rc"] != 0 or (failures or 0) > 0:   # kegagalan TERDETEKSI → pulihkan dengan mengirim ulang (idempoten)
        redo = stream(ctx, "produce", "--limit", "8000")
        stream(ctx, "consume", "--idle-exit", STREAM_IDLE)
    stored = scalar(ctx, "SELECT count(DISTINCT event_id) FROM stream.order_events")
    return {"exit": r["rc"], "send_failures": failures, "down_s": box.get("down_seconds"), "healthy_again": box.get("healthy_again"),
            "stored_before_redo": stored_before_redo, "lost_before_redo": expected - stored_before_redo,
            "redo_exit": None if redo is None else redo["rc"], "stored_final": stored}


def case_broker_outage(ctx: Ctx) -> dict:
    from .producer import all_events, load_orders
    t0 = time.time()
    c = ctx.conn()
    try:
        expected = len(all_events(load_orders(c, 8000)))
    finally:
        c.close()
    short = _outage_variant(ctx, [], 6.0, expected)                  # "beberapa detik": producer yang memeriksa hasil send()
    naive = _outage_variant(ctx, ["--unchecked"], 40.0, expected)    # outage > request_timeout (30 s): producer naif
    robust = _outage_variant(ctx, [], 40.0, expected)                # outage panjang: producer yang memeriksa hasil send()
    k = stream_state(ctx)["consumer"]
    silent_loss = naive["lost_before_redo"] if naive["exit"] == 0 and not naive["send_failures"] else None
    robust_detected = robust["exit"] != 0 or (robust["send_failures"] or 0) > 0 or robust["lost_before_redo"] == 0
    caught = (short["healthy_again"] is True and short["stored_final"] == expected and robust["stored_final"] == expected
              and robust_detected and k["status_match"] == k["status_total"] == 8000)
    return result(
        "d", "Broker Kafka mati beberapa detik di tengah replay",
        f"`docker stop {KAFKA}` ±2 s setelah producer mulai (replay {expected:,} event unik, 3.000 pesan/detik), lalu `docker start` setelah "
        f"{short['down_s']} s (pendek) dan setelah {robust['down_s']} s (panjang, melewati `request_timeout_ms` 30 s)",
        f"outage pendek: producer exit {short['exit']}, kegagalan {short['send_failures']}, {short['stored_final']:,} event tiba. "
        f"Outage panjang: producer naif exit {naive['exit']} dengan {naive['lost_before_redo']:,} event hilang; "
        f"producer yang memeriksa hasil `send()` exit {robust['exit']} dengan {robust['send_failures']} kegagalan terlapor",
        caught,
        "errback pada tiap future `send()` + `flush()` + `SendFailure` membuat run GAGAL keras bila ada pesan yang menyerah; retry internal (`acks=all`, `retries=5`) menyerap outage pendek",
        "kirim ulang producer (idempoten lewat `event_id`) lalu jalankan consumer; tidak perlu reset", bool(caught),
        {"expected_unique_events": expected, "short_outage_s": short["down_s"], "short_exit": short["exit"],
         "short_send_failures": short["send_failures"], "short_events_stored": short["stored_final"], "long_outage_s": robust["down_s"],
         "naive_exit": naive["exit"], "naive_reported_failures": naive["send_failures"], "naive_silent_loss": silent_loss if silent_loss is not None else naive["lost_before_redo"],
         "checked_exit": robust["exit"], "checked_send_failures": robust["send_failures"], "checked_lost_before_resend": robust["lost_before_redo"],
         "checked_resend_exit": robust["redo_exit"], "checked_events_stored_final": robust["stored_final"],
         "status_match": f"{k['status_match']}/{k['status_total']}"}, t0)


# ---- (e) seluruh jalur batch dijalankan ulang tanpa perubahan -----------------------------------------------
def _batch_snapshot(ctx: Ctx) -> dict:
    from .marts import MART_TABLES
    dw = ["dim_date", "dim_geo", "dim_customer", "dim_seller", "dim_product", "fact_order", "fact_order_item", "fact_payment", "fact_review"]
    snap = {}
    c = ctx.conn()
    try:
        with c.cursor() as cur:
            for schema, tables in (("raw", [m.name for m in load_models()]), ("dw", dw), ("mart", [t for t in MART_TABLES if not t.startswith("stream_")])):
                for t in tables:
                    cur.execute(f'SELECT count(*) FROM {schema}."{t}"')
                    snap[f"{schema}.{t}"] = cur.fetchone()[0]
            cur.execute("SELECT sum(revenue) FROM mart.sales_monthly")
            snap["mart.revenue_sum"] = str(cur.fetchone()[0])
            cur.execute("SELECT count(*) FROM dw.fact_order WHERE is_late")
            snap["dw.late_orders"] = cur.fetchone()[0]
    finally:
        c.close()
    return snap


def case_batch_rerun(ctx: Ctx) -> dict:
    t0 = time.time()
    before = _batch_snapshot(ctx)
    runs = {"bronze": py(ctx, "bronze"), "silver": py(ctx, "silver"), "gate": py(ctx, "gate"), "load": py(ctx, "load"),
            "dbt": dbt_build(ctx), "marts": py(ctx, "marts")}
    after = _batch_snapshot(ctx)
    bronze = json.loads((ctx.root / "outputs" / "bronze.json").read_text())
    statuses = sorted({f["status"] for f in bronze["files"]})
    diff = {k: (before[k], after[k]) for k in before if before[k] != after.get(k)}
    caught = all(r["rc"] == 0 for r in runs.values()) and statuses == ["unchanged"] and not diff
    return result(
        "e", "Seluruh jalur batch dijalankan ulang tanpa perubahan",
        "`bronze → silver → gate → load → dbt build → marts` dijalankan lagi tanpa mengubah data sumber",
        f"bronze: semua file `{', '.join(statuses)}`; {len(before)} besaran (baris tiap tabel raw/dw/mart, Σ pendapatan, order terlambat) "
        f"{'identik' if not diff else 'BERBEDA: ' + str(diff)}",
        caught,
        "bronze: perbandingan sha256 dengan manifest; silver/load: TRUNCATE+COPY satu transaksi; dw/mart: dbt membangun ulang tabel penuh dari raw",
        "tidak ada yang perlu dipulihkan", caught,
        {"stage_seconds": {k: v["seconds"] for k, v in runs.items()}, "stage_exit": {k: v["rc"] for k, v in runs.items()},
         "quantities_compared": len(before), "differences": len(diff), "bronze_statuses": statuses}, t0)


# ---- (f) objek bronze diubah diam-diam di bucket ---------------------------------------------------------------
def case_bronze_tamper(ctx: Ctx) -> dict:
    t0 = time.time()
    s3 = ctx.s3()
    m = read_manifest(s3, "bronze", "sellers")
    key = m["versions"][-1]["object_key"] if "versions" in m else m["latest"]["object_key"]
    orig = s3.get_object(Bucket="bronze", Key=key)["Body"].read()
    bad = bytearray(orig)
    bad[len(bad) // 2] ^= 0x20
    raw_before = scalar(ctx, 'SELECT count(*) FROM raw."sellers"')
    try:
        s3.put_object(Bucket="bronze", Key=key, Body=bytes(bad))
        s = py(ctx, "silver")
    finally:
        s3.put_object(Bucket="bronze", Key=key, Body=orig)   # selalu kembalikan objek asli
    after = scalar(ctx, 'SELECT count(*) FROM raw."sellers"')
    r = py(ctx, "silver")
    caught = s["rc"] != 0 and "sha256" in s["out"] and raw_before == after
    return result(
        "f", "Objek bronze berubah diam-diam di bucket",
        f"satu byte di `{key}` diubah langsung di MinIO (manifest tidak diubah)",
        line_with(s["out"], "sha256") + f" (exit {s['rc']})",
        caught,
        "`verified_entry` (olist_v2/silver.py) menghitung sha256 objek dan membandingkannya dengan manifest sebelum memproses",
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
    """Kembalikan stack ke keadaan bersih: replay penuh + marts + smoke."""
    from .smoke import run_smoke
    t0 = time.time()
    s = stream(ctx, "run")
    d = dbt_build(ctx)             # mart freshness dibangun ulang dari metrik stream terbaru
    m = py(ctx, "marts")
    c = ctx.conn()
    try:
        smoke = run_smoke(ctx.env, c, ctx.s3(), ctx.root)
    finally:
        c.close()
    return {"stream_exit": s["rc"], "dbt_exit": d["rc"], "marts_exit": m["rc"], "smoke": smoke,
            "ok": s["rc"] == 0 and d["rc"] == 0 and m["rc"] == 0 and all(x["ok"] for x in smoke), "seconds": round(time.time() - t0, 1)}


def report_md(cases: list[dict], restored: dict | None, ran_at: str) -> str:
    n_ok = sum(c["verdict"] == "TERTANGKAP" for c in cases)
    o = ["# Failure Test (T6)\n",
         f"Dijalankan {ran_at} terhadap stack yang hidup (`make inject`). **{n_ok}/{len(cases)} kasus TERTANGKAP.**\n",
         "| # | Kasus | Verdict | Pulih | Symptom |", "|---|---|---|:---:|---|"]
    for c in cases:
        o.append(f"| {c['id']} | {c['name']} | **{c['verdict']}** | {'ya' if c['recovered'] else 'TIDAK'} | {c['symptom']} |")
    for c in cases:
        o += [f"\n## ({c['id']}) {c['name']} — {c['verdict']}\n",
              f"- **Yang dirusak:** {c['broken']}", f"- **Symptom:** {c['symptom']}",
              f"- **Mekanisme yang menangkap:** {c['mechanism']}", f"- **Cara pulih:** {c['recovery']}",
              "- **Bukti (angka):** " + "; ".join(f"{k} = {v}" for k, v in c["evidence"].items()), f"- Durasi uji: {c['seconds']} s"]
    if restored:
        o += ["\n## Pemulihan stack setelah semua gangguan\n",
              f"Replay penuh + dbt + marts + smoke: **{'OK' if restored['ok'] else 'GAGAL'}** ({restored['seconds']} s).\n",
              "| Pemeriksaan smoke | Hasil | Detail |", "|---|:---:|---|"]
        o += [f"| {x['check']} | {'ya' if x['ok'] else 'TIDAK'} | {x['detail']} |" for x in restored["smoke"]]
    return "\n".join(o) + "\n"


def write_outputs(root: Path, cases: list[dict], restored: dict | None) -> Path:
    ran_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    out = root / "outputs"
    (out / "failures.json").write_text(json.dumps({"ran_at": ran_at, "cases": cases, "restore": restored}, indent=2, ensure_ascii=False, default=str))
    md = out / "failure_tests.md"
    md.write_text(report_md(cases, restored, ran_at), encoding="utf-8")
    return md


def main(argv=None) -> int:
    import argparse
    import sys

    ap = argparse.ArgumentParser(prog="olist_v2.failures", description="Failure injection T6 + smoke (jalan dari laptop, butuh stack hidup)")
    ap.add_argument("action", nargs="?", default="inject", choices=["inject", "smoke", "restore"])
    ap.add_argument("--only", help="kasus tertentu, mis. a,c,e")
    ap.add_argument("--no-restore", action="store_true", help="inject: jangan pulihkan stack (replay penuh + dbt + marts + smoke) setelahnya")
    a = ap.parse_args(argv)
    root = Path(__file__).resolve().parent.parent
    ctx = Ctx(root, host_env(root))
    if a.action == "smoke":
        from .smoke import run_smoke
        c = ctx.conn()
        try:
            res = run_smoke(ctx.env, c, ctx.s3(), root)
        finally:
            c.close()
        for x in res:
            print(f"  {'ok  ' if x['ok'] else 'GAGAL'} {x['check']:60s} {x['detail']}")
        return 0 if all(x["ok"] for x in res) else 1
    if a.action == "restore":
        r = restore(ctx)
        print(f"restore {'OK' if r['ok'] else 'GAGAL'} ({r['seconds']} s)")
        return 0 if r["ok"] else 1
    cases = run_cases(ctx, a.only.split(",") if a.only else None)
    restored = None if (a.no_restore or a.only) else restore(ctx)
    md = write_outputs(root, cases, restored)
    print(md.read_text(encoding="utf-8"))
    ok = all(c["verdict"] == "TERTANGKAP" for c in cases) and (restored is None or restored["ok"])
    return 0 if ok else 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
