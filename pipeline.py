#!/usr/bin/env python3
"""pipeline.py — satu pintu untuk Olist Data Platform (starter Tugas Besar).

Yang SUDAH ada: init / up / down (infrastruktur, sama dengan lab Pertemuan 9).
Yang KAMU tulis: check, profile, bronze, silver, load, stream, model, marts, inject, report.
Tiap sub-perintah di bawah hanya menjelaskan targetnya lalu berhenti — ganti badannya dengan panggilan ke modulmu.

    python pipeline.py init && python pipeline.py up
    python pipeline.py check
    ...
"""

from __future__ import annotations

import argparse
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
SERVICES = ["postgres", "kafka", "minio", "kafka-ui", "jupyter"]


def read_env() -> dict[str, str]:
    env = {}
    p = ROOT / ".env"
    if p.exists():
        for line in p.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def compose(*args):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True)


def port_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(0.3)
        return s.connect_ex(("127.0.0.1", port)) != 0


def cmd_init(a):
    if not (ROOT / ".env").exists():
        shutil.copy(ROOT / ".env.example", ROOT / ".env")
        print("[1] .env dibuat dari .env.example")
    e = read_env()
    print("[2] port:")
    for k, key in [("postgres", "POSTGRES_PORT"), ("kafka", "KAFKA_HOST_PORT"), ("kafka-ui", "KAFKA_UI_PORT"), ("minio", "MINIO_API_PORT"), ("jupyter", "JUPYTER_PORT")]:
        p = int(e.get(key, 0))
        print(f"    {k:10s} {p:6d}  {'bebas' if port_free(p) else 'BENTROK — ganti di .env (atau stack sudah jalan)'}")
    raw = ROOT / e.get("RAW_DIR", "data/raw")
    print(f"[3] data mentah: {raw} → {'ada' if any(raw.glob('*.csv')) else 'BELUM ADA — unduh dataset Kaggle ke sini, lalu python scripts/check_dataset.py'}")
    print("[4] TODO kamu: buat skema (sql/00_schemas.sql), topic Kafka, dan pastikan bucket MinIO ada")


def cmd_up(a):
    compose("up", "-d", *([] if a.build else ["--no-build"]))
    t0 = time.time()
    seen = {}
    while time.time() - t0 < 240 and len(seen) < len(SERVICES):
        for svc in SERVICES:
            if svc in seen:
                continue
            r = subprocess.run(["docker", "inspect", f"olist-{svc}", "--format", "{{.State.Health.Status}}"], capture_output=True, text=True)
            if r.returncode == 0 and r.stdout.strip() == "healthy":
                seen[svc] = round(time.time() - t0, 1)
                print(f"    {svc:10s} healthy pada {seen[svc]} s")
        time.sleep(1)
    missing = [s for s in SERVICES if s not in seen]
    if missing:
        sys.exit(f"TIMEOUT: belum healthy: {missing} — cek `docker compose ps` dan `docker compose logs <svc>`")


def cmd_down(a):
    compose("down", *(["-v"] if a.volumes else []))


def todo(stage: str, target: str):
    def fn(a):
        print(f"[{stage}] belum diimplementasikan.\n  Target: {target}\n  Lihat TUGAS_BESAR.md bagian {stage}.")
        sys.exit(2)
    return fn


def cmd_check(a):
    r = subprocess.run([sys.executable, "scripts/check_dataset.py"], cwd=ROOT, capture_output=True, text=True)
    out = ROOT / "outputs"; out.mkdir(exist_ok=True)
    (out / "dataset_check.md").write_text("# Dataset check\n\n```\n" + r.stdout + r.stderr + "```\n", encoding="utf-8")
    print(r.stdout + r.stderr, end="")
    sys.exit(r.returncode)


def cmd_profile(a):
    from olist.profile import run
    run(ROOT, read_env().get("RAW_DIR", "data/raw"))
    print("outputs/profile.md + outputs/profile.json ditulis")


def cmd_bronze(a):
    from olist.bronze import make_s3, run_ingest, write_outputs
    env = read_env()
    try:
        run = run_ingest(make_s3(env), "bronze", ROOT / env.get("RAW_DIR", "data/raw"))
    except FileNotFoundError as e:
        sys.exit(f"bronze berhenti: {e}")
    md = write_outputs(ROOT, run)
    for r in run["files"]:
        print(f"  {r['table']:30s} {r['status']:12s} v{r['version']}  {r['rows']:>9,} baris  {r['seconds']:>5} s")
    print(f"selesai {run['seconds']} s → {md.relative_to(ROOT)}")


def cmd_silver(a):
    from olist.bronze import make_s3
    from olist.gate import SourceError
    from olist.silver import run_silver, write_outputs
    try:
        run = run_silver(make_s3(read_env()))
    except SourceError as e:
        sys.exit(f"silver berhenti (Postgres tidak disentuh): {e}")
    md = write_outputs(ROOT, run)
    for t in run["tables"]:
        print(f"  {t['table']:30s} {t['verdict']:15s} {t['source_rows']:>9,} sumber  {t['quarantined']:>6,} karantina")
    print(f"selesai {run['seconds']} s → {md.relative_to(ROOT)}")
    sys.exit(1 if any(t["verdict"] == "FAIL" for t in run["tables"]) else 0)


def cmd_load(a):
    import psycopg
    from olist.bronze import make_s3
    from olist.gate import SourceError
    from olist.load import pg_dsn, run_load, write_outputs
    env = read_env()
    try:
        with psycopg.connect(pg_dsn(env), connect_timeout=5) as conn:
            run = run_load(make_s3(env), conn)
    except SourceError as e:
        sys.exit(f"load berhenti: {e}")
    md = write_outputs(ROOT, run)
    for r in run["tables"]:
        print(f"  raw.{r['table']:30s} {r['loaded']:>9,} baris  {r['seconds']:>5} s")
    if run["skipped_fail"]:
        print(f"  TIDAK dimuat (FAIL): {', '.join(run['skipped_fail'])}")
    print(f"selesai {run['seconds']} s → {md.relative_to(ROOT)}")
    sys.exit(1 if run["skipped_fail"] else 0)


def _pg(env):
    import psycopg
    from olist.load import pg_dsn
    return psycopg.connect(pg_dsn(env), connect_timeout=5)


def _stream_sql(conn):
    conn.execute((ROOT / "sql" / "10_stream.sql").read_text(encoding="utf-8"))
    conn.commit()


def _stream_reset(env, conn):
    from olist.producer import reset_kafka
    reset_kafka(env)
    conn.execute("TRUNCATE stream.order_events, stream.order_current, stream.dlq, stream.batch_metrics")
    conn.commit()
    (ROOT / "outputs" / "stream_truth.json").unlink(missing_ok=True)
    print("[stream] reset: topic + consumer group + tabel stream.* + stream_truth.json dikosongkan")


def cmd_stream(a):
    from olist.consumer import run_consumer
    from olist.producer import SendFailure, run_producer
    from olist.stream import Disturbance
    from olist.stream_report import write_outputs
    env = read_env()
    truth = ROOT / "outputs" / "stream_truth.json"
    with _pg(env) as conn:
        _stream_sql(conn)
        if a.action == "reset":
            return _stream_reset(env, conn)
        if a.action == "produce":
            d = Disturbance(a.dup, a.late, a.corrupt, seed=a.seed)
            try:
                run = run_producer(env, conn, limit=a.limit, speed=a.speed, seed=a.seed, disturb=d, truth_path=truth)
            except SendFailure as e:
                sys.exit(f"producer GAGAL: {e}")
            return print(f"[producer] selesai {run['seconds']} s, {run['counts']['messages_total']:,} pesan")
        if a.action == "consume":
            t = run_consumer(env, conn, idle_exit=a.idle_exit, start_grace=a.start_grace, crash_at_batch=a.crash_at_batch)
            return print(f"[consumer] selesai {t}")
        if a.action == "report":
            md, r = write_outputs(ROOT, env, conn)
            print(md.read_text(encoding="utf-8"))
            sys.exit(0 if all(r["checks"].values()) else 1)
        # run: reset → consumer (proses terpisah, mulai lebih dulu) → producer → tunggu consumer → laporan
        if not a.no_reset:
            _stream_reset(env, conn)
        cons = subprocess.Popen([sys.executable, "pipeline.py", "stream", "consume", "--idle-exit", str(a.idle_exit)], cwd=ROOT)
        time.sleep(3)
        t0 = time.time()
        d = Disturbance(a.dup, a.late, a.corrupt, seed=a.seed)
        try:
            run_producer(env, conn, limit=a.limit, speed=a.speed, seed=a.seed, disturb=d, truth_path=truth)
        except SendFailure as e:
            cons.terminate()
            sys.exit(f"producer GAGAL: {e}")
        cons.wait()
        print(f"[stream] replay selesai {round(time.time() - t0, 1)} s (producer + consumer)")
        md, r = write_outputs(ROOT, env, conn)
        print(md.read_text(encoding="utf-8"))
        sys.exit(0 if all(r["checks"].values()) else 1)


def cmd_contract(a):
    from olist.contract import link_data, write
    raw = read_env().get("RAW_DIR", "data/raw")
    f = write(ROOT)
    print(f"kontrak ODCS → {f.relative_to(ROOT)}")
    exe = str(Path(sys.executable).parent / "datacontract")
    r = subprocess.run([exe, "lint", str(f)], cwd=ROOT)
    if r.returncode or a.no_test:
        sys.exit(r.returncode)
    link_data(ROOT, raw)
    sys.exit(subprocess.run([exe, "test", str(f)], cwd=ROOT).returncode)


def cmd_contract_doc(a):
    from olist.contract_md import write
    print(f"{write(ROOT).relative_to(ROOT)} ditulis")


PARSER = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
SUB = PARSER.add_subparsers(dest="cmd", required=True)
p = SUB.add_parser("init"); p.set_defaults(func=cmd_init)
p = SUB.add_parser("up"); p.add_argument("--build", action="store_true", help="bangun image jupyter (pertama kali)"); p.set_defaults(func=cmd_up)
p = SUB.add_parser("down"); p.add_argument("-v", "--volumes", action="store_true"); p.set_defaults(func=cmd_down)
p = SUB.add_parser("check"); p.set_defaults(func=cmd_check)
p = SUB.add_parser("profile"); p.set_defaults(func=cmd_profile)
p = SUB.add_parser("contract", help="bangkitkan kontrak ODCS + lint + test"); p.add_argument("--no-test", action="store_true"); p.set_defaults(func=cmd_contract)
p = SUB.add_parser("contract-doc", help="bangkitkan DATA_CONTRACT.md"); p.set_defaults(func=cmd_contract_doc)
p = SUB.add_parser("bronze", help="CSV → MinIO bronze apa adanya + manifest (idempoten)"); p.set_defaults(func=cmd_bronze)
p = SUB.add_parser("silver", help="bronze → silver Parquet + gate kualitas + karantina"); p.set_defaults(func=cmd_silver)
p = SUB.add_parser("load", help="silver → raw.* (TRUNCATE+COPY, satu transaksi)"); p.set_defaults(func=cmd_load)
p = SUB.add_parser("stream", help="replay order → Kafka → Postgres (stream run|produce|consume|report|reset)")
p.add_argument("action", nargs="?", default="run", choices=["run", "produce", "consume", "report", "reset"])
p.add_argument("--limit", type=int, help="replay hanya N order pertama (berdasar waktu beli)")
p.add_argument("--speed", type=float, default=0.0, help="pesan/detik (0 = secepatnya)")
p.add_argument("--seed", type=int, default=42)
p.add_argument("--dup", type=float, default=0.03, help="proporsi event dikirim dobel")
p.add_argument("--late", type=float, default=0.02, help="proporsi event ditahan lalu dikirim terlambat")
p.add_argument("--corrupt", type=float, default=0.005, help="proporsi salinan rusak (tambahan)")
p.add_argument("--idle-exit", type=float, default=10.0, help="consumer berhenti setelah sekian detik tanpa pesan")
p.add_argument("--start-grace", type=float, default=90.0, help="consumer menunggu pesan pertama sekian detik")
p.add_argument("--crash-at-batch", type=int, help="[uji] consumer mati paksa setelah DB commit batch ke-N, sebelum commit offset")
p.add_argument("--no-reset", action="store_true", help="run: jangan reset topic/tabel dulu")
p.set_defaults(func=cmd_stream)
for name, stage, target in [
    ("model", "T4", "star schema dw.* dengan PK/FK terpasang → outputs/dw_report.md"),
    ("marts", "T5", "mart.* + rekonsiliasi + ekspor gold → outputs/marts_report.md"),
    ("inject", "T6", "≥ 5 failure injection yang pulih sendiri → outputs/failure_tests.md"),
    ("report", "T6", "EVALUATION_EVIDENCE.md dari outputs/*.json"),
]:
    p = SUB.add_parser(name); p.set_defaults(func=todo(stage, target))

if __name__ == "__main__":
    args = PARSER.parse_args()
    args.func(args)
