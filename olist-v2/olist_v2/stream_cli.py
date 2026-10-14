"""T3 — satu pintu perintah stream: reset | produce | consume | report | run (replay penuh).

  python -m olist_v2.stream_cli run [--limit N] [--speed S] [--seed 42] ...

`run`: reset → consumer (proses terpisah, mulai lebih dulu) → producer → tunggu consumer → laporan.
Exit code ≠ 0 bila salah satu pemeriksaan laporan gagal (event unik, duplikat, DLQ, status terkini).
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from .contract import REPO

SQL = Path(__file__).parent / "sql" / "10_stream.sql"


def outputs(env=os.environ) -> Path:
    return Path(env.get("OLIST_OUTPUTS", REPO / "outputs"))


def connect(env=os.environ):
    import psycopg2

    conn = psycopg2.connect(env["WAREHOUSE_DSN"], connect_timeout=5)
    with conn, conn.cursor() as cur:
        cur.execute(SQL.read_text(encoding="utf-8"))
    return conn


def reset(env, conn) -> None:
    from .producer import reset_kafka

    reset_kafka(env)
    with conn, conn.cursor() as cur:
        cur.execute("TRUNCATE stream.order_events, stream.order_current, stream.dlq, stream.batch_metrics")
    (outputs(env) / "stream_truth.json").unlink(missing_ok=True)
    print("[stream] reset: topic + consumer group + tabel stream.* + stream_truth.json dikosongkan")


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="olist_v2.stream_cli", description=__doc__.split("\n")[0])
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
    p.add_argument("--unchecked", action="store_true", help="[uji] producer naif: future send() tidak diperiksa")
    return p


def main(argv=None, env=os.environ) -> int:
    from .consumer import run_consumer
    from .producer import SendFailure, run_producer
    from .stream import Disturbance
    from .stream_report import write_outputs

    a = parser().parse_args(argv)
    env = dict(env)
    truth = outputs(env) / "stream_truth.json"
    conn = connect(env)
    try:
        if a.action == "reset":
            reset(env, conn)
            return 0
        if a.action == "produce":
            try:
                run = run_producer(env, conn, limit=a.limit, speed=a.speed, seed=a.seed, truth_path=truth, unchecked=a.unchecked,
                                   disturb=Disturbance(a.dup, a.late, a.corrupt, seed=a.seed))
            except SendFailure as e:
                print(f"producer GAGAL: {e}", file=sys.stderr)
                return 1
            print(f"[producer] selesai {run['seconds']} s, {run['counts']['messages_total']:,} pesan")
            return 0
        if a.action == "consume":
            print(f"[consumer] selesai {run_consumer(env, conn, idle_exit=a.idle_exit, start_grace=a.start_grace, crash_at_batch=a.crash_at_batch)}")
            return 0
        if a.action == "run":
            if not a.no_reset:
                reset(env, conn)
            cons = subprocess.Popen([sys.executable, "-m", "olist_v2.stream_cli", "consume", "--idle-exit", str(a.idle_exit)])
            time.sleep(3)
            t0 = time.time()
            try:
                run_producer(env, conn, limit=a.limit, speed=a.speed, seed=a.seed, truth_path=truth,
                             disturb=Disturbance(a.dup, a.late, a.corrupt, seed=a.seed))
            except SendFailure as e:
                cons.terminate()
                print(f"producer GAGAL: {e}", file=sys.stderr)
                return 1
            cons.wait()
            print(f"[stream] replay selesai {round(time.time() - t0, 1)} s (producer + consumer)")
        conn.rollback()                         # akhiri transaksi baca lama agar laporan melihat data terbaru
        md, r = write_outputs(outputs(env), env, conn)
        print(md.read_text(encoding="utf-8"))
        return 0 if all(r["checks"].values()) else 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
