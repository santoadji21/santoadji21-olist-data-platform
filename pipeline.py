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


PARSER = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
SUB = PARSER.add_subparsers(dest="cmd", required=True)
p = SUB.add_parser("init"); p.set_defaults(func=cmd_init)
p = SUB.add_parser("up"); p.add_argument("--build", action="store_true", help="bangun image jupyter (pertama kali)"); p.set_defaults(func=cmd_up)
p = SUB.add_parser("down"); p.add_argument("-v", "--volumes", action="store_true"); p.set_defaults(func=cmd_down)
for name, stage, target in [
    ("check", "T0", "9 file ada & sesuai kontrak → outputs/dataset_check.md"),
    ("profile", "T0", "profil per tabel + daftar temuan bernomor → outputs/profile.md"),
    ("bronze", "T1", "CSV → MinIO bronze apa adanya, manifest, idempoten → outputs/bronze_report.md"),
    ("silver", "T2", "tipe sesuai kontrak, gate kualitas, karantina → outputs/dq_report.md"),
    ("load", "T2", "silver → raw.* dengan COPY, TRUNCATE+COPY satu transaksi → outputs/load_report.md"),
    ("stream", "T3", "producer replay + consumer idempoten + DLQ + freshness → outputs/stream_report.md"),
    ("model", "T4", "star schema dw.* dengan PK/FK terpasang → outputs/dw_report.md"),
    ("marts", "T5", "mart.* + rekonsiliasi + ekspor gold → outputs/marts_report.md"),
    ("inject", "T6", "≥ 5 failure injection yang pulih sendiri → outputs/failure_tests.md"),
    ("report", "T6", "EVALUATION_EVIDENCE.md dari outputs/*.json"),
]:
    p = SUB.add_parser(name); p.set_defaults(func=todo(stage, target))

if __name__ == "__main__":
    args = PARSER.parse_args()
    args.func(args)
