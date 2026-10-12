"""Smoke test seluruh platform: service healthy, tiap lapisan konsisten, stream benar. Exit code ≠ 0 bila ada yang gagal.

Dipakai oleh `pipeline.py smoke` dan sebagai pemeriksaan pemulihan setelah failure injection.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

from .bronze import read_manifest
from .schema import TABLES

SERVICES = ["postgres", "kafka", "minio", "kafka-ui", "jupyter"]


def _check(name: str, ok: bool, detail: str = "") -> dict:
    return {"check": name, "ok": bool(ok), "detail": detail}


def run_smoke(env: dict, conn, s3, root: Path) -> list[dict]:
    from .model import integrity_checks
    from .silver import DQ_KEY
    from .stream_report import collect

    out: list[dict] = []
    for svc in SERVICES:
        r = subprocess.run(["docker", "inspect", f"olist-{svc}", "--format", "{{.State.Health.Status}}"], capture_output=True, text=True)
        out.append(_check(f"service {svc} healthy", r.returncode == 0 and r.stdout.strip() == "healthy", r.stdout.strip() or r.stderr.strip()[:60]))

    missing = [t.name for t in TABLES if read_manifest(s3, "bronze", t.name) is None]
    out.append(_check("bronze: 9 manifest ada", not missing, f"hilang: {missing}" if missing else "9/9"))

    try:
        dq = {t["table"]: t for t in json.loads(s3.get_object(Bucket="silver", Key=DQ_KEY)["Body"].read())["tables"]}
    except Exception as e:
        dq = {}
        out.append(_check("silver: laporan DQ ada", False, str(e)[:80]))
    if dq:
        fails = [n for n, t in dq.items() if t["verdict"] == "FAIL"]
        out.append(_check("silver: tidak ada tabel FAIL", not fails, f"FAIL: {fails}" if fails else "9/9 lolos"))
        bad = []
        with conn.cursor() as cur:
            for t in TABLES:
                cur.execute(f"SELECT count(*) FROM raw.{t.name}")
                n = cur.fetchone()[0]
                d = dq.get(t.name)
                if d is None or n != d["source_rows"] - d["quarantined"]:
                    bad.append(f"{t.name}={n}")
        out.append(_check("raw: baris = sumber − karantina (9 tabel)", not bad, ", ".join(bad) or "9/9 cocok"))

    try:
        chk = integrity_checks(conn, root)
        bad = [c["check"] for c in chk if c["bad_rows"]]
        out.append(_check("dw: pemeriksaan integritas = 0", not bad, f"{len(chk)} pemeriksaan" if not bad else f"gagal: {bad}"))
    except Exception as e:
        conn.rollback()
        out.append(_check("dw: pemeriksaan integritas = 0", False, str(e).splitlines()[0][:80]))
    try:
        with conn.cursor() as cur:
            cur.execute((root / "sql" / "checks" / "mart_checks.sql").read_text(encoding="utf-8"))
            chk = cur.fetchall()
        bad = [n for n, b in chk if b]
        out.append(_check("mart: pemeriksaan = 0", not bad, f"{len(chk)} pemeriksaan" if not bad else f"gagal: {bad}"))
    except Exception as e:
        conn.rollback()
        out.append(_check("mart: pemeriksaan = 0", False, str(e).splitlines()[0][:80]))

    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM stream.order_current")
        has_stream = cur.fetchone()[0] > 0
    if has_stream:
        r = collect(env, conn, root / "outputs" / "stream_truth.json")
        out.append(_check("stream: 4 pemeriksaan producer vs consumer", all(r["checks"].values()),
                          ", ".join(k for k, v in r["checks"].items() if not v) or "4/4 cocok"))
    else:
        out.append(_check("stream: ada data", False, "stream.order_current kosong — jalankan `make stream`"))
    return out
