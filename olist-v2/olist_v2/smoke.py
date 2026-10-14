"""Smoke test seluruh platform: service sehat, tiap lapisan konsisten, stream benar. Exit code ≠ 0 bila ada yang gagal.

Dijalankan dari laptop (`make smoke`) dan sebagai pemeriksaan pemulihan setelah failure injection.
"""
from __future__ import annotations

import json
import subprocess
from pathlib import Path

from .bronze import read_manifest
from .contract import load_models
from .marts import run_checks, run_failing_rows, run_test_sql
from .silver import DQ_KEY

SERVICES = ["postgres", "minio", "kafka", "airflow-webserver", "airflow-scheduler"]


def _check(name: str, ok: bool, detail: str = "") -> dict:
    return {"check": name, "ok": bool(ok), "detail": detail}


def run_smoke(env: dict, conn, s3, root: Path) -> list[dict]:
    from .stream_report import collect

    out: list[dict] = []
    for svc in SERVICES:
        r = subprocess.run(["docker", "inspect", f"olist-v2-{svc}", "--format", "{{.State.Health.Status}}"], capture_output=True, text=True)
        out.append(_check(f"service {svc} healthy", r.returncode == 0 and r.stdout.strip() == "healthy", r.stdout.strip() or r.stderr.strip()[:60]))

    names = [m.name for m in load_models()]
    missing = [n for n in names if read_manifest(s3, "bronze", n) is None]
    out.append(_check(f"bronze: {len(names)} manifest ada", not missing, f"hilang: {missing}" if missing else f"{len(names)}/{len(names)}"))

    try:
        dq = {t["table"]: t for t in json.loads(s3.get_object(Bucket="silver", Key=DQ_KEY)["Body"].read())["tables"]}
    except Exception as e:
        dq = {}
        out.append(_check("silver: laporan DQ ada", False, str(e)[:80]))
    if dq:
        fails = [n for n, t in dq.items() if t["verdict"] == "FAIL"]
        out.append(_check("silver: tidak ada tabel FAIL", not fails, f"FAIL: {fails}" if fails else f"{len(dq)}/{len(dq)} lolos"))
        bad = []
        with conn.cursor() as cur:
            for n in names:
                cur.execute(f'SELECT count(*) FROM raw."{n}"')
                c = cur.fetchone()[0]
                d = dq.get(n)
                if d is None or c != d["source_rows"] - d["quarantined"]:
                    bad.append(f"{n}={c}")
        out.append(_check("raw: baris = sumber − karantina", not bad, ", ".join(bad) or f"{len(names)}/{len(names)} cocok"))

    for label, fn in (("dw: pemeriksaan integritas = 0", lambda: run_test_sql(conn, root / "dbt" / "tests" / "dw_integrity.sql")),
                      ("dw: angka cek tugas (96.096 / 99.441 / 112.650 / 775 / 1)", lambda: run_failing_rows(conn, root / "dbt" / "tests" / "dw_check_numbers.sql")),
                      ("mart: pemeriksaan = 0", lambda: run_checks(conn, root / "dbt"))):
        try:
            chk = fn()
            bad = [c["check"] for c in chk if c["bad_rows"]]
            out.append(_check(label, not bad, f"{len(chk)} pemeriksaan" if not bad else f"gagal: {bad}"))
        except Exception as e:
            conn.rollback()
            out.append(_check(label, False, str(e).splitlines()[0][:80]))

    conn.rollback()
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
