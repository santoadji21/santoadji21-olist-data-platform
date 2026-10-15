"""T6 — satu pintu dari nol sampai dashboard, dengan waktu tiap tahap (batas tugas: ≤ 15 menit dan ≤ 10 perintah).

  make all            # warm: stack dibiarkan, hanya jalankan 10 perintah
  make all YES=1      # fresh: `make reset` (down -v, CSV tetap) lalu 10 perintah
Hasil: outputs/run_all.json — dibaca oleh olist_v2.evidence.
"""
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
# Tepat 10 perintah (reset dan `make init` tidak dihitung: init otomatis di `up`; reset adalah titik awal "down -v").
STAGES = [
    ("up", ["make", "up", "P=stream bi"]),
    ("download", ["make", "download"]),
    ("bronze", ["make", "bronze"]),
    ("silver", ["make", "silver"]),
    ("gate", ["make", "gate"]),
    ("load", ["make", "load"]),
    ("stream", ["make", "stream"]),
    ("dbt", ["make", "dbt-build"]),
    ("marts", ["make", "marts"]),
    ("dashboard", ["make", "dashboard"]),
]


def wait_healthy(names=("olist-v2-postgres", "olist-v2-minio", "olist-v2-kafka", "olist-v2-airflow-scheduler", "olist-v2-metabase"), timeout=420) -> float:
    """`up -d` kembali sebelum service sehat; tunggu (dihitung ke durasi tahap `up`) agar tahap berikutnya tidak gagal."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        ok = True
        for n in names:
            r = subprocess.run(["docker", "inspect", n, "--format", "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}"],
                               capture_output=True, text=True)
            ok &= r.stdout.strip() in ("healthy", "running")
        if ok:
            return round(time.time() - t0, 1)
        time.sleep(3)
    raise RuntimeError(f"service belum sehat setelah {timeout} s: lihat `make ps`")


def main(argv=None) -> int:
    fresh = "--fresh" in (argv if argv is not None else sys.argv[1:])
    started = datetime.now(timezone.utc)
    t_all = time.time()
    reset_s = None
    if fresh:
        t0 = time.time()
        if subprocess.run(["make", "reset", "YES=1"], cwd=ROOT).returncode != 0:
            return 1
        reset_s = round(time.time() - t0, 1)
    stages = []
    for name, cmd in STAGES:
        print(f"\n=== [{name}] {' '.join(cmd)}", flush=True)
        t0 = time.time()
        rc = subprocess.run(cmd, cwd=ROOT).returncode
        if rc == 0 and name == "up":
            wait_healthy()
        stages.append({"stage": name, "command": " ".join(cmd), "seconds": round(time.time() - t0, 1), "exit": rc})
        if rc != 0:
            print(f"\nBERHENTI di tahap {name} (exit {rc})", file=sys.stderr)
            break
    total = round(sum(s["seconds"] for s in stages), 1)
    run = {"started_at": started.isoformat(timespec="seconds"), "fresh": fresh, "reset_seconds": reset_s, "stages": stages,
           "total_seconds": total, "wall_seconds": round(time.time() - t_all, 1), "commands": len(stages),
           "ok": len(stages) == len(STAGES) and all(s["exit"] == 0 for s in stages)}
    (ROOT / "outputs").mkdir(exist_ok=True)
    (ROOT / "outputs" / "run_all.json").write_text(json.dumps(run, indent=2), encoding="utf-8")
    print(f"\n{'SELESAI' if run['ok'] else 'GAGAL'}: {len(stages)} perintah, {total} s ({round(total / 60, 1)} menit)"
          f"{' setelah reset' if fresh else ' (tanpa reset)'} → outputs/run_all.json")
    return 0 if run["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
