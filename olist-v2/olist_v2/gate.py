"""Gate kontrak: `datacontract test` terhadap silver di MinIO (server `silver-minio` di kontrak ODCS).

  * `failed`/`error` → gate GAGAL (DAG berhenti, tidak ada `load_raw`).
  * `warning` → lolos: pelanggaran soft yang dikenal (dilaporkan apa adanya) dan pemeriksaan tipe pada Parquet yang
    belum didukung datacontract-cli (dilaporkan sebagai KETERBATASAN, bukan disembunyikan; tipe silver diverifikasi
    oleh gate Spark di silver.py).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .contract import REPO, contract_path

SERVER = "silver-minio"
UNSUPPORTED = "not supported yet"


def summarize(result: dict) -> dict:
    checks = result.get("checks", [])
    failed = [c for c in checks if c.get("result") in ("failed", "error")]
    warn = [c for c in checks if c.get("result") == "warning"]
    unsupported = [c for c in warn if UNSUPPORTED in (c.get("reason") or "")]
    soft = [c for c in warn if c not in unsupported]

    def item(c):
        return {"model": c.get("model") or c.get("field"), "name": c.get("name"), "reason": c.get("reason")}

    return {"result": result.get("result"), "checks": len(checks),
            "passed": sum(1 for c in checks if c.get("result") == "passed"),
            "failed": [item(c) for c in failed], "soft_violations": [item(c) for c in soft],
            "unsupported_type_checks": len(unsupported)}


def run_gate(env=os.environ, runner: Callable = subprocess.run) -> dict:
    exe = env.get("DATACONTRACT_BIN", "datacontract")
    run_env = {**env, "DATACONTRACT_S3_ACCESS_KEY_ID": env["MINIO_ROOT_USER"], "DATACONTRACT_S3_SECRET_ACCESS_KEY": env["MINIO_ROOT_PASSWORD"]}
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "result.json"
        r = runner([exe, "test", "--server", SERVER, "--output", str(out), "--output-format", "json", str(contract_path())],
                   capture_output=True, text=True, env=run_env)
        if not out.exists():
            raise RuntimeError(f"datacontract test tidak menghasilkan hasil (exit {r.returncode}): {(r.stderr or r.stdout)[-400:]}")
        summary = summarize(json.loads(out.read_text(encoding="utf-8")))
    summary.update({"ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "server": SERVER, "exit_code": r.returncode})
    return summary


def report_md(s: dict) -> str:
    o = ["# Laporan Gate Kontrak (datacontract test)\n", f"Dijalankan {s['ran_at']} · server `{s['server']}` (silver di MinIO)\n",
         f"**Hasil: `{s['result']}`** — {s['checks']} pemeriksaan: {s['passed']} lolos, {len(s['failed'])} gagal, "
         f"{len(s['soft_violations'])} peringatan soft, {s['unsupported_type_checks']} pemeriksaan tipe tidak didukung.\n"]
    if s["failed"]:
        o += ["## Gagal (menghentikan DAG)\n"] + [f"- `{x['model']}`: {x['name']} — {x['reason']}" for x in s["failed"]] + [""]
    o += ["## Pelanggaran soft yang dikenal (dicatat, baris tetap lewat)\n", "| Tabel | Aturan | Detail |", "|---|---|---|"]
    o += [f"| {x['model']} | {x['name']} | {x['reason']} |" for x in s["soft_violations"]] or ["| — | — | — |"]
    o.append(f"\n## Keterbatasan\n\n{s['unsupported_type_checks']} pemeriksaan tipe bertanda *not supported yet*: datacontract-cli belum "
             "dapat memeriksa tipe pada file Parquet. Tipe silver diverifikasi oleh gate Spark (`outputs/dq_report.md`) dan test-nya.")
    return "\n".join(o) + "\n"


def write_outputs(outputs: Path, s: dict) -> Path:
    outputs.mkdir(parents=True, exist_ok=True)
    (outputs / "contract_gate.json").write_text(json.dumps(s, indent=2, ensure_ascii=False), encoding="utf-8")
    md = outputs / "contract_gate.md"
    md.write_text(report_md(s), encoding="utf-8")
    return md


def main(env=os.environ) -> int:
    try:
        s = run_gate(env)
    except RuntimeError as e:
        print(f"gate berhenti: {e}", file=sys.stderr)
        return 1
    write_outputs(Path(env.get("OLIST_OUTPUTS", REPO / "outputs")), s)
    print(f"gate: {s['result']} — {s['passed']}/{s['checks']} lolos, {len(s['failed'])} gagal, {len(s['soft_violations'])} soft, "
          f"{s['unsupported_type_checks']} tipe tidak didukung → outputs/contract_gate.md")
    return 1 if s["failed"] or s["result"] in ("failed", "error") else 0


if __name__ == "__main__":
    sys.exit(main())
