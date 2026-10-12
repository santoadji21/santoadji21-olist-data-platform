"""T2 — Silver: bronze → data bertipe + gate kualitas → Parquet di bucket silver (+ karantina + laporan).

Silver bisa dibangun ulang dari bronze kapan saja: input = objek `latest` di manifest bronze (diverifikasi sha256),
output ditimpa pada kunci tetap (idempoten). Baris bermasalah TIDAK dibuang: masuk Parquet karantina dengan alasan.
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .bronze import PREFIX, ensure_bucket, read_manifest
from .contract import PROCESS_ORDER
from .gate import SourceError, TableResult, check_header, process_table
from .schema import TABLES, Table

BRONZE, SILVER = "bronze", "silver"
BY_NAME = {t.name: t for t in TABLES}


def silver_key(table: str) -> str:
    return f"{PREFIX}/{table}/{table}.parquet"


def quarantine_key(table: str) -> str:
    return f"{PREFIX}/_quarantine/{table}.parquet"


DQ_KEY = f"{PREFIX}/_dq/dq.json"


def fetch_latest(s3, t: Table) -> tuple[bytes, dict]:
    """Ambil versi terbaru tabel dari bronze (via manifest) dan pastikan sha256-nya cocok."""
    m = read_manifest(s3, BRONZE, t.name)
    if not m or not m.get("versions"):
        raise SourceError(f"{t.name}: belum ada di bronze — jalankan `python pipeline.py bronze` dulu")
    v = m["versions"][-1]
    body = s3.get_object(Bucket=BRONZE, Key=v["object_key"])["Body"].read()
    if hashlib.sha256(body).hexdigest() != v["sha256"]:
        raise SourceError(f"{t.name}: isi objek bronze tidak cocok dengan sha256 di manifest ({v['object_key']})")
    return body, v


def header_of(body: bytes, t: Table) -> list[str]:
    text = body[:65536].decode(t.encoding, errors="replace")
    return next(csv.reader(io.StringIO(text, newline="")), [])


def read_raw(body: bytes, t: Table) -> pd.DataFrame:
    """Semua kolom sebagai TEKS; konversi tipe adalah keputusan kontrak, bukan default pandas."""
    return pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False, encoding=t.encoding)


def _parquet(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False)
    return buf.getvalue()


def run_silver(s3, tables=TABLES, order=PROCESS_ORDER) -> dict:
    t0 = time.time()
    names = [n for n in order if n in {t.name for t in tables}]
    bodies: dict[str, tuple[bytes, dict]] = {}
    for n in names:  # 1) ambil semua + cek kolom SEBELUM menulis apa pun
        t = BY_NAME[n]
        body, ver = fetch_latest(s3, t)
        check_header(t, header_of(body, t))
        bodies[n] = (body, ver)

    ensure_bucket(s3, SILVER)
    parents: dict[str, pd.DataFrame] = {}
    results: list[TableResult] = []
    for n in names:  # 2) induk sebelum anak
        t = BY_NAME[n]
        body, ver = bodies[n]
        r = process_table(t, read_raw(body, t), parents)
        results.append(r)
        parents[n] = r.silver
        meta = {"Metadata": {"source_sha256": ver["sha256"], "source_version": str(ver["version"])}}
        if r.quarantined:
            s3.put_object(Bucket=SILVER, Key=quarantine_key(n), Body=_parquet(r.quarantine), **meta)
        else:  # tidak ada karantina → buang sisa karantina run lama agar tidak menyesatkan
            s3.delete_object(Bucket=SILVER, Key=quarantine_key(n))
        if r.verdict == "FAIL":  # sumber rusak: jangan sediakan silver (load tidak akan memuatnya)
            s3.delete_object(Bucket=SILVER, Key=silver_key(n))
        else:
            s3.put_object(Bucket=SILVER, Key=silver_key(n), Body=_parquet(r.silver), **meta)
        r.source = {"version": ver["version"], "sha256": ver["sha256"]}

    run = {
        "stage": "silver", "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "seconds": round(time.time() - t0, 2),
        "tables": [{**r.as_dict(), "source": r.source} for r in results],
    }
    s3.put_object(Bucket=SILVER, Key=DQ_KEY, Body=json.dumps(run, indent=2).encode(), ContentType="application/json")
    return run


def read_silver(s3, table: str) -> pd.DataFrame:
    body = s3.get_object(Bucket=SILVER, Key=silver_key(table))["Body"].read()
    return pd.read_parquet(io.BytesIO(body))


def report_md(run: dict) -> str:
    o = ["# Laporan Kualitas Data (T2 — gate silver)\n",
         f"Dijalankan {run['ran_at']} · {run['seconds']} s\n",
         "Verdict: `PASS` (tanpa pelanggaran hard) · `PASS+KARANTINA` (ada baris dikarantina, di bawah ambang) · "
         "`FAIL` (di atas ambang / kunci tidak unik / jumlah baris menyimpang → tabel tidak dimuat).\n",
         "## Ringkasan per tabel\n",
         "| Tabel | Baris sumber | Karantina | Lolos (silver) | Karantina % | Ambang % | Verdict |",
         "|---|---:|---:|---:|---:|---:|---|"]
    for t in run["tables"]:
        o.append(f"| {t['table']} | {t['source_rows']:,} | {t['quarantined']:,} | {t['clean']:,} | "
                 f"{t['quarantine_pct']:.3f} | {t['threshold_pct']} | **{t['verdict']}** |")
    fails = [t for t in run["tables"] if t["fail_reasons"]]
    if fails:
        o.append("\n## Alasan FAIL\n")
        for t in fails:
            for why in t["fail_reasons"]:
                o.append(f"- `{t['table']}`: {why}")
    o.append("\n## Aturan\n")
    o.append("| Tabel | Aturan | Jenis | Tindakan | Pelanggaran | % |")
    o.append("|---|---|---|---|---:|---:|")
    for t in run["tables"]:
        for r in t["rules"]:
            o.append(f"| {r['table']} | `{r['name']}` | {r['kind']} | {r['action']} | {r['violations']:,} | {r['pct']:.3f} |")
    hard = sum(1 for t in run["tables"] for r in t["rules"] if r["kind"] == "hard")
    soft = sum(1 for t in run["tables"] for r in t["rules"] if r["kind"] == "soft")
    o.append(f"\nTotal aturan: {hard} hard, {soft} soft (minimal tugas: 10 hard, 8 soft).")
    o.append("\nKarantina: `silver/olist/_quarantine/<tabel>.parquet` (kolom sumber apa adanya + `_row` + `_reasons`).")
    return "\n".join(o) + "\n"


def write_outputs(root: Path, run: dict) -> Path:
    out = root / "outputs"
    out.mkdir(exist_ok=True)
    (out / "dq.json").write_text(json.dumps(run, indent=2, ensure_ascii=False), encoding="utf-8")
    md = out / "dq_report.md"
    md.write_text(report_md(run), encoding="utf-8")
    return md
