"""T2 — Load: silver (Parquet di MinIO) → PostgreSQL `raw.*`.

  * DDL dibangkitkan dari `olist/schema.py` (satu sumber kebenaran) + kolom flag dari gate.
  * Semua tabel dimuat dalam SATU transaksi: TRUNCATE + COPY. Dijalankan dua kali → baris sama;
    gagal di tengah → rollback, tidak ada tabel setengah terisi.
  * Jumlah baris diverifikasi (= baris sumber − karantina) di dalam transaksi sebelum commit.
  * Tabel dengan verdict FAIL tidak dimuat.
"""
from __future__ import annotations

import io
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .gate import SourceError, flag_columns
from .schema import TABLES, Table
from .silver import DQ_KEY, SILVER, read_silver

CHUNK_ROWS = 100_000


def pg_dsn(env: dict[str, str]) -> str:
    return (f"host={env.get('POSTGRES_HOST', 'localhost')} port={env.get('POSTGRES_PORT', '5432')} "
            f"dbname={env.get('POSTGRES_DB', 'olist')} user={env.get('POSTGRES_USER', 'olist')} "
            f"password={env.get('POSTGRES_PASSWORD', '')}")


def columns(t: Table) -> list[tuple[str, str]]:
    cols = [(c.src, c.type + ("" if c.nullable else " NOT NULL")) for c in t.cols]
    return cols + [(f, "boolean NOT NULL") for f in flag_columns(t)]


def ddl(t: Table) -> str:
    body = ",\n  ".join(f'"{n}" {typ}' for n, typ in columns(t))
    return f"CREATE TABLE IF NOT EXISTS raw.{t.name} (\n  {body}\n)"


def _copy_df(cur, t: Table, df: pd.DataFrame) -> None:
    names = [n for n, _ in columns(t)]
    stmt = f"COPY raw.{t.name} ({', '.join(chr(34) + n + chr(34) for n in names)}) FROM STDIN WITH (FORMAT csv, NULL '')"
    with cur.copy(stmt) as cp:
        for i in range(0, len(df), CHUNK_ROWS):
            buf = io.StringIO()
            df.iloc[i:i + CHUNK_ROWS][names].to_csv(buf, index=False, header=False)
            cp.write(buf.getvalue())


def read_dq(s3) -> dict:
    try:
        return json.loads(s3.get_object(Bucket=SILVER, Key=DQ_KEY)["Body"].read())
    except Exception as e:
        raise SourceError("laporan DQ silver tidak ada — jalankan `python pipeline.py silver` dulu") from e


def run_load(s3, conn) -> dict:
    t0 = time.time()
    dq = {t["table"]: t for t in read_dq(s3)["tables"]}
    skipped = [n for n, t in dq.items() if t["verdict"] == "FAIL"]
    todo = [t for t in TABLES if t.name in dq and t.name not in skipped]

    # Siapkan SEMUA data dulu; Postgres baru disentuh setelah semuanya siap.
    frames = {t.name: read_silver(s3, t.name) for t in todo}

    rows: list[dict] = []
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute("CREATE SCHEMA IF NOT EXISTS raw")
            for t in todo:
                ts = time.time()
                cur.execute(ddl(t))
                cur.execute(f"TRUNCATE raw.{t.name}")
                _copy_df(cur, t, frames[t.name])
                cur.execute(f"SELECT count(*) FROM raw.{t.name}")
                n = cur.fetchone()[0]
                d = dq[t.name]
                expected = d["source_rows"] - d["quarantined"]
                if n != expected:  # exception → rollback seluruh transaksi
                    raise RuntimeError(f"raw.{t.name}: {n:,} baris dimuat, seharusnya {expected:,} (sumber − karantina)")
                rows.append({"table": t.name, "source_rows": d["source_rows"], "quarantined": d["quarantined"],
                             "expected": expected, "loaded": n, "match": True, "seconds": round(time.time() - ts, 2)})
    return {"stage": "load", "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "seconds": round(time.time() - t0, 2), "tables": rows, "skipped_fail": skipped}


def report_md(run: dict) -> str:
    o = ["# Laporan Load ke `raw.*` (T2)\n",
         f"Dijalankan {run['ran_at']} · total {run['seconds']} s · satu transaksi (TRUNCATE + COPY)\n",
         "| Tabel | Baris sumber | Karantina | Seharusnya | Dimuat | Cocok | Waktu (s) |",
         "|---|---:|---:|---:|---:|:---:|---:|"]
    for r in run["tables"]:
        o.append(f"| raw.{r['table']} | {r['source_rows']:,} | {r['quarantined']:,} | {r['expected']:,} | "
                 f"{r['loaded']:,} | {'ya' if r['match'] else 'TIDAK'} | {r['seconds']} |")
    if run["skipped_fail"]:
        o.append(f"\nTidak dimuat (verdict FAIL): {', '.join(run['skipped_fail'])}.")
    return "\n".join(o) + "\n"


def write_outputs(root: Path, run: dict) -> Path:
    out = root / "outputs"
    out.mkdir(exist_ok=True)
    (out / "load.json").write_text(json.dumps(run, indent=2), encoding="utf-8")
    md = out / "load_report.md"
    md.write_text(report_md(run), encoding="utf-8")
    return md
