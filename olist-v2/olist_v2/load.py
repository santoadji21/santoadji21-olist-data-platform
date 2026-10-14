"""Load: silver (Parquet di MinIO) → PostgreSQL `raw.*` (ketentuan T2, dibawa dari T0–T6).

  * DDL dibangkitkan dari kontrak ODCS (tipe, NOT NULL) + kolom flag soft; bila definisi tabel di Postgres menyimpang dari
    kontrak, tabel dibuat ulang DI DALAM transaksi yang sama (DDL Postgres transaksional).
  * SEMUA tabel dimuat dalam SATU transaksi: TRUNCATE + COPY. Dijalankan dua kali → jumlah baris sama; gagal di tengah →
    rollback, tidak ada tabel setengah terisi.
  * Jumlah baris diverifikasi (= baris sumber − karantina, dari laporan DQ silver) SEBELUM commit.
  * Tabel berverdict FAIL tidak dimuat (dan load berakhir dengan kode ≠ 0).
"""
from __future__ import annotations

import io
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Iterator

from .bronze import make_s3
from .contract import REPO, Model, load_models
from .rules import SourceError
from .silver import DQ_KEY, SILVER, silver_prefix

SCHEMA = "raw"


# ---------------------------------------------------------------- DDL
def pg_type(contract_type: str) -> str:
    t = contract_type.strip().lower()
    return "double precision" if t == "double" else t


def _format_type(contract_type: str) -> str:
    """Bentuk yang dikembalikan format_type() Postgres, untuk membandingkan tabel yang ada dengan kontrak."""
    t = pg_type(contract_type)
    if t.startswith("varchar("):
        return "character varying(" + t[len("varchar("):]
    return {"timestamp": "timestamp without time zone"}.get(t, t)


def columns(m: Model) -> list[tuple[str, str, bool]]:
    """(nama, tipe kontrak, NOT NULL) + kolom flag soft (boolean NOT NULL)."""
    cols = [(c.name, c.pg_type, c.required) for c in m.columns]
    if m.soft and m.soft_flag:
        cols.append((m.soft_flag, "boolean", True))
    return cols


def ddl(m: Model, schema: str = SCHEMA) -> str:
    body = ",\n  ".join(f'"{n}" {pg_type(t)}{" NOT NULL" if nn else ""}' for n, t, nn in columns(m))
    return f'CREATE TABLE {schema}."{m.name}" (\n  {body}\n)'


def ensure_table(cur, m: Model, schema: str = SCHEMA) -> str:
    """Pastikan tabel sesuai kontrak. Kembalikan 'created' | 'ok' | 'recreated' (menyimpang dari kontrak)."""
    cur.execute("SELECT to_regclass(%s)", (f'{schema}."{m.name}"',))
    exists = cur.fetchone()[0] is not None
    want = [(n, _format_type(t)) for n, t, _ in columns(m)]
    if exists:
        cur.execute("SELECT a.attname, format_type(a.atttypid, a.atttypmod) FROM pg_attribute a "
                    "WHERE a.attrelid = %s::regclass AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                    (f'{schema}."{m.name}"',))
        if [tuple(r) for r in cur.fetchall()] == want:
            return "ok"
        cur.execute(f'DROP TABLE {schema}."{m.name}"')
    cur.execute(ddl(m, schema))
    return "recreated" if exists else "created"


# ---------------------------------------------------------------- data
def to_csv_bytes(table, m: Model) -> bytes:
    """pyarrow Table → CSV tanpa header, urutan kolom = tabel. Timestamp dipaksa ke mikrodetik (Spark menulis INT96/ns)."""
    import pyarrow as pa
    import pyarrow.csv as pacsv

    names = [n for n, _, _ in columns(m)]
    missing = [n for n in names if n not in table.column_names]
    if missing:
        raise SourceError(f"{m.name}: kolom tidak ada di silver: {', '.join(missing)}")
    table = table.select(names)
    for i, f in enumerate(table.schema):
        if pa.types.is_timestamp(f.type) and f.type.unit != "us":
            table = table.set_column(i, f.name, table.column(i).cast(pa.timestamp("us")))
    buf = io.BytesIO()
    pacsv.write_csv(table, buf, pacsv.WriteOptions(include_header=False))
    return buf.getvalue()


def copy_tables(cur, m: Model, tables: Iterable, schema: str = SCHEMA) -> int:
    """TRUNCATE lalu COPY semua potongan tabel; kembalikan jumlah baris di tabel setelahnya (dihitung Postgres)."""
    cur.execute(f'TRUNCATE {schema}."{m.name}"')
    cols = ", ".join(f'"{n}"' for n, _, _ in columns(m))
    for t in tables:
        cur.copy_expert(f'COPY {schema}."{m.name}" ({cols}) FROM STDIN WITH (FORMAT csv, NULL \'\')', io.BytesIO(to_csv_bytes(t, m)))
    cur.execute(f'SELECT count(*) FROM {schema}."{m.name}"')
    return int(cur.fetchone()[0])


def iter_parts(s3, table: str) -> Iterator:
    """Potongan Parquet tabel dari bucket silver (Spark menulis banyak berkas part-*.parquet), satu per satu."""
    import pyarrow.parquet as pq

    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=SILVER, Prefix=silver_prefix(table)):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".parquet")]
    if not keys:
        raise SourceError(f"{table}: tidak ada Parquet di s3://{SILVER}/{silver_prefix(table)} — jalankan `make silver` dulu")
    for k in sorted(keys):
        yield pq.read_table(io.BytesIO(s3.get_object(Bucket=SILVER, Key=k)["Body"].read()))


def read_dq(s3) -> dict:
    try:
        return {t["table"]: t for t in json.loads(s3.get_object(Bucket=SILVER, Key=DQ_KEY)["Body"].read())["tables"]}
    except Exception as e:
        raise SourceError("laporan DQ silver tidak ada — jalankan `make silver` dulu") from e


def expected_rows(dq_table: dict) -> int:
    return dq_table["source_rows"] - dq_table["quarantined"]


def run_load(s3, conn, models: list[Model] | None = None, schema: str = SCHEMA) -> dict:
    models = models or load_models()
    t0 = time.time()
    dq = read_dq(s3)
    skipped = [m.name for m in models if dq.get(m.name, {}).get("verdict") == "FAIL" or m.name not in dq]
    todo = [m for m in models if m.name not in skipped]
    rows = []
    with conn:                                        # satu transaksi: commit di akhir, rollback bila ada exception
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            for m in todo:
                ts = time.time()
                action = ensure_table(cur, m, schema)
                n = copy_tables(cur, m, iter_parts(s3, m.name), schema)
                want = expected_rows(dq[m.name])
                if n != want:                          # exception → rollback seluruh transaksi
                    raise RuntimeError(f"{schema}.{m.name}: {n:,} baris dimuat, seharusnya {want:,} (sumber − karantina)")
                rows.append({"table": m.name, "source_rows": dq[m.name]["source_rows"], "quarantined": dq[m.name]["quarantined"],
                             "expected": want, "loaded": n, "match": True, "table_ddl": action, "seconds": round(time.time() - ts, 2)})
    return {"stage": "load", "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), "seconds": round(time.time() - t0, 2),
            "tables": rows, "skipped_fail": skipped}


def report_md(run: dict) -> str:
    o = ["# Laporan Load ke `raw.*`\n", f"Dijalankan {run['ran_at']} · total {run['seconds']} s · satu transaksi (TRUNCATE + COPY)\n",
         "| Tabel | Baris sumber | Karantina | Seharusnya | Dimuat | Cocok | DDL | Waktu (s) |", "|---|---:|---:|---:|---:|:---:|---|---:|"]
    for r in run["tables"]:
        o.append(f"| raw.{r['table']} | {r['source_rows']:,} | {r['quarantined']:,} | {r['expected']:,} | {r['loaded']:,} | "
                 f"{'ya' if r['match'] else 'TIDAK'} | {r['table_ddl']} | {r['seconds']} |")
    if run["skipped_fail"]:
        o.append(f"\nTidak dimuat (FAIL atau tidak ada di laporan DQ): {', '.join(run['skipped_fail'])}.")
    return "\n".join(o) + "\n"


def write_outputs(outputs: Path, run: dict) -> Path:
    outputs.mkdir(parents=True, exist_ok=True)
    (outputs / "load.json").write_text(json.dumps(run, indent=2), encoding="utf-8")
    md = outputs / "load_report.md"
    md.write_text(report_md(run), encoding="utf-8")
    return md


def main(env=os.environ) -> int:
    import psycopg2

    try:
        conn = psycopg2.connect(env["WAREHOUSE_DSN"], connect_timeout=5)
        try:
            run = run_load(make_s3(env), conn)
        finally:
            conn.close()
    except SourceError as e:
        print(f"load berhenti (Postgres tidak berubah): {e}", file=sys.stderr)
        return 1
    write_outputs(Path(env.get("OLIST_OUTPUTS", REPO / "outputs")), run)
    for r in run["tables"]:
        print(f"  raw.{r['table']:30s} {r['loaded']:>9,} baris  {r['table_ddl']:9s} {r['seconds']:>5} s")
    if run["skipped_fail"]:
        print(f"  TIDAK dimuat: {', '.join(run['skipped_fail'])}")
    print(f"selesai {run['seconds']} s → outputs/load_report.md")
    return 1 if run["skipped_fail"] else 0


if __name__ == "__main__":
    sys.exit(main())
