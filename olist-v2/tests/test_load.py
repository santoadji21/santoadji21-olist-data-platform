"""Load ke raw: logika murni + terhadap Postgres sungguhan (schema sementara; dilewati bila Postgres v2 tidak menyala)."""
import io
import json
import os
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from olist_v2 import load
from olist_v2.contract import Column, Model, Rule, load_models
from olist_v2.rules import SourceError

MODEL = Model("t", "t.csv", 3, "utf-8", ("id",),
              (Column("id", "text", True), Column("zip", "varchar(5)", True), Column("amount", "numeric(10,2)", False),
               Column("ts", "timestamp", False), Column("x", "double", False)),
              (Rule("big", "warning", "amount >= 1000"),), (), 0.0, "has_dq_warning")


def table(rows):
    return pa.table({"id": [r[0] for r in rows], "zip": [r[1] for r in rows], "amount": pa.array([r[2] for r in rows], pa.decimal128(10, 2)),
                     "ts": pa.array([r[3] for r in rows], pa.timestamp("ns")), "x": [r[4] for r in rows], "has_dq_warning": [r[5] for r in rows]})


ROWS = [("a", "01003", Decimal("10.50"), datetime(2017, 1, 1, 10, 0, 0), 1.5, False),
        ("b", "99999", None, None, None, True),
        ("c", "00010", Decimal("1500.00"), datetime(2018, 2, 3, 4, 5, 6), -2.0, True)]


# ---------------------------------------------------------------- murni
def test_ddl_comes_from_contract_with_not_null_flag_and_postgres_types():
    d = load.ddl(MODEL)
    assert '"id" text NOT NULL' in d and '"zip" varchar(5) NOT NULL' in d and '"x" double precision' in d
    assert '"amount" numeric(10,2)\n' in d + "\n" or '"amount" numeric(10,2),' in d
    assert '"has_dq_warning" boolean NOT NULL' in d


def test_every_contract_table_has_a_ddl_and_flags_where_soft_rules_exist():
    for m in load_models():
        d = load.ddl(m)
        assert d.startswith(f'CREATE TABLE raw."{m.name}"') and (m.soft_flag is not None and m.soft_flag in d) == bool(m.soft)


def test_csv_bytes_null_is_unquoted_empty_and_column_order_follows_the_table():
    import csv
    lines = load.to_csv_bytes(table(ROWS), MODEL).decode().splitlines()
    first = next(csv.reader([lines[0]]))
    assert first[:3] == ["a", "01003", "10.50"] and first[3].startswith("2017-01-01 10:00:00") and first[5] == "false"
    assert ",,,," in lines[1] and lines[1].endswith("true")          # NULL = kosong TAK dikutip (bukan string kosong)


def test_missing_column_in_silver_is_a_source_error():
    with pytest.raises(SourceError, match="amount"):
        load.to_csv_bytes(table(ROWS).drop_columns(["amount"]), MODEL)


def test_expected_rows_is_source_minus_quarantine():
    assert load.expected_rows({"source_rows": 100, "quarantined": 3}) == 97


# ---------------------------------------------------------------- Postgres sungguhan
def _dsn():
    if os.environ.get("OLIST_TEST_DSN"):
        return os.environ["OLIST_TEST_DSN"]
    env = {}
    p = Path(__file__).resolve().parent.parent / ".env"
    if p.exists():
        env = dict(l.split("=", 1) for l in p.read_text().splitlines() if "=" in l and not l.startswith("#"))
    return f"host=localhost port={env.get('V2_PG_PORT', '15432')} dbname=olist_dw user={env.get('V2_PG_USER', 'olist')} password={env.get('V2_PG_PASSWORD', '')}"


@pytest.fixture
def pg():
    import psycopg2
    try:
        conn = psycopg2.connect(_dsn(), connect_timeout=3)
    except psycopg2.OperationalError as e:
        pytest.skip(f"Postgres v2 tidak bisa dihubungi: {e}")
    schema = f"zz_load_{uuid.uuid4().hex[:8]}"
    with conn, conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA {schema}")
    yield conn, schema
    conn.rollback()
    with conn, conn.cursor() as cur:
        cur.execute(f"DROP SCHEMA {schema} CASCADE")
    conn.close()


def count(conn, schema):
    with conn, conn.cursor() as cur:
        cur.execute(f'SELECT count(*) FROM {schema}."t"')
        return cur.fetchone()[0]


def test_ensure_table_creates_then_is_ok_then_recreates_on_drift(pg):
    conn, schema = pg
    with conn, conn.cursor() as cur:
        assert load.ensure_table(cur, MODEL, schema) == "created"
    with conn, conn.cursor() as cur:
        assert load.ensure_table(cur, MODEL, schema) == "ok"
        cur.execute(f'ALTER TABLE {schema}."t" ADD COLUMN extra int')            # menyimpang dari kontrak
    with conn, conn.cursor() as cur:
        assert load.ensure_table(cur, MODEL, schema) == "recreated"


def test_copy_loads_nulls_booleans_decimals_and_timestamps_and_is_idempotent(pg):
    conn, schema = pg
    with conn, conn.cursor() as cur:
        load.ensure_table(cur, MODEL, schema)
        assert load.copy_tables(cur, MODEL, [table(ROWS)], schema) == 3
        assert load.copy_tables(cur, MODEL, [table(ROWS)], schema) == 3          # TRUNCATE + COPY: tidak dobel
        cur.execute(f'SELECT zip, amount, ts, x, has_dq_warning FROM {schema}."t" ORDER BY id')
        rows = cur.fetchall()
    assert rows[0] == ("01003", Decimal("10.50"), datetime(2017, 1, 1, 10, 0), 1.5, False)
    assert rows[1] == ("99999", None, None, None, True)


def test_failure_midway_rolls_back_and_leaves_previous_data(pg):
    conn, schema = pg
    with conn, conn.cursor() as cur:
        load.ensure_table(cur, MODEL, schema)
        load.copy_tables(cur, MODEL, [table(ROWS)], schema)

    def boom():
        yield table(ROWS[:1])
        raise RuntimeError("gangguan di tengah")

    with pytest.raises(RuntimeError):
        with conn, conn.cursor() as cur:
            load.copy_tables(cur, MODEL, boom(), schema)
    assert count(conn, schema) == 3                                               # rollback: data lama utuh


class FakeS3:
    def __init__(self, parquet_parts, dq):
        self.o = {f"olist/t/part-{i}.parquet": b for i, b in enumerate(parquet_parts)}
        self.o["olist/_dq/dq.json"] = json.dumps({"tables": dq}).encode()

    def get_paginator(self, _):
        outer = self

        class P:
            def paginate(self, Bucket, Prefix):
                return [{"Contents": [{"Key": k} for k in outer.o if k.startswith(Prefix)]}]
        return P()

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.o[Key])}


def parquet(rows):
    b = io.BytesIO()
    pq.write_table(table(rows), b)
    return b.getvalue()


def dq(verdict="PASS", source=3, quarantined=0):
    return [{"table": "t", "verdict": verdict, "source_rows": source, "quarantined": quarantined}]


def test_run_load_reads_multiple_parquet_parts_and_verifies_expected_rows(pg):
    conn, schema = pg
    s3 = FakeS3([parquet(ROWS[:2]), parquet(ROWS[2:])], dq(source=4, quarantined=1))           # 4 − 1 = 3
    run = load.run_load(s3, conn, [MODEL], schema)
    assert run["tables"][0]["loaded"] == 3 and run["tables"][0]["match"] and count(conn, schema) == 3


def test_count_mismatch_rolls_back_everything(pg):
    conn, schema = pg
    with pytest.raises(RuntimeError, match="seharusnya 99"):
        load.run_load(FakeS3([parquet(ROWS)], dq(source=99)), conn, [MODEL], schema)
    with conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f'{schema}."t"',))
        assert cur.fetchone()[0] is None                                          # tabel pun tidak tertinggal


def test_fail_table_is_not_loaded(pg):
    conn, schema = pg
    run = load.run_load(FakeS3([parquet(ROWS)], dq(verdict="FAIL")), conn, [MODEL], schema)
    assert run["skipped_fail"] == ["t"] and run["tables"] == []


def test_missing_dq_report_stops_before_touching_postgres(pg):
    conn, schema = pg

    class NoDq(FakeS3):
        def get_object(self, Bucket, Key):
            raise KeyError(Key)
    with pytest.raises(SourceError, match="laporan DQ"):
        load.run_load(NoDq([], []), conn, [MODEL], schema)
