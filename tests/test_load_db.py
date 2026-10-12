"""Test load dengan Docker (Postgres + MinIO): di-skip bila stack mati."""
import pytest

from olist import load as load_mod
from olist.bronze import make_s3
from pipeline import read_env
from tests.conftest import q1


@pytest.fixture(scope="module")
def s3():
    c = make_s3(read_env())
    try:
        c.head_bucket(Bucket="silver")
        c.head_object(Bucket="silver", Key=load_mod.DQ_KEY)
    except Exception as e:
        pytest.skip(f"silver belum ada di MinIO (jalankan `make silver`): {e}")
    return c


def test_load_twice_gives_same_rowcount(db, s3):
    import psycopg
    with psycopg.connect(_dsn()) as conn:
        a = load_mod.run_load(s3, conn)
    with psycopg.connect(_dsn()) as conn:
        b = load_mod.run_load(s3, conn)
    assert {t["table"]: t["loaded"] for t in a["tables"]} == {t["table"]: t["loaded"] for t in b["tables"]}
    assert q1(db, "select count(*) from raw.orders") == 99_441


def test_failure_midway_leaves_tables_untouched(db, s3, monkeypatch):
    import psycopg
    before = q1(db, "select count(*) from raw.customers")
    real, calls = load_mod._copy_df, {"n": 0}

    def boom(cur, t, df):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("gangguan simulasi")
        real(cur, t, df)

    monkeypatch.setattr(load_mod, "_copy_df", boom)
    with psycopg.connect(_dsn()) as conn, pytest.raises(RuntimeError):
        load_mod.run_load(s3, conn)
    assert q1(db, "select count(*) from raw.customers") == before   # tabel pertama tidak setengah terisi/kosong


def _dsn():
    return load_mod.pg_dsn(read_env())
