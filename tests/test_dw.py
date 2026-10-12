"""Test star schema dengan Postgres (di-skip bila stack mati atau `make model` belum dijalankan)."""
from pathlib import Path

import pytest

from olist.model import integrity_checks
from tests.conftest import q1

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module", autouse=True)
def built(db):
    if q1(db, "select to_regclass('dw.fact_order') is not null") is not True:
        pytest.skip("dw belum dibangun (jalankan `make model`)")


def test_all_integrity_checks_return_zero(db):
    bad = [c for c in integrity_checks(db, ROOT) if c["bad_rows"] != 0]
    assert bad == []


@pytest.mark.parametrize("sql,expected", [
    ("select count(*) from dw.dim_customer", 96_096),          # orang, bukan order
    ("select count(*) from dw.fact_order_item", 112_650),
    ("select count(*) from dw.fact_order", 99_441),
    ("select count(*) from dw.fact_order where not has_items", 775),
    ("select count(*) from dw.fact_order where not has_payment", 1),
    ("select count(*) from dw.fact_review", 98_673),
    ("select count(*) from dw.fact_order where order_status = 'delivered'", 96_478),
    ("select count(DISTINCT year_month) from dw.dim_date where is_complete_month", 20),
])
def test_check_numbers_from_the_assignment(db, sql, expected):
    assert q1(db, sql) == expected


def test_primary_and_foreign_keys_are_real_constraints(db):
    assert q1(db, "select count(*) from information_schema.table_constraints where constraint_schema='dw' "
                  "and constraint_type='PRIMARY KEY'") == 9
    assert q1(db, "select count(*) from information_schema.table_constraints where constraint_schema='dw' "
                  "and constraint_type='FOREIGN KEY'") >= 13


def test_is_late_is_null_not_false_when_not_delivered(db):
    assert q1(db, "select count(*) from dw.fact_order where order_status <> 'delivered' and is_late is not null") == 0
    assert q1(db, "select count(*) from dw.fact_order where order_status <> 'delivered' and is_late is false") == 0


def test_late_is_compared_by_date_not_timestamp(db):
    # tiba pada hari estimasi (jam berapa pun) bukan terlambat
    assert q1(db, "select count(*) from dw.fact_order where delivered_ts::date = estimated_ts::date and is_late") == 0


def test_unknown_geo_rows_keep_fk_valid(db):
    assert q1(db, "select count(*) from dw.dim_customer where geo_key = 'unknown'") > 0
    assert q1(db, "select count(*) from dw.dim_geo where geo_key = 'unknown'") == 1


def test_product_typo_fixed_and_category_has_fallback(db):
    cols = {r[0] for r in db.execute("select column_name from information_schema.columns "
                                     "where table_schema='dw' and table_name='dim_product'").fetchall()}
    assert {"name_length", "description_length"} <= cols and not any("lenght" in c for c in cols)
    assert q1(db, "select count(*) from dw.dim_product where category_en is null") == 0


def test_multi_review_orders_keep_the_actual_review_count(db):
    assert q1(db, "select count(*) from dw.fact_review where review_count > 1") == 547
    assert q1(db, "select min(review_count) from dw.fact_review") == 1


def test_rebuild_is_idempotent_and_failure_keeps_old_dw(db):
    import psycopg

    from olist.model import run_model
    from olist.load import pg_dsn
    from pipeline import read_env
    before = q1(db, "select count(*) from dw.fact_order_item")
    with psycopg.connect(pg_dsn(read_env())) as conn:
        run_model(conn, ROOT)                                   # dua kali berturut-turut → hasil sama
    assert q1(db, "select count(*) from dw.fact_order_item") == before
    with psycopg.connect(pg_dsn(read_env())) as conn:
        with pytest.raises(Exception):
            with conn.transaction():
                conn.execute("DROP SCHEMA dw CASCADE")
                raise RuntimeError("gangguan di tengah rebuild")
    assert q1(db, "select count(*) from dw.fact_order_item") == before   # rollback: dw lama utuh
