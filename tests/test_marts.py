"""Test mart dengan Postgres (di-skip bila stack mati atau `make marts` belum dijalankan)."""
from pathlib import Path

import pytest

from olist.marts import MART_TABLES, queries, read_df, run_marts
from tests.conftest import q1

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="module", autouse=True)
def built(db):
    if q1(db, "select to_regclass('mart.reconciliation') is not null") is not True:
        pytest.skip("mart belum dibangun (jalankan `make marts`)")


def test_all_mart_checks_pass_and_every_table_is_filled(db):
    import psycopg

    from olist.load import pg_dsn
    from pipeline import read_env
    with psycopg.connect(pg_dsn(read_env())) as conn:
        run = run_marts(conn, ROOT)                      # rebuild kedua kali: harus tetap lulus (idempoten)
    assert all(c["bad_rows"] == 0 for c in run["checks"]) and len(run["checks"]) >= 15
    assert all(run["rows"][t] > 0 for t in MART_TABLES)


def test_traceability_revenue_equals_fact_sum(db):
    mart = q1(db, "select sum(revenue) from mart.sales_monthly")
    fact = q1(db, "select sum(price) from dw.fact_order_item where order_status not in ('canceled','unavailable')")
    assert mart == fact


def test_late_rate_uses_honest_denominator(db):
    assert q1(db, "select sum(delivered_orders) from mart.delivery_monthly") == q1(db, "select count(*) from dw.fact_order where is_late is not null")
    assert q1(db, "select sum(delivered_orders) from mart.delivery_monthly") < q1(db, "select count(*) from dw.fact_order where order_status = 'delivered'")


def test_review_gap_between_on_time_and_late_is_large(db):
    r = read_df(db, queries()["q2_review"]).iloc[0]
    assert float(r.avg_on_time) - float(r.avg_late) > 1.5   # ≈ 4,3 vs ≈ 2,3 (angka cek tugas)


def test_reconciliation_matches_at_least_98_percent_and_classifies_the_rest(db):
    assert q1(db, "select 100.0 * count(*) filter (where class='cocok') / count(*) from mart.reconciliation") >= 98
    classes = {r[0] for r in db.execute("select distinct class from mart.reconciliation").fetchall()}
    assert classes <= {"cocok", "tanpa_item", "tanpa_pembayaran", "voucher", "cicilan", "tidak_dijelaskan"}
    assert q1(db, "select count(*) from mart.reconciliation where class='tanpa_item'") == 775
    assert q1(db, "select count(*) from mart.reconciliation where class='tanpa_pembayaran'") == 1


def test_rfm_frequency_is_banded_not_raw_quintile(db):
    # ±96 % orang hanya belanja sekali: kuintil mentah akan memecah F = 1 menjadi 5 skor; pita tetap tidak.
    assert q1(db, "select count(distinct f_score) from mart.customer_rfm where frequency = 1") == 1
    assert q1(db, "select max(f_score) from mart.customer_rfm") == 5
    assert q1(db, "select count(*) from mart.customer_rfm where f_score <> least(frequency, 5)") == 0


def test_seller_with_too_few_orders_is_never_flagged(db):
    assert q1(db, "select count(*) from mart.seller_scorecard where orders < 30 and needs_action") == 0
    assert q1(db, "select count(*) from mart.seller_scorecard where is_rated") == 634


def test_incomplete_months_are_flagged_not_dropped(db):
    assert q1(db, "select count(distinct year_month) from mart.sales_monthly where is_complete_month") == 20
    assert q1(db, "select count(distinct year_month) from mart.sales_monthly where not is_complete_month") >= 2


def test_gold_export_is_readable_from_minio(db):
    from olist.bronze import make_s3
    from pipeline import read_env
    s3 = make_s3(read_env())
    try:
        body = s3.get_object(Bucket="gold", Key="olist/mart/reconciliation.csv")["Body"].read()
    except Exception as e:
        pytest.skip(f"gold belum diekspor: {e}")
    assert body.count(b"\n") == 99_441 + 1
