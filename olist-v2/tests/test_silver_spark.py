"""Transformasi + gate silver dengan Spark LOKAL pada data kecil buatan (tanpa MinIO/Docker).
Dilewati otomatis bila pyspark tidak terpasang (mis. di venv dev yang hanya berisi dbt)."""
import pytest

pyspark = pytest.importorskip("pyspark")

from olist_v2.contract import FK, Column, Model, Rule  # noqa: E402
from olist_v2.silver import process_table  # noqa: E402

PARENT = Model("parent", "parent.csv", 3, "utf-8", ("id",), (Column("id", "text", True),), (), (), 0.0, None)
CHILD = Model(
    "child", "child.csv", 5, "utf-8", ("id",),
    (Column("id", "text", True), Column("parent_id", "text", True), Column("zip", "char(5)", True),
     Column("amount", "numeric(10,2)", False), Column("ts", "timestamp", False), Column("n", "integer", False)),
    (Rule("amount_positive", "error", "amount <= 0"), Rule("amount_big", "warning", "amount >= 1000")),
    (FK("parent_id", "parent", "id"),), 100.0, "has_dq_warning")
COLS = ["id", "parent_id", "zip", "amount", "ts", "n"]


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[1]").appName("t").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2").config("spark.sql.session.timeZone", "UTC").getOrCreate())
    yield s
    s.stop()


def good(i, **kw):
    r = {"id": f"c{i}", "parent_id": "p1", "zip": "01003", "amount": "10.50", "ts": "2017-01-01 10:00:00", "n": "3"}
    r.update(kw)
    return tuple(r[c] for c in COLS)


def run(spark, rows, model=CHILD):
    parents = {"parent": spark.createDataFrame([("p1",), ("p2",), ("p3",)], ["id"])}
    return process_table(spark.createDataFrame(rows, COLS), model, parents)


def reasons(res):
    return {r["id"]: r["_reasons"] for r in res.quarantine.collect()}


def test_all_clean_is_pass_and_types_are_applied(spark):
    r = run(spark, [good(i) for i in range(5)])
    assert r.verdict == "PASS" and r.quarantined == 0 and r.clean == 5
    dt = dict(r.silver.dtypes)
    assert dt["ts"] == "timestamp" and dt["amount"].startswith("decimal(10,2)") and dt["n"] == "int"


def test_zip_prefix_keeps_leading_zero_as_text(spark):
    r = run(spark, [good(0, zip="1003")] + [good(i) for i in range(1, 5)])
    assert r.silver.where("id = 'c0'").collect()[0]["zip"] == "01003"


def test_cast_failure_and_not_null_are_quarantined_with_reasons(spark):
    rows = [good(0, ts="31/12/2017"), good(1, zip=None), good(2, n="1.5"), good(3), good(4)]
    r = run(spark, rows)
    q = reasons(r)
    assert "cast:ts:timestamp" in q["c0"] and "not_null:zip" in q["c1"] and "cast:n:integer" in q["c2"]
    assert r.quarantined + r.clean == r.source_rows == 5          # tidak ada baris yang hilang diam-diam


def test_hard_rule_quarantines_and_soft_rule_keeps_and_flags(spark):
    r = run(spark, [good(0, amount="-5"), good(1, amount="5000")] + [good(i) for i in range(2, 5)])
    assert list(reasons(r)) == ["c0"] and "rule:amount_positive" in reasons(r)["c0"]
    kept = {x["id"]: x["has_dq_warning"] for x in r.silver.collect()}
    assert kept["c1"] is True and kept["c2"] is False
    assert [x.violations for x in r.rules if x.kind == "soft"] == [1]


def test_fk_orphan_is_quarantined_not_dropped(spark):
    r = run(spark, [good(0, parent_id="ZZZ")] + [good(i) for i in range(1, 5)])
    assert "fk:parent_id→parent.id" in reasons(r)["c0"]


def test_threshold_pass_and_fail_at_the_boundary(spark):
    m = Model(**{**CHILD.__dict__, "rows": 100, "threshold_pct": 2.0})
    rows = lambda bad: [good(i, amount="-1") if i < bad else good(i) for i in range(100)]
    assert run(spark, rows(2), m).verdict == "PASS+KARANTINA"     # tepat 2,0 % lolos
    assert run(spark, rows(3), m).verdict == "FAIL"               # 3,0 % gagal


def test_duplicate_key_and_row_count_deviation_fail(spark):
    assert run(spark, [good(0), good(0)] + [good(i) for i in range(2, 5)]).verdict == "FAIL"
    assert "row_count" in run(spark, [good(i) for i in range(4)]).fail_reasons[0]


def test_empty_string_is_null_not_text(spark):
    from olist_v2.silver import read_raw  # noqa: F401  (jalur baca CSV diuji di test CSV di bawah)
    r = run(spark, [good(0, amount=None, ts=None)] + [good(i) for i in range(1, 5)])
    row = r.silver.where("id = 'c0'").collect()[0]
    assert row["amount"] is None and row["ts"] is None


def test_csv_with_bom_quoted_newlines_and_empty_values(spark, tmp_path):
    from olist_v2.silver import read_raw
    p = tmp_path / "x.csv"
    p.write_bytes('﻿id,note\n1,"baris\nbaru"\n2,\n'.encode("utf-8"))
    df = read_raw(spark, str(p))
    assert df.columns == ["id", "note"]                           # BOM dibuang dari nama kolom
    rows = {r["id"]: r["note"] for r in df.collect()}
    assert rows["1"] == "baris\nbaru" and rows["2"] is None        # newline dalam kutip utuh; kosong → NULL
