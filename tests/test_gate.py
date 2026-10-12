"""Test gate kualitas T2 tanpa Docker: tabel mini buatan + aturan mini."""
import pandas as pd
import pytest

from olist.gate import REASONS, SourceError, check_header, process_table
from olist.schema import TABLES, Col, Table

PARENT = Table("parent", "parent.csv", 3, ("id",), "induk", (Col("id", "text", False, "id"),))
CHILD = Table(
    "child", "child.csv", 5, ("id",), "anak",
    (
        Col("id", "text", False, "id"),
        Col("parent_id", "text", False, "fk"),
        Col("zip", "char(5)", False, "kode pos"),
        Col("amount", "numeric(10,2)", True, "nilai"),
        Col("ts", "timestamp", True, "waktu"),
    ),
)
RULES = {"child": [
    ("amount_positive", "amount > 0", "amount <= 0", "error"),
    ("amount_small", "amount < 1000", "amount >= 1000", "warning"),
]}
FKS = (("child", "parent_id", "parent", "id"),)


def parents():
    return {"parent": pd.DataFrame({"id": ["p1", "p2", "p3"]})}


def child_df(rows):
    return pd.DataFrame(rows, columns=["id", "parent_id", "zip", "amount", "ts"], dtype=str)


def good(i, **kw):
    r = {"id": f"c{i}", "parent_id": "p1", "zip": "01003", "amount": "10.50", "ts": "2017-01-01 10:00:00"}
    r.update(kw)
    return [r[c] for c in ["id", "parent_id", "zip", "amount", "ts"]]


def run(rows, th=100.0):
    t = Table(**{**CHILD.__dict__, "rows": len(rows)})   # kontrak jumlah baris = jumlah baris uji
    return process_table(t, child_df(rows), parents(), RULES, FKS, {"child": (th, "uji")})


def test_all_clean_is_pass_and_types_are_applied():
    r = run([good(i) for i in range(5)])
    assert r.verdict == "PASS" and r.quarantined == 0 and r.clean == 5
    assert str(r.silver["ts"].dtype).startswith("datetime64")
    assert str(r.silver["amount"].dtype) == "float64"


def test_zip_prefix_keeps_leading_zero_as_text():
    r = run([good(0, zip="1003")] + [good(i) for i in range(1, 5)])
    assert r.silver.loc[0, "zip"] == "01003"


def test_empty_string_becomes_null_not_empty_text():
    r = run([good(0, amount="", ts="")] + [good(i) for i in range(1, 5)])
    assert pd.isna(r.silver.loc[0, "amount"]) and pd.isna(r.silver.loc[0, "ts"])


def test_cast_failure_and_not_null_go_to_quarantine_with_reason():
    rows = [good(0, ts="31/12/2017"), good(1, zip=""), good(2), good(3), good(4)]
    r = run(rows)
    q = r.quarantine.set_index("id")[REASONS]
    assert "cast:ts:timestamp" in q["c0"] and "not_null:zip" in q["c1"]
    assert r.quarantined == 2 and r.clean == 3
    assert r.quarantined + r.clean == r.source_rows      # tidak ada baris yang hilang diam-diam


def test_hard_rule_quarantines_soft_rule_keeps_and_flags():
    rows = [good(0, amount="-5"), good(1, amount="5000")] + [good(i) for i in range(2, 5)]
    r = run(rows)
    assert list(r.quarantine["id"]) == ["c0"]
    assert "c1" in set(r.silver["id"])                    # soft: tetap lewat
    assert bool(r.silver.set_index("id").loc["c1", "has_dq_warning"]) is True
    soft = [x for x in r.rules if x.kind == "soft"][0]
    assert soft.violations == 1


def test_fk_orphan_is_quarantined_not_dropped():
    r = run([good(0, parent_id="ZZZ")] + [good(i) for i in range(1, 5)])
    assert "fk:parent_id→parent.id" in r.quarantine.iloc[0][REASONS]


def test_threshold_pass_and_fail_cases():
    rows = lambda bad: [good(i, amount="-1") if i < bad else good(i) for i in range(100)]
    ok = run(rows(2), th=2.0)
    bad = run(rows(3), th=2.0)
    assert ok.verdict == "PASS+KARANTINA" and ok.quarantine_pct == 2.0      # tepat di ambang: lolos
    assert bad.verdict == "FAIL" and bad.quarantine_pct == 3.0              # di atas ambang: gagal
    assert any("ambang" in f for f in bad.fail_reasons)


def test_duplicate_key_fails_the_table():
    r = run([good(0), good(0)] + [good(i) for i in range(2, 5)])
    assert r.verdict == "FAIL" and any("tidak unik" in f for f in r.fail_reasons)


def test_row_count_deviation_fails():
    rows = [good(i) for i in range(4)]
    r = process_table(CHILD, child_df(rows), parents(), RULES, FKS, {"child": (100.0, "uji")})  # kontrak 5 baris
    assert r.verdict == "FAIL" and any("row_count" in f for f in r.fail_reasons)


def test_missing_column_names_the_column_and_stops():
    t = next(x for x in TABLES if x.name == "customers")
    header = [c for c in t.col_names if c != "customer_unique_id"]
    with pytest.raises(SourceError, match="customer_unique_id"):
        check_header(t, header)


def test_renamed_column_reports_both_missing_and_unknown():
    t = next(x for x in TABLES if x.name == "sellers")
    header = ["seller_identifier" if c == "seller_id" else c for c in t.col_names]
    with pytest.raises(SourceError) as e:
        check_header(t, header)
    assert "seller_id" in str(e.value) and "seller_identifier" in str(e.value)


def test_real_contract_rules_have_enough_hard_and_soft():
    from olist.contract import FKS as REAL_FKS, RULES as REAL
    hard = sum(1 for v in REAL.values() for *_x, s in v if s == "error") + len(REAL_FKS) + 2 * len(TABLES)
    soft = sum(1 for v in REAL.values() for *_x, s in v if s != "error")
    assert hard >= 10 and soft >= 8
