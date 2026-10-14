"""Logika gate murni + kontrak: tanpa Spark, tanpa Docker."""
import duckdb
import pytest

from olist_v2 import rules
from olist_v2.contract import FK, Column, Model, load_models

MODELS = {m.name: m for m in load_models()}


def mini(rows=100, key=("id",), threshold=2.0):
    return Model("t", "t.csv", rows, "utf-8", key, (Column("id", "text", True),), (), (), threshold, None)


def test_loader_reads_all_nine_models_with_rules_fks_thresholds_and_flags():
    assert len(MODELS) == 9
    assert sum(len(m.hard) for m in MODELS.values()) >= 7 and sum(len(m.soft) for m in MODELS.values()) >= 8
    assert MODELS["orders"].threshold_pct == 0.1 and MODELS["geolocation"].threshold_pct == 2.0 and MODELS["customers"].threshold_pct == 0.0
    assert MODELS["geolocation"].soft_flag == "is_outside_brazil" and MODELS["orders"].soft_flag == "has_time_anomaly"
    assert MODELS["customers"].soft_flag is None
    assert {(f.column, f.parent) for f in MODELS["order_items"].fks} == {("order_id", "orders"), ("product_id", "products"), ("seller_id", "sellers")}
    assert MODELS["order_items"].key == ("order_id", "order_item_id") and MODELS["geolocation"].key == ()


def test_rules_are_row_level_where_clauses_not_key_checks():
    names = {r.name for m in MODELS.values() for r in m.rules}
    assert "composite_key_unique" not in names and "state_len2" in names and "lat_in_brazil" in names
    assert all("HAVING" not in r.where.upper() for m in MODELS.values() for r in m.rules)


def test_every_contract_rule_is_valid_sql_against_typed_columns():
    """Jalankan SETIAP aturan kontrak pada tabel kosong bertipe (DuckDB): membuktikan sintaks valid dan semua kolom ada."""
    ddl = {"text": "VARCHAR", "varchar(2)": "VARCHAR", "varchar(5)": "VARCHAR", "integer": "INTEGER", "smallint": "SMALLINT",
           "double": "DOUBLE", "timestamp": "TIMESTAMP"}
    con = duckdb.connect()
    for m in MODELS.values():
        cols = ", ".join(f'"{c.name}" ' + (ddl.get(c.pg_type) or "DECIMAL(10,2)") for c in m.columns)
        con.execute(f"CREATE TABLE {m.name} ({cols})")
        for r in m.rules:
            assert con.execute(f"SELECT COUNT(*) FROM {m.name} WHERE {r.where}").fetchone()[0] == 0, (m.name, r.name)


def test_process_order_puts_every_parent_before_its_children():
    order = rules.process_order(list(MODELS.values()))
    pos = {n: i for i, n in enumerate(order)}
    assert set(order) == set(MODELS)
    for m in MODELS.values():
        for f in m.fks:
            assert pos[f.parent] < pos[m.name]


def test_fk_cycle_is_rejected():
    a = Model("a", "a", 1, "utf-8", (), (), (), (FK("x", "b", "y"),), 0, None)
    b = Model("b", "b", 1, "utf-8", (), (), (), (FK("y", "a", "x"),), 0, None)
    with pytest.raises(ValueError, match="siklus"):
        rules.process_order([a, b])


def test_missing_column_names_the_column_and_stops():
    m = MODELS["customers"]
    with pytest.raises(rules.SourceError, match="customer_unique_id"):
        rules.check_header(m, [c for c in m.col_names if c != "customer_unique_id"])


def test_renamed_column_reports_missing_and_unknown():
    m = MODELS["sellers"]
    with pytest.raises(rules.SourceError) as e:
        rules.check_header(m, ["seller_identifier" if c == "seller_id" else c for c in m.col_names])
    assert "seller_id" in str(e.value) and "seller_identifier" in str(e.value)


def test_bom_in_first_column_is_stripped_so_the_header_matches():
    m = MODELS["product_category_translation"]
    rules.check_header(m, ["﻿" + m.col_names[0]] + m.col_names[1:])     # tidak boleh melempar


def test_threshold_boundary_pass_and_fail():
    m = mini(rows=100, threshold=2.0)
    ok, bad = rules.decide(m, 100, 2, 0), rules.decide(m, 100, 3, 0)
    assert ok.verdict == "PASS+KARANTINA" and bad.verdict == "FAIL" and "ambang" in bad.fail_reasons[0]
    assert rules.decide(m, 100, 0, 0).verdict == "PASS"


def test_zero_threshold_fails_on_a_single_quarantined_row():
    assert rules.decide(mini(threshold=0.0), 100, 1, 0).verdict == "FAIL"


def test_duplicate_key_and_row_count_deviation_fail_the_table():
    m = mini(rows=100)
    assert "tidak unik" in rules.decide(m, 100, 0, 5).fail_reasons[0]
    assert "row_count" in rules.decide(m, 99, 0, 0).fail_reasons[0]


def test_cast_kinds_cover_every_contract_type():
    kinds = {rules.cast_kind(c.pg_type) for m in MODELS.values() for c in m.columns}
    assert kinds == {"text", "zip5", "int", "decimal", "double", "timestamp"}
    assert rules.decimal_spec("numeric(10,2)") == "decimal(10,2)"
