"""Test kontrak data tanpa Docker: skema terpusat, kontrak ODCS turunan, ambang, FK, urutan proses."""
import shutil
from pathlib import Path

import yaml

from olist.contract import FKS, PROCESS_ORDER, RULES, THRESHOLDS, write
from olist.schema import TABLES

ROOT = Path(__file__).resolve().parent.parent
BY = {t.name: t for t in TABLES}
PG_TYPES = {"text", "char(2)", "char(5)", "integer", "smallint", "double precision", "timestamp"}


def test_nine_tables_with_explicit_target_types():
    assert len(TABLES) == 9
    for t in TABLES:
        for c in t.cols:
            assert c.type in PG_TYPES or c.type.startswith("numeric("), f"{t.name}.{c.src}: {c.type}"


def test_timestamps_are_not_text_money_is_numeric_and_zip_is_text5():
    types = {(t.name, c.src): c.type for t in TABLES for c in t.cols}
    assert types[("orders", "order_purchase_timestamp")] == "timestamp"
    assert types[("order_items", "price")].startswith("numeric")
    zips = [k for k, v in types.items() if k[1].endswith("zip_code_prefix")]
    assert len(zips) == 3 and all(types[k] == "char(5)" for k in zips)   # kode pos: teks 5 digit di tiga tabel


def test_expected_row_counts_match_the_assignment():
    assert {t.name: t.rows for t in TABLES} == {
        "customers": 99_441, "geolocation": 1_000_163, "order_items": 112_650, "order_payments": 103_886,
        "order_reviews": 99_224, "orders": 99_441, "products": 32_951, "sellers": 3_095, "product_category_translation": 71}


def test_natural_keys_are_declared_correctly():
    assert BY["order_items"].key == ("order_id", "order_item_id")
    assert BY["order_payments"].key == ("order_id", "payment_sequential")
    assert BY["order_reviews"].key == () and BY["geolocation"].key == ()      # bukan kunci: harus dinyatakan tidak unik
    assert BY["product_category_translation"].encoding == "utf-8-sig"          # BOM


def test_source_typo_columns_are_kept_verbatim_in_the_contract():
    names = BY["products"].col_names
    assert "product_name_lenght" in names and "product_description_lenght" in names   # diperbaiki di dw, bukan di kontrak sumber


def test_every_table_has_a_threshold_and_fk_targets_exist():
    assert set(THRESHOLDS) == set(BY)
    for child, col, parent, pcol in FKS:
        assert col in BY[child].col_names and pcol in BY[parent].col_names


def test_process_order_puts_parents_before_children():
    pos = {n: i for i, n in enumerate(PROCESS_ORDER)}
    assert set(pos) == set(BY)
    for child, _c, parent, _p in FKS:
        assert pos[parent] < pos[child]


def test_odcs_file_is_generated_from_the_schema(tmp_path):
    root = tmp_path
    (root / "contracts").mkdir()
    doc = yaml.safe_load(write(root).read_text(encoding="utf-8"))
    models = {s["name"]: s for s in doc["schema"]}
    assert set(models) == set(BY)
    for name, t in BY.items():
        assert [p["name"] for p in models[name]["properties"]] == t.col_names
        rc = next(q for q in models[name]["quality"] if q.get("name") == "row_count")
        assert rc["mustBe"] == t.rows
    sev = {q["severity"] for s in doc["schema"] for q in s["quality"]}
    assert sev == {"error", "warning"}                                          # hard (error) dan soft (warning)


def test_committed_contract_equals_a_fresh_generation(tmp_path):
    (tmp_path / "contracts").mkdir()
    fresh = write(tmp_path).read_text(encoding="utf-8")
    assert fresh == (ROOT / "contracts" / "olist_raw.odcs.yaml").read_text(encoding="utf-8"), "jalankan `python pipeline.py contract`"


def test_soft_rules_cover_the_known_dirty_findings():
    soft = {name for rules in RULES.values() for name, _d, _w, sev in rules if sev == "warning"}
    assert {"lat_in_brazil", "lng_in_brazil", "delivered_has_date", "customer_after_carrier", "value_positive"} <= soft
