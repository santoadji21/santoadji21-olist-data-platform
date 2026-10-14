"""Gate kontrak tanpa Docker: ringkasan hasil `datacontract test` dan perilaku lulus/gagal."""
import json
import subprocess

import pytest

from olist_v2 import gate

SAMPLE = {"result": "warning", "checks": [
    {"result": "passed", "model": "orders", "name": "row_count"},
    {"result": "passed", "model": "orders", "name": "status_known"},
    {"result": "warning", "model": "orders", "field": "order_id", "name": "has type string", "reason": "Checking types in parquet files is not supported yet."},
    {"result": "warning", "model": "orders", "name": "delivered wajib punya tanggal", "reason": "Actual custom_sql(orders) was 8, expected = 0"},
]}


def test_summarize_separates_soft_violations_from_unsupported_type_checks():
    s = gate.summarize(SAMPLE)
    assert (s["checks"], s["passed"], s["unsupported_type_checks"]) == (4, 2, 1)
    assert [x["name"] for x in s["soft_violations"]] == ["delivered wajib punya tanggal"] and s["failed"] == []


def test_failed_check_is_listed_as_failed():
    s = gate.summarize({"result": "failed", "checks": [{"result": "failed", "model": "orders", "name": "row_count", "reason": "was 5"}]})
    assert s["failed"][0]["name"] == "row_count"


def fake_runner(payload, code=0):
    def run(cmd, **kw):
        out = cmd[cmd.index("--output") + 1]
        if payload is not None:
            open(out, "w").write(json.dumps(payload))
        return subprocess.CompletedProcess(cmd, code, "", "")
    return run


ENV = {"MINIO_ROOT_USER": "u", "MINIO_ROOT_PASSWORD": "p", "DATACONTRACT_BIN": "dc"}


def test_run_gate_passes_s3_credentials_and_uses_the_silver_server():
    seen = {}

    def runner(cmd, **kw):
        seen.update(cmd=cmd, env=kw["env"])
        return fake_runner(SAMPLE)(cmd, **kw)

    s = gate.run_gate(ENV, runner=runner)
    assert "silver-minio" in seen["cmd"] and seen["env"]["DATACONTRACT_S3_ACCESS_KEY_ID"] == "u" and s["result"] == "warning"


def test_missing_result_file_is_an_error_not_a_silent_pass():
    with pytest.raises(RuntimeError, match="tidak menghasilkan hasil"):
        gate.run_gate(ENV, runner=fake_runner(None, code=1))


def test_report_states_the_parquet_type_limitation_instead_of_hiding_it():
    md = gate.report_md({**gate.summarize(SAMPLE), "ran_at": "x", "server": "silver-minio"})
    assert "tidak didukung" in md and "delivered wajib punya tanggal" in md and "Keterbatasan" in md
