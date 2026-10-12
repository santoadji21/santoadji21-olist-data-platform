"""Pelapor failure test dan pembangkit EVALUATION_EVIDENCE tanpa Docker: angka harus datang dari file hasil."""
import json

from olist.evidence import build
from olist.failures import report_md, result


def case(cid, caught=True):
    return result(cid, f"kasus {cid}", "dirusak", "symptom spesifik 123", caught, "mekanisme", "pulih", caught, {"angka": 4242}, 0)


def test_failure_report_has_verdict_mechanism_recovery_and_evidence_per_case():
    md = report_md([case("a"), case("b", caught=False)], None, "2026-10-12T00:00:00+00:00")
    assert "1/2 kasus TERTANGKAP" in md
    assert "**TERTANGKAP**" in md and "**LOLOS**" in md
    for needle in ("Yang dirusak", "Symptom", "Mekanisme yang menangkap", "Cara pulih", "angka = 4242"):
        assert needle in md


def test_evidence_reads_numbers_from_json_files_not_from_code(tmp_path):
    out = tmp_path / "outputs"
    out.mkdir()
    (out / "run_all.json").write_text(json.dumps({"started_at": "2026-10-12", "total_seconds": 321.5, "stages": [
        {"stage": "bronze", "command": "python pipeline.py bronze", "seconds": 7.7, "exit": 0}]}))
    (out / "dw.json").write_text(json.dumps({"seconds": 9.9, "rows": {"fact_order": 987654}, "checks": [{"check": "x", "bad_rows": 0}],
                                              "key_numbers": {"unique_people": 1, "orders_without_items": 2, "orders_without_payment": 3,
                                                              "delivered": 4, "late_rate_pct": 5.55, "delivered_with_date": 6, "complete_months": 20}}))
    (out / "failures.json").write_text(json.dumps({"cases": [{"id": "a", "name": "n", "verdict": "TERTANGKAP", "recovered": True,
                                                              "symptom": "s", "evidence": {}}], "restore": None}))
    md = build(tmp_path)
    assert "321.5" in md and "7.7" in md          # durasi dari run_all.json
    assert "987,654" in md and "9.9" in md        # baris dw dari dw.json
    assert "1/1 kasus TERTANGKAP" in md
    for section in ("## 6. Keputusan desain", "## 7. Limitation", "## 8. Next experiment", "## 9. Reproduksi"):
        assert section in md


def test_evidence_has_at_least_four_limitations_and_three_experiments(tmp_path):
    (tmp_path / "outputs").mkdir()
    md = build(tmp_path)
    lim = md.split("## 7. Limitation")[1].split("## 8.")[0]
    exp = md.split("## 8. Next experiment")[1].split("## 9.")[0]
    assert sum(1 for ln in lim.splitlines() if ln[:2].strip(". ").isdigit()) >= 4
    assert sum(1 for ln in exp.splitlines() if ln[:2].strip(". ").isdigit()) >= 3
    assert "belum ada" in md.lower()              # bagian tanpa file hasil menyatakannya, bukan mengarang angka
