"""Daftar tabel dibaca dari kontrak ODCS (contracts/olist_raw.odcs.yaml): SATU sumber untuk nama tabel, file sumber,
jumlah baris yang diharapkan, dan encoding. Tidak ada daftar kedua yang diketik ulang."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Table:
    name: str
    file: str
    rows: int
    encoding: str = "utf-8"


def contract_path() -> Path:
    return Path(os.environ.get("OLIST_CONTRACT", REPO / "contracts" / "olist_raw.odcs.yaml"))


def _prop(model: dict, key: str, default=None):
    for p in model.get("customProperties", []):
        if p.get("property") == key:
            return p.get("value")
    return default


def load_tables(path: Path | None = None) -> list[Table]:
    doc = yaml.safe_load((path or contract_path()).read_text(encoding="utf-8"))
    tables = []
    for m in doc["schema"]:
        rows = next((q["mustBe"] for q in m.get("quality", []) if q.get("name") == "row_count"), 0)
        tables.append(Table(m["name"], _prop(m, "sourceFile"), int(rows), _prop(m, "encoding", "utf-8")))
    return tables
