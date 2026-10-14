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


# ---------------------------------------------------------------- model lengkap (silver / gate)
import re  # noqa: E402


@dataclass(frozen=True)
class Column:
    name: str
    pg_type: str          # tipe target: text, char(2), char(5), integer, smallint, numeric(10,2), double precision, timestamp
    required: bool


@dataclass(frozen=True)
class Rule:
    name: str
    severity: str         # error = hard (karantina) | warning = soft (hitung + flag)
    where: str            # klausa WHERE pelanggar (dipakai Spark SQL, DuckDB, dan datacontract test: satu versi)


@dataclass(frozen=True)
class FK:
    column: str
    parent: str
    parent_column: str


@dataclass(frozen=True)
class Model:
    name: str
    file: str
    rows: int
    encoding: str
    key: tuple[str, ...]
    columns: tuple[Column, ...]
    rules: tuple[Rule, ...]
    fks: tuple[FK, ...]
    threshold_pct: float
    soft_flag: str | None

    @property
    def col_names(self) -> list[str]:
        return [c.name for c in self.columns]

    @property
    def hard(self) -> list[Rule]:
        return [r for r in self.rules if r.severity == "error"]

    @property
    def soft(self) -> list[Rule]:
        return [r for r in self.rules if r.severity != "error"]


_WHERE = re.compile(r"\bWHERE\s+(.*)$", re.IGNORECASE | re.DOTALL)


def _where(query: str) -> str:
    m = _WHERE.search(query)
    if not m:
        raise ValueError(f"aturan SQL tanpa WHERE: {query}")
    return m.group(1).strip()


def _fks(raw: str | None) -> tuple[FK, ...]:
    out = []
    for part in (raw or "").split(","):
        part = part.strip()
        if part:
            col, ref = part.split("->")
            parent, pcol = ref.strip().split(".")
            out.append(FK(col.strip(), parent, pcol))
    return tuple(out)


def load_models(path: Path | None = None) -> list[Model]:
    doc = yaml.safe_load((path or contract_path()).read_text(encoding="utf-8"))
    models = []
    for m in doc["schema"]:
        quality = m.get("quality", [])
        rows = next((q["mustBe"] for q in quality if q.get("name") == "row_count"), 0)
        # Aturan per baris = klausa WHERE. Aturan keunikan kunci (GROUP BY ... HAVING) bersifat tingkat-batch dan
        # ditangani cek kunci di gate (rules.decide), jadi tidak dimuat sebagai aturan baris.
        rules = tuple(Rule(q["name"], q.get("severity", "error"), _where(q["query"]))
                      for q in quality if q.get("type") == "sql" and not re.search(r"\bHAVING\b", q["query"], re.IGNORECASE))
        key = tuple(p["name"] for p in m["properties"] if p.get("primaryKey"))
        cols = tuple(Column(p["name"], p["physicalType"], bool(p.get("required", False))) for p in m["properties"])
        models.append(Model(
            m["name"], _prop(m, "sourceFile"), int(rows), _prop(m, "encoding", "utf-8"), key, cols, rules,
            _fks(_prop(m, "foreignKeys")), float(_prop(m, "quarantineThresholdPct", 0.0)), _prop(m, "softFlagColumn")))
    return models
