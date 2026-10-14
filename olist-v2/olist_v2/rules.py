"""Logika gate yang MURNI (tanpa Spark/S3): header, urutan proses, ambang, verdict, pemetaan tipe.
Diuji tanpa cluster; `silver.py` hanya menerapkannya ke DataFrame Spark."""
from __future__ import annotations

import re
from dataclasses import dataclass, field

from .contract import Model


class SourceError(Exception):
    """Sumber rusak di level struktur (kolom hilang/berubah, objek bronze tidak cocok) → berhenti sebelum menulis apa pun."""


def clean_header(cols: list[str]) -> list[str]:
    """Buang BOM UTF-8 (U+FEFF) dan spasi di nama kolom: BOM di file terjemahan membuat join kategori diam-diam gagal."""
    return [c.replace("﻿", "").strip() for c in cols]


def check_header(m: Model, header: list[str]) -> None:
    got = clean_header(header)
    missing = [c for c in m.col_names if c not in got]
    extra = [c for c in got if c not in m.col_names]
    if missing or extra:
        parts = []
        if missing:
            parts.append(f"kolom hilang: {', '.join(missing)}")
        if extra:
            parts.append(f"kolom tak dikenal: {', '.join(extra)}")
        raise SourceError(f"{m.file} ({m.name}): " + "; ".join(parts))


def process_order(models: list[Model]) -> list[str]:
    """Urutan topologis: induk sebelum anak (FK dicek terhadap baris induk yang lolos). Siklus = kontrak salah."""
    by = {m.name: m for m in models}
    done: list[str] = []
    pending = [m.name for m in models]
    while pending:
        ready = [n for n in pending if all(f.parent in done or f.parent == n for f in by[n].fks)]
        if not ready:
            raise ValueError(f"siklus FK di kontrak: {pending}")
        done.extend(ready)
        pending = [n for n in pending if n not in ready]
    return done


def cast_kind(pg_type: str) -> str:
    t = pg_type.strip().lower()
    if t == "text" or t == "char(2)":
        return "text"
    if t == "char(5)":
        return "zip5"
    if t in ("integer", "smallint"):
        return "int"
    if t.startswith("numeric"):
        return "decimal"
    if t == "double precision":
        return "double"
    if t == "timestamp":
        return "timestamp"
    raise ValueError(f"tipe belum didukung: {pg_type}")


def decimal_spec(pg_type: str) -> str:
    m = re.search(r"\((\d+)\s*,\s*(\d+)\)", pg_type)
    return f"decimal({m.group(1)},{m.group(2)})" if m else "decimal(18,2)"


def flag_column(m: Model) -> str | None:
    return m.soft_flag if m.soft else None


@dataclass
class RuleResult:
    table: str
    name: str
    kind: str          # hard | soft
    action: str
    violations: int
    pct: float

    def as_dict(self) -> dict:
        return self.__dict__.copy()


@dataclass
class Verdict:
    verdict: str                                  # PASS | PASS+KARANTINA | FAIL
    fail_reasons: list[str] = field(default_factory=list)


def decide(m: Model, source_rows: int, quarantined: int, duplicate_key_rows: int) -> Verdict:
    """FAIL: jumlah baris menyimpang, kunci tidak unik, atau karantina > ambang. Karantina ≤ ambang = PASS+KARANTINA."""
    fails: list[str] = []
    if source_rows != m.rows:
        fails.append(f"row_count: {source_rows:,} baris, kontrak {m.rows:,}")
    if m.key and duplicate_key_rows:
        fails.append(f"kunci {', '.join(m.key)} tidak unik: {duplicate_key_rows:,} baris terlibat")
    pct = 100.0 * quarantined / source_rows if source_rows else 0.0
    if pct > m.threshold_pct:
        fails.append(f"karantina {pct:.3f}% > ambang {m.threshold_pct}%")
    if fails:
        return Verdict("FAIL", fails)
    return Verdict("PASS+KARANTINA" if quarantined else "PASS")
