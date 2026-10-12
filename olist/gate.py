"""T2 — Gate kualitas: tipe, aturan hard (karantina), aturan soft (hitung + flag), ambang, verdict.

Murni (tanpa S3/Docker) supaya bisa diuji dengan data kecil buatan.
Aturan dibaca dari `olist.contract.RULES` — klausa WHERE yang SAMA dipakai kontrak ODCS (datacontract test)
dan gate ini (dijalankan DuckDB di atas DataFrame), jadi tidak ada dua versi aturan.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

import duckdb
import pandas as pd

from .contract import DEFAULT_FLAG, FKS, FLAG_NAMES, RULES, THRESHOLDS
from .schema import Col, Table

ROW = "_row"          # nomor baris data sumber (1-based)
REASONS = "_reasons"  # alasan karantina, dipisah ';'


class SourceError(Exception):
    """Sumber rusak di level struktur (kolom hilang/berubah) → berhenti sebelum menyentuh Postgres."""


@dataclass
class RuleResult:
    table: str
    name: str
    kind: str        # hard | soft
    action: str
    violations: int
    pct: float

    def as_dict(self) -> dict:
        return self.__dict__.copy()


@dataclass
class TableResult:
    table: str
    source_rows: int
    expected_rows: int
    quarantined: int
    clean: int
    threshold_pct: float
    verdict: str                                  # PASS | PASS+KARANTINA | FAIL
    fail_reasons: list[str] = field(default_factory=list)
    rules: list[RuleResult] = field(default_factory=list)
    silver: pd.DataFrame | None = None            # baris lolos (+ kolom flag)
    quarantine: pd.DataFrame | None = None        # baris sumber (teks) + _row + _reasons

    @property
    def quarantine_pct(self) -> float:
        return 100.0 * self.quarantined / self.source_rows if self.source_rows else 0.0

    def as_dict(self) -> dict:
        return {
            "table": self.table, "source_rows": self.source_rows, "expected_rows": self.expected_rows,
            "quarantined": self.quarantined, "clean": self.clean,
            "quarantine_pct": round(self.quarantine_pct, 4), "threshold_pct": self.threshold_pct,
            "verdict": self.verdict, "fail_reasons": self.fail_reasons,
            "rules": [r.as_dict() for r in self.rules],
        }


# ---- struktur: kolom wajib ------------------------------------------------------------------------
def check_header(t: Table, header: list[str]) -> None:
    """Kolom hilang/berubah nama → SourceError yang menyebut kolom mana (dipanggil sebelum apa pun dimuat)."""
    want, got = t.col_names, [h.strip() for h in header]
    missing = [c for c in want if c not in got]
    extra = [c for c in got if c not in want]
    if missing or extra:
        parts = []
        if missing:
            parts.append(f"kolom hilang: {', '.join(missing)}")
        if extra:
            parts.append(f"kolom tak dikenal: {', '.join(extra)}")
        raise SourceError(f"{t.file} ({t.name}): " + "; ".join(parts))


# ---- konversi tipe ---------------------------------------------------------------------------------
def _cast(col: Col, s: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Kembalikan (nilai bertipe, mask gagal-cast). Nilai kosong/null bukan kegagalan cast."""
    present = s.notna()
    typ = col.type
    if typ == "text" or typ == "char(2)":
        return s, pd.Series(False, index=s.index)
    if typ == "char(5)":
        ok = s.str.fullmatch(r"\d{1,5}").fillna(False).astype(bool)
        out = s.where(~ok, s.str.zfill(5))
        return out, present & ~ok
    if typ in ("integer", "smallint"):
        num = pd.to_numeric(s, errors="coerce")
        bad = present & (num.isna() | (num % 1 != 0))
        return num.where(~bad).astype("Int64"), bad
    if typ.startswith("numeric") or typ == "double precision":
        num = pd.to_numeric(s, errors="coerce").astype("float64")
        return num, present & num.isna()
    if typ == "timestamp":
        ts = pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
        return ts, present & ts.isna()
    raise ValueError(f"tipe belum didukung: {typ}")


def parse_source(t: Table, raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Teks mentah → DataFrame bertipe + Series alasan karantina (kosong = lolos tipe/null)."""
    raw = raw.replace({"": None})
    typed = pd.DataFrame(index=raw.index)
    reasons = pd.Series("", index=raw.index, dtype="object")
    for c in t.cols:
        val, bad = _cast(c, raw[c.src])
        typed[c.src] = val
        reasons = _add(reasons, bad, f"cast:{c.src}:{c.type}")
        if not c.nullable:
            reasons = _add(reasons, raw[c.src].isna(), f"not_null:{c.src}")
    return typed, reasons


def _add(reasons: pd.Series, mask: pd.Series, why: str) -> pd.Series:
    mask = mask.fillna(False).astype(bool)
    return reasons.where(~mask, reasons.where(reasons == "", reasons + ";") + why)


# ---- aturan SQL (DuckDB) ---------------------------------------------------------------------------
def _violating_rows(typed: pd.DataFrame, where: str) -> set[int]:
    con = duckdb.connect()
    try:
        con.register("t", typed)
        return {r[0] for r in con.execute(f"SELECT {ROW} FROM t WHERE {where}").fetchall()}
    finally:
        con.close()


def flag_name(table: str) -> str:
    return FLAG_NAMES.get(table, DEFAULT_FLAG)


def process_table(
    t: Table, raw: pd.DataFrame, parents: dict[str, pd.DataFrame] | None = None,
    rules: dict | None = None, fks: tuple = FKS, thresholds: dict | None = None,
) -> TableResult:
    """Jalankan seluruh gate untuk satu tabel. `raw` = semua kolom bertipe teks (dtype=str)."""
    rules = RULES if rules is None else rules
    thresholds = THRESHOLDS if thresholds is None else thresholds
    parents = parents or {}
    n = len(raw)
    th = thresholds[t.name][0]
    fails: list[str] = []
    results: list[RuleResult] = []

    def rec(name, kind, action, count):
        results.append(RuleResult(t.name, name, kind, action, int(count), round(100.0 * count / n, 4) if n else 0.0))

    raw = raw.reset_index(drop=True)
    typed, reasons = parse_source(t, raw)
    typed.insert(0, ROW, range(1, n + 1))

    # hard, tingkat batch: jumlah baris & kunci unik → FAIL (bukan karantina)
    rec("row_count", "hard", "batch berhenti", 0 if n == t.rows else abs(n - t.rows))
    if n != t.rows:
        fails.append(f"row_count: {n:,} baris, kontrak {t.rows:,}")
    if t.key:
        dup = int(typed.duplicated(list(t.key), keep=False).sum())
        rec(f"key_unique({','.join(t.key)})", "hard", "batch berhenti", dup)
        if dup:
            fails.append(f"kunci {', '.join(t.key)} tidak unik: {dup:,} baris terlibat")

    # hard, tingkat baris → karantina
    rec("type_and_not_null", "hard", "baris → karantina", int((reasons != "").sum()))
    for name, _desc, where, sev in rules.get(t.name, []):
        if sev != "error":
            continue
        hit = _violating_rows(typed, where)
        rec(name, "hard", "baris → karantina", len(hit))
        reasons = _add(reasons, typed[ROW].isin(hit), f"rule:{name}")
    for child, col, parent, pcol in fks:
        if child != t.name:
            continue
        ok_vals = parents.get(parent)
        if ok_vals is None:
            raise SourceError(f"{t.name}: tabel induk {parent} belum diproses (urutan FK salah)")
        orphan = typed[col].notna() & ~typed[col].isin(ok_vals[pcol])
        rec(f"fk({col}→{parent}.{pcol})", "hard", "baris → karantina", int(orphan.sum()))
        reasons = _add(reasons, orphan, f"fk:{col}→{parent}.{pcol}")

    # soft → hitung + flag, baris tetap lewat
    soft = [(nm, w) for nm, _d, w, sev in rules.get(t.name, []) if sev != "error"]
    flag = pd.Series(False, index=typed.index)
    for name, where in soft:
        hit = _violating_rows(typed, where)
        rec(name, "soft", "simpan + flag", len(hit))
        flag |= typed[ROW].isin(hit)

    bad = reasons != ""
    quarantined = int(bad.sum())
    qpct = 100.0 * quarantined / n if n else 0.0
    if qpct > th:
        fails.append(f"karantina {qpct:.3f}% > ambang {th}%")

    silver = typed.loc[~bad].drop(columns=[ROW]).reset_index(drop=True)
    if soft:
        silver[flag_name(t.name)] = flag.loc[~bad].to_numpy()
    q = raw.loc[bad].copy()
    q.insert(0, ROW, typed.loc[bad, ROW].to_numpy())
    q[REASONS] = reasons.loc[bad].to_numpy()

    verdict = "FAIL" if fails else ("PASS+KARANTINA" if quarantined else "PASS")
    return TableResult(t.name, n, t.rows, quarantined, len(silver), th, verdict, fails, results, silver, q)


def flag_columns(t: Table, rules: dict | None = None) -> list[str]:
    """Nama kolom flag tambahan di silver/raw untuk tabel ini (kosong bila tak ada aturan soft)."""
    rules = RULES if rules is None else rules
    return [flag_name(t.name)] if any(sev != "error" for *_x, sev in rules.get(t.name, [])) else []
