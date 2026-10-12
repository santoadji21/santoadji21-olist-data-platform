"""Generator kontrak data ODCS (Open Data Contract Standard 3.x, https://datacontract.com).

`olist/schema.py` adalah sumber kebenaran; file YAML di `contracts/` adalah turunannya
(jangan diedit tangan — jalankan `python pipeline.py contract`).

Aturan kualitas dibagi dua (lihat DATA_CONTRACT.md bagian 3):
  * severity=error   → gate HARD: pelanggaran = pipeline berhenti / baris dikarantina
  * severity=warning → gate SOFT: temuan kotor yang SUDAH diketahui; dicatat, tidak menggagalkan
"""
from __future__ import annotations

import re
import shutil
from pathlib import Path

import yaml

from .schema import BRAZIL_LAT, BRAZIL_LNG, TABLES, Table

CONTRACT_PATH = "contracts/olist_raw.odcs.yaml"
VIEW_DIR = "data/contract"          # symlink {model}.csv → file asli (nama file ≠ nama tabel)

# ---- pemetaan tipe PostgreSQL → ODCS ---------------------------------------------------------
def _types(pg: str) -> tuple[str, dict]:
    if pg.startswith("char("):
        n = int(re.search(r"\((\d+)\)", pg).group(1))
        return "string", {"minLength": n, "maxLength": n}
    if pg in ("text",):
        return "string", {}
    if pg in ("integer", "smallint"):
        return "integer", {}
    if pg.startswith("numeric") or pg == "double precision":
        return "number", {}
    if pg == "timestamp":
        return "timestamp", {}
    raise ValueError(f"tipe belum dipetakan: {pg}")


def _sql(name: str, desc: str, where: str, sev: str, table: str) -> dict:
    return {
        "type": "sql", "name": name, "description": desc, "severity": sev,
        "query": f"SELECT COUNT(*) FROM {table} WHERE {where}", "mustBe": 0,
    }


# Aturan tingkat-tabel di luar tipe/null/kunci. (nama, deskripsi, WHERE pelanggar, severity)
RULES: dict[str, list[tuple[str, str, str, str]]] = {
    "customers": [
        ("state_len2", "customer_state harus 2 huruf (UF)", "length(customer_state) <> 2", "error"),
    ],
    "geolocation": [
        ("lat_in_brazil", f"lat dalam kotak Brasil {BRAZIL_LAT} (temuan #5: titik di luar = soft)",
         f"geolocation_lat NOT BETWEEN {BRAZIL_LAT[0]} AND {BRAZIL_LAT[1]}", "warning"),
        ("lng_in_brazil", f"lng dalam kotak Brasil {BRAZIL_LNG} (temuan #5: titik di luar = soft)",
         f"geolocation_lng NOT BETWEEN {BRAZIL_LNG[0]} AND {BRAZIL_LNG[1]}", "warning"),
    ],
    "order_items": [
        ("price_positive", "price > 0", "price <= 0", "error"),
        ("freight_nonneg", "freight_value >= 0", "freight_value < 0", "error"),
    ],
    "order_payments": [
        ("value_positive", "payment_value > 0 (temuan #8: 9 baris bernilai 0 = soft)", "payment_value <= 0", "warning"),
        ("type_known", "payment_type bukan 'not_defined' (temuan #8: 3 baris = soft)", "payment_type = 'not_defined'", "warning"),
        ("installments_ge1", "payment_installments >= 1 (temuan #8: 2 baris bernilai 0 = soft)", "payment_installments < 1", "warning"),
    ],
    "order_reviews": [
        ("score_1_5", "review_score di 1..5", "review_score NOT BETWEEN 1 AND 5", "error"),
        ("answer_after_creation", "answer >= creation (temuan #15: 0 baris saat ini; soft)", "review_answer_timestamp < review_creation_date", "warning"),
    ],
    "orders": [
        ("status_known", "order_status salah satu dari 8 status",
         "order_status NOT IN ('delivered','shipped','canceled','unavailable','invoiced','processing','created','approved')", "error"),
        ("approved_after_purchase", "approved_at >= purchase", "order_approved_at < order_purchase_timestamp", "error"),
        ("delivered_has_date", "status delivered wajib punya tanggal tiba (temuan #9: 8 baris = soft)",
         "order_status = 'delivered' AND order_delivered_customer_date IS NULL", "warning"),
        ("customer_after_carrier", "tiba >= diserahkan ke kurir (temuan #9: 23 baris = soft)",
         "order_delivered_customer_date < order_delivered_carrier_date", "warning"),
    ],
    "products": [
        ("weight_positive", "product_weight_g > 0 (temuan #16: 4 baris bernilai 0 = soft)",
         "product_weight_g <= 0", "warning"),
    ],
    "sellers": [
        ("state_len2", "seller_state harus 2 huruf (UF)", "length(seller_state) <> 2", "error"),
    ],
}


# Ambang karantina per tabel: (persen baris, alasan). Lebih dari ambang → tabel FAIL (tidak dimuat).
# Ambang hanya berlaku untuk pelanggaran HARD; aturan soft tidak pernah mengarantina.
THRESHOLDS: dict[str, tuple[float, str]] = {
    "customers": (0.0, "kunci & FK ke orders; satu baris rusak = indikasi file korup → batch berhenti"),
    "geolocation": (2.0, "titik di luar Brasil (1,17%) soft: disimpan + flag, tidak dihitung; ambang untuk pelanggaran hard (tipe/null); >2% berarti sumber rusak"),
    "order_items": (0.0, "sumber pendapatan Q1; baris tanpa order/produk/seller tak boleh lolos"),
    "order_payments": (0.1, "14 baris janggal (0,01%) bersifat soft (simpan + flag); ambang untuk pelanggaran hard; >0,1% berarti anomali baru"),
    "order_reviews": (0.5, "skor di luar 1–5 mengubah rata-rata Q2; toleransi kecil"),
    "orders": (0.1, "31 baris waktu janggal (0,03%) bersifat soft (simpan + flag); ambang untuk pelanggaran hard; >0,1% = anomali baru"),
    "products": (0.1, "kategori null (1,85%) bukan error — jadi 'unknown', bukan karantina"),
    "sellers": (0.0, "hanya 3.095 baris; kunci & FK harus bersih"),
    "product_category_translation": (0.0, "71 baris; harus persis, BOM ditangani saat baca"),
}

# FK lintas tabel (temuan #13), semuanya HARD: baris anak yatim → karantina. (anak, kolom, induk, kolom induk)
FKS: tuple[tuple[str, str, str, str], ...] = (
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("order_items", "seller_id", "sellers", "seller_id"),
    ("order_payments", "order_id", "orders", "order_id"),
    ("order_reviews", "order_id", "orders", "order_id"),
)

# Urutan proses: induk sebelum anak (FK dicek terhadap baris induk yang lolos).
PROCESS_ORDER: tuple[str, ...] = (
    "customers", "sellers", "products", "product_category_translation", "geolocation",
    "orders", "order_items", "order_payments", "order_reviews",
)

# Kolom flag untuk aturan soft (simpan + flag). Tabel dengan aturan soft lain memakai nama bawaan.
FLAG_NAMES = {"geolocation": "is_outside_brazil", "orders": "has_time_anomaly"}
DEFAULT_FLAG = "has_dq_warning"


def _schema_object(t: Table) -> dict:
    props = []
    for c in t.cols:
        logical, opts = _types(c.type)
        p: dict = {
            "name": c.src, "logicalType": logical, "physicalType": c.type,
            "description": c.desc + (f" — {c.note}" if c.note else ""),
            "required": not c.nullable,
        }
        if opts:
            p["logicalTypeOptions"] = opts
        if t.key == (c.src,):
            p["primaryKey"] = True
            p["unique"] = True
        elif c.src in t.key:
            p["primaryKey"] = True
            p["primaryKeyPosition"] = t.key.index(c.src) + 1
        props.append(p)
    quality: list[dict] = [{
        "type": "library", "metric": "rowCount", "mustBe": t.rows,
        "name": "row_count", "description": f"jumlah baris harus {t.rows:,} (parser CSV, bukan wc -l)",
        "severity": "error",
    }]
    for name, desc, where, sev in RULES.get(t.name, []):
        quality.append(_sql(name, desc, where, sev, "{model}"))
    if len(t.key) > 1:
        quality.append({
            "type": "sql", "name": "composite_key_unique", "severity": "error",
            "description": f"({', '.join(t.key)}) unik",
            "query": f"SELECT COUNT(*) FROM (SELECT {', '.join(t.key)} FROM {{model}} GROUP BY {', '.join(t.key)} HAVING COUNT(*) > 1)",
            "mustBe": 0,
        })
    return {
        "name": t.name, "physicalName": t.name, "physicalType": "table",
        "description": t.desc, "properties": props, "quality": quality,
        "customProperties": [
            {"property": "sourceFile", "value": t.file},
            {"property": "naturalKey", "value": ", ".join(t.key) if t.key else "none"},
            {"property": "encoding", "value": t.encoding},
        ],
    }


def build() -> dict:
    return {
        "apiVersion": "v3.1.0", "kind": "DataContract",
        "id": "olist-raw", "name": "Olist E-Commerce — raw (landing)", "version": "1.0.0", "status": "active",
        "domain": "ecommerce", "dataProduct": "olist-data-platform",
        "description": {
            "purpose": "Kontrak 9 file CSV Olist yang masuk ke bronze/silver. Mendefinisikan tabel, tipe target, "
                       "kunci alami, jumlah baris, dan aturan kualitas (error=hard, warning=soft).",
            "limitations": "Dataset sudah beranonimisasi; 3 file punya kunci tak unik (geolocation, order_reviews).",
        },
        "servers": [{
            "server": "local-csv", "type": "local", "format": "csv", "delimiter": "comma",
            "path": f"{VIEW_DIR}/{{model}}.csv", "environment": "dev",
            "description": "CSV lokal; `python pipeline.py contract` membuat symlink data/contract/{tabel}.csv",
        }],
        "schema": [_schema_object(t) for t in TABLES],
        "team": {"name": "olist-data-platform"},
    }


def write(root: Path) -> Path:
    out = root / CONTRACT_PATH
    out.parent.mkdir(exist_ok=True)
    out.write_text(
        "# DIBANGKITKAN dari olist/schema.py oleh `python pipeline.py contract` — jangan edit tangan.\n"
        + yaml.safe_dump(build(), sort_keys=False, allow_unicode=True, width=110),
        encoding="utf-8",
    )
    return out


def link_data(root: Path, raw_dir: str = "data/raw") -> None:
    """Symlink {tabel}.csv → file asli, supaya server `path` bisa memakai placeholder {model}."""
    view = root / VIEW_DIR
    if view.exists():
        shutil.rmtree(view)
    view.mkdir(parents=True)
    for t in TABLES:
        (view / f"{t.name}.csv").symlink_to((root / raw_dir / t.file).resolve())
