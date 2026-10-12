"""T4 — Bangun star schema `dw` dari raw.* memakai SQL di sql/dw/*.sql (urut nama file).

Rebuild penuh dalam SATU transaksi: DROP SCHEMA dw CASCADE → CTAS tiap tabel → ADD PRIMARY KEY (tes grain gratis:
gagal bila grain salah) → ADD FOREIGN KEY (gagal bila ada yatim) → pemeriksaan integritas. Ada yang gagal/≠ 0
→ rollback, dw lama tetap utuh. Alasan rebuild penuh: dw murni turunan raw (±1,5 juta baris, ±10 detik), jadi
tidak ada logika inkremental yang bisa melenceng; idempoten per definisi.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

DW_TABLES = ("dim_date", "dim_geo", "dim_customer", "dim_product", "dim_seller",
             "fact_order", "fact_order_item", "fact_payment", "fact_review")


class IntegrityError(RuntimeError):
    pass


def sql_files(root: Path) -> list[Path]:
    return sorted((root / "sql" / "dw").glob("*.sql"))


def integrity_checks(conn, root: Path) -> list[dict]:
    sql = (root / "sql" / "checks" / "dw_integrity.sql").read_text(encoding="utf-8")
    with conn.cursor() as cur:
        cur.execute(sql)
        return [{"check": n, "bad_rows": int(b)} for n, b in cur.fetchall()]


def key_numbers(conn) -> dict:
    q = {
        "unique_people": "SELECT count(*) FROM dw.dim_customer",
        "orders": "SELECT count(*) FROM dw.fact_order",
        "order_items": "SELECT count(*) FROM dw.fact_order_item",
        "orders_without_items": "SELECT count(*) FROM dw.fact_order WHERE NOT has_items",
        "orders_without_payment": "SELECT count(*) FROM dw.fact_order WHERE NOT has_payment",
        "orders_with_review": "SELECT count(*) FROM dw.fact_review",
        "orders_with_multiple_reviews": "SELECT count(*) FROM dw.fact_review WHERE review_count > 1",
        "delivered": "SELECT count(*) FROM dw.fact_order WHERE order_status = 'delivered'",
        "delivered_with_date": "SELECT count(*) FROM dw.fact_order WHERE is_late IS NOT NULL",
        "late_orders": "SELECT count(*) FROM dw.fact_order WHERE is_late",
        "complete_months": "SELECT count(DISTINCT year_month) FROM dw.dim_date WHERE is_complete_month AND date <= '2018-08-31'",
        "customers_geo_unknown": "SELECT count(*) FROM dw.dim_customer WHERE geo_key = 'unknown'",
        "sellers_geo_unknown": "SELECT count(*) FROM dw.dim_seller WHERE geo_key = 'unknown'",
        "products_category_fallback_pt": "SELECT count(*) FROM dw.dim_product WHERE category_pt IS NOT NULL AND category_en = category_pt",
        "products_category_unknown": "SELECT count(*) FROM dw.dim_product WHERE category_en = 'unknown'",
    }
    out = {}
    with conn.cursor() as cur:
        for k, s in q.items():
            cur.execute(s)
            out[k] = int(cur.fetchone()[0])
    out["late_rate_pct"] = round(100.0 * out["late_orders"] / out["delivered_with_date"], 2) if out["delivered_with_date"] else None
    with conn.cursor() as cur:
        cur.execute("SELECT string_agg(year_month, ', ' ORDER BY year_month) FROM (SELECT DISTINCT year_month FROM dw.dim_date WHERE is_complete_month) x")
        out["complete_months_list"] = cur.fetchone()[0]
    return out


def run_model(conn, root: Path) -> dict:
    t0 = time.time()
    files = sql_files(root)
    timings = []
    with conn.transaction():
        for f in files:
            ts = time.time()
            conn.execute(f.read_text(encoding="utf-8"))
            timings.append({"file": f.name, "seconds": round(time.time() - ts, 2)})
        checks = integrity_checks(conn, root)
        bad = [c for c in checks if c["bad_rows"] != 0]
        if bad:
            raise IntegrityError("pemeriksaan integritas gagal: " + ", ".join(f"{c['check']}={c['bad_rows']}" for c in bad))
        rows = {}
        with conn.cursor() as cur:
            for t in DW_TABLES:
                cur.execute(f"SELECT count(*) FROM dw.{t}")
                rows[t] = int(cur.fetchone()[0])
        nums = key_numbers(conn)
    return {"stage": "model", "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "seconds": round(time.time() - t0, 2), "files": timings, "rows": rows, "checks": checks, "key_numbers": nums}


GRAIN = {
    "dim_date": "satu baris per tanggal kalender", "dim_geo": "satu baris per zip prefix (+ baris `unknown`)",
    "dim_customer": "satu baris per ORANG (`customer_unique_id`)", "dim_product": "satu baris per produk",
    "dim_seller": "satu baris per seller", "fact_order": "satu baris per order (header)",
    "fact_order_item": "satu baris per unit barang", "fact_payment": "satu baris per pembayaran",
    "fact_review": "satu baris per order yang punya ulasan",
}


def report_md(run: dict) -> str:
    n = run["key_numbers"]
    o = ["# Laporan Star Schema `dw` (T4)\n",
         f"Dibangun {run['ran_at']} · {run['seconds']} s · satu transaksi (DROP + CTAS + PK + FK + pemeriksaan)\n",
         "## Jumlah baris dan grain\n", "| Tabel | Baris | Grain |", "|---|---:|---|"]
    for t, r in run["rows"].items():
        o.append(f"| dw.{t} | {r:,} | {GRAIN[t]} |")
    o += ["\n## Integritas (semua harus 0 baris bermasalah)\n", "| Pemeriksaan | Baris bermasalah |", "|---|---:|"]
    for c in run["checks"]:
        o.append(f"| {c['check']} | {c['bad_rows']} |")
    o.append(f"\n**{sum(1 for c in run['checks'] if c['bad_rows'] == 0)}/{len(run['checks'])} pemeriksaan lulus.** "
             "PK dan FK terpasang sebagai constraint (bukan hanya didokumentasikan).")
    o += ["\n## Angka kunci\n", "| Besaran | Nilai |", "|---|---:|"]
    labels = [("unique_people", "orang unik (`dim_customer`)"), ("orders", "`fact_order`"), ("order_items", "`fact_order_item`"),
              ("orders_without_items", "order tanpa item"), ("orders_without_payment", "order tanpa pembayaran"),
              ("orders_with_review", "order dengan ≥ 1 ulasan (`fact_review`)"),
              ("orders_with_multiple_reviews", "order dengan > 1 ulasan"), ("delivered", "order `delivered`"),
              ("delivered_with_date", "delivered dengan tanggal tiba (`is_late` tidak NULL)"), ("late_orders", "terlambat"),
              ("late_rate_pct", "late rate (% dari yang tidak NULL)"), ("complete_months", "bulan lengkap (≤ Agu-2018)"),
              ("customers_geo_unknown", "pelanggan → `dim_geo` unknown"), ("sellers_geo_unknown", "seller → `dim_geo` unknown"),
              ("products_category_fallback_pt", "produk dengan kategori Portugis (tanpa terjemahan)"),
              ("products_category_unknown", "produk tanpa kategori (`unknown`)")]
    for k, lab in labels:
        v = n[k]
        o.append(f"| {lab} | {v:,} |" if isinstance(v, int) else f"| {lab} | {v} |")
    o.append(f"\nBulan lengkap: {n['complete_months_list']}.")
    o.append("\n## Waktu per file SQL\n\n| File | Detik |\n|---|---:|")
    o += [f"| {f['file']} | {f['seconds']} |" for f in run["files"]]
    return "\n".join(o) + "\n"


def write_outputs(root: Path, run: dict) -> Path:
    out = root / "outputs"
    out.mkdir(exist_ok=True)
    (out / "dw.json").write_text(json.dumps(run, indent=2, ensure_ascii=False), encoding="utf-8")
    md = out / "dw_report.md"
    md.write_text(report_md(run), encoding="utf-8")
    return md
