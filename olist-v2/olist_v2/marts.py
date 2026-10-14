"""T5 — laporan mart dan ekspor gold. Mart sendiri dibangun dbt (`make dbt-build`); modul ini:

  * menjalankan 17 pemeriksaan mart (dbt/tests/mart_checks.sql, referensi `ref()` diganti nama skema) — harus 0 semua
  * mengekspor tiap mart ke bucket gold (Parquet + CSV)
  * menulis outputs/marts_report.md + marts.json dari olist_v2.questions: query dan insight yang SAMA dengan dashboard Metabase
"""
from __future__ import annotations

import io
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from .bronze import ensure_bucket, make_s3
from .contract import REPO
from .questions import QUESTIONS, fetch, n

GOLD = "gold"
MART_TABLES = ("sales_monthly", "revenue_vs_paid_monthly", "delivery_monthly", "seller_scorecard", "customer_rfm", "reconciliation",
               "stream_freshness", "stream_freshness_batch")
SHOW_ROWS = 15

# Penelusuran angka: mart → model dbt → tabel fakta/dimensi sumber.
LINEAGE = {
    "sales_monthly": ("dbt/models/mart/sales_monthly.sql", "dw.fact_order_item, dw.dim_product, dw.dim_date"),
    "revenue_vs_paid_monthly": ("dbt/models/mart/revenue_vs_paid_monthly.sql", "dw.fact_order, dw.dim_date"),
    "delivery_monthly": ("dbt/models/mart/delivery_monthly.sql", "dw.fact_order (is_late), dw.fact_review, dw.dim_date"),
    "seller_scorecard": ("dbt/models/mart/seller_scorecard.sql", "dw.fact_order_item, dw.fact_order, dw.fact_review, dw.dim_seller"),
    "customer_rfm": ("dbt/models/mart/customer_rfm.sql", "dw.fact_order, dw.dim_customer"),
    "reconciliation": ("dbt/models/mart/reconciliation.sql", "dw.fact_order, dw.fact_payment"),
    "stream_freshness": ("dbt/models/mart/stream_freshness.sql", "stream.batch_metrics"),
    "stream_freshness_batch": ("dbt/models/mart/stream_freshness_batch.sql", "stream.batch_metrics"),
}


class MartCheckError(RuntimeError):
    pass


def run_checks(conn, dbt_dir: Path) -> list[dict]:
    """Jalankan dbt/tests/mart_checks.sql apa adanya (ref() → skema.tabel); satu baris hasil = satu pemeriksaan GAGAL."""
    sql = (dbt_dir / "tests" / "mart_checks.sql").read_text(encoding="utf-8")
    inner = sql.split("select * from (", 1)[1].rsplit(") t where", 1)[0]
    inner = re.sub(r"\{\{\s*ref\('(\w+)'\)\s*\}\}", lambda m: ("mart." if m.group(1) in MART_TABLES else "dw.") + m.group(1), inner)
    inner = re.sub(r"\{\{\s*source\('(\w+)',\s*'(\w+)'\)\s*\}\}", r"\1.\2", inner)
    return [{"check": r["check_name"], "bad_rows": int(r["bad_rows"])} for r in fetch(conn, inner)]


def export_gold(s3, conn) -> list[dict]:
    import pandas as pd

    ensure_bucket(s3, GOLD)
    out = []
    for t in MART_TABLES:
        df = pd.DataFrame(fetch(conn, f"SELECT * FROM mart.{t}"))
        pq = io.BytesIO()
        df.to_parquet(pq, engine="pyarrow", index=False)
        s3.put_object(Bucket=GOLD, Key=f"olist/mart/{t}.parquet", Body=pq.getvalue())
        s3.put_object(Bucket=GOLD, Key=f"olist/mart/{t}.csv", Body=df.to_csv(index=False).encode("utf-8"), ContentType="text/csv")
        out.append({"table": t, "rows": len(df), "parquet_bytes": len(pq.getvalue())})
    return out


def _cell(v) -> str:
    if isinstance(v, bool) or v is None or isinstance(v, str):
        return str(v)
    if isinstance(v, int):
        return n(v)
    if isinstance(v, float):
        return n(v, 0 if v == int(v) else 2)
    return str(v)


def md_table(rows: list[dict]) -> list[str]:
    if not rows:
        return ["_(tidak ada baris)_"]
    cols = list(rows[0])
    o = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    o += ["| " + " | ".join(_cell(r[c]) for c in cols) + " |" for r in rows[:SHOW_ROWS]]
    if len(rows) > SHOW_ROWS:
        o.append(f"\n_{len(rows) - SHOW_ROWS} baris lain tidak ditampilkan._")
    return o


def build_report(conn, checks: list[dict], exported: list[dict], seconds: float) -> tuple[str, dict]:
    ran_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    results = {q.key: fetch(conn, q.sql) for q in QUESTIONS}
    insights = {q.key: q.insight(results[q.key]) for q in QUESTIONS}
    insights["q3_sellers"] = insights["q3_summary"]
    ok = sum(c["bad_rows"] == 0 for c in checks)
    o = ["# Laporan Mart (T5)\n",
         f"Dibangun {ran_at} · {seconds} s · {ok}/{len(checks)} pemeriksaan mart = 0 (`dbt/tests/mart_checks.sql`). "
         "Setiap bagian di bawah memakai query dan kalimat insight yang SAMA dengan kartu di dashboard Metabase "
         "(`olist_v2/questions.py`); hanya **bulan lengkap** yang ditampilkan.\n"]
    for q in QUESTIONS:
        o += [f"## {q.title}\n"]
        if insights[q.key]:
            o += [f"**Insight:** {insights[q.key]}\n"]
        o += md_table(results[q.key]) + [""]
    o += ["## Pemeriksaan mart\n", "| Pemeriksaan | Baris bermasalah |", "|---|---:|"]
    o += [f"| {c['check']} | {c['bad_rows']} |" for c in checks]
    o += ["\n## Ekspor gold\n", "| Mart | Baris | Parquet (byte) |", "|---|---:|---:|"]
    o += [f"| s3://{GOLD}/olist/mart/{e['table']} | {e['rows']:,} | {e['parquet_bytes']:,} |" for e in exported]
    o += ["\n## Penelusuran angka (mart → model dbt → fakta)\n", "| Mart | Model | Sumber |", "|---|---|---|"]
    o += [f"| mart.{k} | {v[0]} | {v[1]} |" for k, v in LINEAGE.items()]
    o.append("\nContoh penelusuran: pendapatan Q1 = `sum(mart.sales_monthly.revenue)` = `sum(dw.fact_order_item.price)` untuk order "
             "bukan `canceled`/`unavailable` (pemeriksaan `sales_revenue_equals_fact_items` = 0).")
    return "\n".join(o) + "\n", {"stage": "marts", "ran_at": ran_at, "seconds": seconds, "checks": checks, "gold_export": exported,
                                 "insights": insights, "answers": {k: v for k, v in results.items() if len(v) <= 50}}


def main(env=os.environ) -> int:
    import psycopg2

    t0 = time.time()
    dbt_dir = Path(env.get("DBT_PROJECT_DIR", REPO / "dbt"))
    conn = psycopg2.connect(env["WAREHOUSE_DSN"], connect_timeout=5)
    try:
        checks = run_checks(conn, dbt_dir)
        bad = [c for c in checks if c["bad_rows"]]
        if bad:
            print("pemeriksaan mart gagal: " + ", ".join(f"{c['check']}={c['bad_rows']}" for c in bad), file=sys.stderr)
            return 1
        exported = export_gold(make_s3(env), conn)
        md, run = build_report(conn, checks, exported, round(time.time() - t0, 2))
    finally:
        conn.close()
    out = Path(env.get("OLIST_OUTPUTS", REPO / "outputs"))
    out.mkdir(parents=True, exist_ok=True)
    (out / "marts_report.md").write_text(md, encoding="utf-8")
    (out / "marts.json").write_text(json.dumps(run, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    print(f"  {len(checks)} pemeriksaan = 0 · gold: {sum(e['rows'] for e in exported):,} baris di {len(exported)} mart → outputs/marts_report.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
