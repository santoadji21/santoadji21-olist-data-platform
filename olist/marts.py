"""T5 — Mart: bangun mart.* dari dw (+ stream), ekspor ke gold, dan sediakan query bersama untuk laporan + dashboard.

Logika bisnis ada di SQL (sql/mart/*.sql) yang bisa diuji; dashboard hanya membaca mart lewat QUERIES di bawah —
query yang SAMA dipakai `outputs/marts_report.md` dan grafik, jadi angka keduanya tidak bisa berbeda.
"""
from __future__ import annotations

import io
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

MART_TABLES = ("sales_monthly", "revenue_vs_paid_monthly", "delivery_monthly", "seller_scorecard",
               "customer_rfm", "reconciliation", "stream_freshness", "stream_freshness_batch")

# Penelusuran angka: mart → file SQL → tabel fakta/dimensi sumber.
LINEAGE = {
    "sales_monthly": ("sql/mart/10_sales_monthly.sql", "dw.fact_order_item, dw.dim_product, dw.dim_date"),
    "revenue_vs_paid_monthly": ("sql/mart/10_sales_monthly.sql", "dw.fact_order, dw.dim_date"),
    "delivery_monthly": ("sql/mart/20_delivery_monthly.sql", "dw.fact_order (is_late), dw.fact_review, dw.dim_date"),
    "seller_scorecard": ("sql/mart/30_seller_scorecard.sql", "dw.fact_order_item, dw.fact_order, dw.fact_review, dw.dim_seller"),
    "customer_rfm": ("sql/mart/40_customer_rfm.sql", "dw.fact_order, dw.dim_customer"),
    "reconciliation": ("sql/mart/50_reconciliation.sql", "dw.fact_order, dw.fact_payment"),
    "stream_freshness": ("sql/mart/60_stream_freshness.sql", "stream.batch_metrics"),
    "stream_freshness_batch": ("sql/mart/60_stream_freshness.sql", "stream.batch_metrics"),
}


class MartCheckError(RuntimeError):
    pass


def run_marts(conn, root: Path) -> dict:
    t0 = time.time()
    timings = []
    with conn.transaction():
        for f in sorted((root / "sql" / "mart").glob("*.sql")):
            ts = time.time()
            conn.execute(f.read_text(encoding="utf-8"))
            timings.append({"file": f.name, "seconds": round(time.time() - ts, 2)})
        with conn.cursor() as cur:
            cur.execute((root / "sql" / "checks" / "mart_checks.sql").read_text(encoding="utf-8"))
            checks = [{"check": n, "bad_rows": int(b)} for n, b in cur.fetchall()]
        bad = [c for c in checks if c["bad_rows"] != 0]
        if bad:
            raise MartCheckError("pemeriksaan mart gagal: " + ", ".join(f"{c['check']}={c['bad_rows']}" for c in bad))
        rows = {}
        with conn.cursor() as cur:
            for t in MART_TABLES:
                cur.execute(f"SELECT count(*) FROM mart.{t}")
                rows[t] = int(cur.fetchone()[0])
    return {"stage": "marts", "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "seconds": round(time.time() - t0, 2), "files": timings, "rows": rows, "checks": checks}


def read_df(conn, sql: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(sql)
        cols = [d.name for d in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


def export_gold(s3, conn, bucket: str = "gold") -> list[dict]:
    """Ekspor tiap mart ke gold sebagai Parquet + CSV (dashboard/BI boleh membaca dari sini)."""
    from .bronze import ensure_bucket

    ensure_bucket(s3, bucket)
    out = []
    for t in MART_TABLES:
        df = read_df(conn, f"SELECT * FROM mart.{t}")
        pq = io.BytesIO()
        df.to_parquet(pq, engine="pyarrow", index=False)
        s3.put_object(Bucket=bucket, Key=f"olist/mart/{t}.parquet", Body=pq.getvalue())
        s3.put_object(Bucket=bucket, Key=f"olist/mart/{t}.csv", Body=df.to_csv(index=False).encode("utf-8"),
                      ContentType="text/csv")
        out.append({"table": t, "rows": len(df), "parquet_bytes": len(pq.getvalue())})
    return out


# ---- query bersama (laporan + dashboard) -----------------------------------------------------------
def queries(complete_only: bool = True) -> dict[str, str]:
    cm = "WHERE is_complete_month" if complete_only else ""
    cma = "AND is_complete_month" if complete_only else ""
    return {
        "q1_monthly": f"""
            SELECT s.year_month, s.is_complete_month, s.revenue, s.items, v.orders_compared, v.ordered_value,
                   v.paid_value, v.paid_minus_ordered
            FROM (SELECT year_month, bool_and(is_complete_month) AS is_complete_month, sum(revenue) AS revenue, sum(items) AS items
                  FROM mart.sales_monthly GROUP BY year_month) s
            JOIN mart.revenue_vs_paid_monthly v USING (year_month)
            {cm.replace('is_complete_month', 's.is_complete_month')} ORDER BY s.year_month""",
        "q1_category": f"""
            SELECT category, sum(revenue) AS revenue, sum(items) AS items FROM mart.sales_monthly
            {cm} GROUP BY category ORDER BY revenue DESC LIMIT 10""",
        "q1_total": f"SELECT sum(revenue) AS revenue, sum(items) AS items FROM mart.sales_monthly {cm}",
        "q2_state": f"""
            SELECT customer_state, sum(delivered_orders) AS delivered, sum(late_orders) AS late,
                   round(100.0 * sum(late_orders) / sum(delivered_orders), 2) AS late_rate_pct,
                   round(sum(avg_delivery_days * delivered_orders) / sum(delivered_orders), 1) AS avg_delivery_days
            FROM mart.delivery_monthly {cm} GROUP BY customer_state ORDER BY late_rate_pct DESC""",
        "q2_review": f"""
            SELECT sum(reviewed_on_time) AS n_on_time, sum(reviewed_late) AS n_late,
                   round(sum(coalesce(avg_review_on_time, 0) * reviewed_on_time) / nullif(sum(reviewed_on_time), 0), 2) AS avg_on_time,
                   round(sum(coalesce(avg_review_late, 0) * reviewed_late) / nullif(sum(reviewed_late), 0), 2) AS avg_late,
                   sum(delivered_orders) AS delivered, sum(late_orders) AS late,
                   round(100.0 * sum(late_orders) / sum(delivered_orders), 2) AS late_rate_pct
            FROM mart.delivery_monthly {cm}""",
        "q3_sellers": """
            SELECT seller_id, state, orders, revenue, delivered_orders, late_rate_pct, canceled_rate_pct, avg_review_score,
                   flag_late, flag_canceled, flag_review, needs_action
            FROM mart.seller_scorecard WHERE is_rated
            ORDER BY needs_action DESC, (flag_late::int + flag_canceled::int + flag_review::int) DESC, late_rate_pct DESC NULLS LAST""",
        "q3_summary": """
            SELECT count(*) AS sellers, count(*) FILTER (WHERE is_rated) AS rated,
                   count(*) FILTER (WHERE needs_action) AS needs_action, count(*) FILTER (WHERE flag_late) AS flag_late,
                   count(*) FILTER (WHERE flag_canceled) AS flag_canceled, count(*) FILTER (WHERE flag_review) AS flag_review,
                   sum(orders) FILTER (WHERE is_rated) AS rated_orders, sum(orders) AS seller_orders
            FROM mart.seller_scorecard""",
        "q4_segments": """
            SELECT segment, count(*) AS people, round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct,
                   sum(monetary) AS monetary, round(avg(monetary), 0) AS avg_monetary, round(avg(recency_days)) AS avg_recency_days
            FROM mart.customer_rfm GROUP BY segment ORDER BY people DESC""",
        "q4_frequency": "SELECT frequency, count(*) AS people FROM mart.customer_rfm GROUP BY frequency ORDER BY frequency",
        "fresh_batch": "SELECT * FROM mart.stream_freshness_batch ORDER BY consumed_at, batch_no",
        "fresh_minute": "SELECT * FROM mart.stream_freshness ORDER BY minute",
        "recon": """
            SELECT class, count(*) AS orders, round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct,
                   round(sum(abs(diff)), 2) AS abs_diff_brl, round(sum(diff), 2) AS net_diff_brl
            FROM mart.reconciliation GROUP BY class ORDER BY orders DESC""",
    }


def frames(conn, complete_only: bool = True) -> dict[str, pd.DataFrame]:
    return {k: read_df(conn, sql) for k, sql in queries(complete_only).items()}


def _f(x, d=0):
    return f"{float(x):,.{d}f}"


def insights(f: dict[str, pd.DataFrame]) -> dict[str, str]:
    """Satu kalimat insight per visual; semua angka dihitung dari frame (tidak diketik)."""
    m, tot, cat = f["q1_monthly"], f["q1_total"].iloc[0], f["q1_category"]
    ordered, paid = float(m.ordered_value.sum()), float(m.paid_value.sum())
    q1 = (f"Pendapatan {m.year_month.iloc[0]} s.d. {m.year_month.iloc[-1]} (bulan lengkap) {_f(tot.revenue)} BRL dari {_f(tot['items'])} item; "
          f"uang terbayar {_f(paid)} BRL vs nilai pesanan {_f(ordered)} BRL ({(paid - ordered) / ordered * 100:+.2f}%); "
          f"kategori terbesar {cat.category.iloc[0]} ({float(cat.revenue.iloc[0]) / float(tot.revenue) * 100:.1f}% pendapatan).")
    s, r = f["q2_state"], f["q2_review"].iloc[0]
    big = s[s.delivered >= 500]
    top, worst = s.iloc[0], big.iloc[0]
    q2 = (f"Late rate {_f(r.late_rate_pct, 2)}% dari {_f(r.delivered)} order terkirim; tertinggi di {top.customer_state} "
          f"({_f(top.late_rate_pct, 1)}%, n={_f(top.delivered)}) dan, di antara negara bagian dengan ≥ 500 order, {worst.customer_state} "
          f"({_f(worst.late_rate_pct, 1)}%, n={_f(worst.delivered)}); skor review turun dari "
          f"{_f(r.avg_on_time, 2)} (tepat waktu) menjadi {_f(r.avg_late, 2)} (terlambat).")
    sm, sl = f["q3_summary"].iloc[0], f["q3_sellers"]
    q3 = (f"{_f(sm.needs_action)} dari {_f(sm.rated)} seller yang dinilai (≥ 30 order, mencakup {sm.rated_orders / sm.seller_orders * 100:.0f}% order) "
          f"perlu ditindak: {_f(sm.flag_late)} karena late rate ≥ 15%, {_f(sm.flag_canceled)} karena batal ≥ 5%, "
          f"{_f(sm.flag_review)} karena skor ≤ 3,5 (seller bisa terkena lebih dari satu).")
    seg, fr = f["q4_segments"].set_index("segment"), f["q4_frequency"]
    people = int(fr.people.sum())
    rep = int(fr[fr.frequency >= 2].people.sum())
    hv = seg.loc["Bernilai tinggi, lama tidak kembali"]
    q4 = (f"{_f(rep)} dari {_f(people)} orang ({rep / people * 100:.1f}%) belanja ulang; {_f(hv.people)} orang bernilai tinggi "
          f"sudah lama tidak kembali (rata-rata {_f(hv.avg_recency_days)} hari, total {_f(hv.monetary)} BRL).")
    b = f["fresh_batch"]
    if len(b):
        k = max(len(b) // 4, 1)
        first, last = b.fresh_p50_ms.iloc[:k].astype(float).mean(), b.fresh_p50_ms.iloc[-k:].astype(float).mean()
        trend = "tumbuh" if last > 1.5 * first else "stabil"
        q5 = (f"Dari {_f(len(b))} batch (maks p95 {_f(b.fresh_p95_ms.astype(float).max())} ms), p50 batch awal {_f(first)} ms → "
              f"batch akhir {_f(last)} ms: lag {trend} selama replay.")
    else:
        q5 = "Belum ada metrik stream (jalankan `make stream` lalu `make marts`)."
    rc = f["recon"].set_index("class")
    n_all = int(rc.orders.sum())
    und = rc.loc["tidak_dijelaskan"] if "tidak_dijelaskan" in rc.index else None
    q6 = (f"{rc.loc['cocok'].orders / n_all * 100:.2f}% order cocok (toleransi 0,01 BRL); selisih tidak terjelaskan "
          f"{_f(und.orders) if und is not None else 0} order = {_f(und.abs_diff_brl, 2) if und is not None else '0'} BRL.")
    return {"q1": q1, "q2": q2, "q3": q3, "q4": q4, "fresh": q5, "recon": q6}


def _md_table(df: pd.DataFrame, cols: list[str], fmt: dict | None = None) -> list[str]:
    fmt = fmt or {}
    o = ["| " + " | ".join(cols) + " |", "|" + "|".join("---" for _ in cols) + "|"]
    for _, r in df.iterrows():
        o.append("| " + " | ".join(fmt.get(c, lambda v: str(v))(r[c]) for c in cols) + " |")
    return o


def report_md(run: dict, f: dict, ins: dict) -> str:
    n0 = lambda v: f"{float(v):,.0f}"
    n2 = lambda v: f"{float(v):,.2f}"
    o = ["# Laporan Mart (T5) — jawaban Q1–Q4\n",
         f"Dibangun {run['ran_at']} · {run['seconds']} s · {sum(c['bad_rows'] == 0 for c in run['checks'])}/{len(run['checks'])} "
         "pemeriksaan mart = 0. Tabel di bawah dihasilkan oleh query yang sama dengan grafik dashboard "
         "(`olist.marts.queries`); hanya **bulan lengkap** (Jan-2017 … Agu-2018) yang ditampilkan.\n",
         "## Q1 — Pendapatan per bulan dan kategori\n",
         "Pendapatan = Σ `price` item pada order berstatus bukan `canceled`/`unavailable` (ongkir terpisah).\n", ins["q1"] + "\n"]
    o += _md_table(f["q1_monthly"], ["year_month", "revenue", "items", "orders_compared", "ordered_value", "paid_value", "paid_minus_ordered"],
                   {c: n2 for c in ("revenue", "ordered_value", "paid_value", "paid_minus_ordered")} | {"items": n0, "orders_compared": n0})
    o += ["\nTop 10 kategori:\n"] + _md_table(f["q1_category"], ["category", "revenue", "items"], {"revenue": n2, "items": n0})
    o += ["\n## Q2 — Keterlambatan pengiriman dan dampaknya ke review\n",
          "Late rate = terlambat ÷ order dengan `is_late IS NOT NULL` (delivered dan tanggal tiba terisi).\n", ins["q2"] + "\n"]
    o += _md_table(f["q2_state"], ["customer_state", "delivered", "late", "late_rate_pct", "avg_delivery_days"],
                   {"delivered": n0, "late": n0})
    o += ["\n## Q3 — Seller yang perlu ditindak\n",
          "Hanya seller dengan ≥ 30 order yang dinilai. Ambang: late rate ≥ 15 %, batal ≥ 5 %, skor ≤ 3,5. "
          "Top 15 menurut jumlah flag lalu late rate:\n", ins["q3"] + "\n"]
    o += _md_table(f["q3_sellers"].head(15), ["seller_id", "state", "orders", "late_rate_pct", "canceled_rate_pct", "avg_review_score"],
                   {"orders": n0, "seller_id": lambda v: str(v)[:12]})
    o += ["\n## Q4 — Siapa pelanggan kita (RFM per orang)\n",
          "Titik acuan recency = tanggal beli terakhir di data; F memakai pita tetap (bukan kuintil mentah).\n", ins["q4"] + "\n"]
    o += _md_table(f["q4_segments"], ["segment", "people", "pct", "avg_monetary", "avg_recency_days", "monetary"],
                   {"people": n0, "avg_monetary": n0, "avg_recency_days": n0, "monetary": n0})
    o += ["\nDistribusi frekuensi (alasan F bukan kuintil):\n"] + _md_table(f["q4_frequency"], ["frequency", "people"], {"people": n0})
    o += ["\n## Freshness stream\n", ins["fresh"] + "\n"]
    if len(f["fresh_minute"]):
        o += _md_table(f["fresh_minute"], ["minute", "batches", "messages", "new_events", "duplicates", "out_of_order", "dlq",
                                           "fresh_p50_ms", "fresh_p95_ms_max"])
    o += ["\n## Rekonsiliasi (nilai pesanan vs dibayar)\n", ins["recon"] + "\n"]
    o += _md_table(f["recon"], ["class", "orders", "pct", "abs_diff_brl", "net_diff_brl"], {"orders": n0})
    o += ["\n## Penelusuran angka (mart → SQL → fakta)\n", "| Mart | SQL | Sumber |", "|---|---|---|"]
    o += [f"| mart.{k} | {v[0]} | {v[1]} |" for k, v in LINEAGE.items()]
    o.append("\nContoh penelusuran: pendapatan Q1 = `sum(mart.sales_monthly.revenue)` = "
             "`sum(dw.fact_order_item.price)` untuk order bukan `canceled`/`unavailable` (pemeriksaan `sales_revenue_equals_fact_items` = 0).")
    return "\n".join(o) + "\n"


def write_outputs(root: Path, run: dict, f: dict, ins: dict, exported: list | None) -> Path:
    out = root / "outputs"
    out.mkdir(exist_ok=True)
    (out / "marts.json").write_text(json.dumps({**run, "insights": ins, "gold_export": exported}, indent=2, ensure_ascii=False),
                                    encoding="utf-8")
    md = out / "marts_report.md"
    md.write_text(report_md(run, f, ins), encoding="utf-8")
    return md
