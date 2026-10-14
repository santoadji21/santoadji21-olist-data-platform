"""Pertanyaan dashboard (T5): SATU sumber query untuk Metabase dan laporan, dibaca dari mart.* (dibangun dbt).

Tiap pertanyaan punya SQL, jenis visual, dan satu kalimat insight berangka yang dihitung dari hasil query yang SAMA
— angka di kalimat tidak bisa berbeda dari angka di grafik. Hanya bulan lengkap yang ditampilkan.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

CM = "WHERE is_complete_month"


@dataclass(frozen=True)
class Question:
    key: str
    title: str
    sql: str
    display: str                                   # Metabase: line | bar | row | pie | table | scalar
    insight: Callable[[list[dict]], str]
    viz: dict | None = None                        # visualization_settings Metabase


def fetch(conn, sql: str) -> list[dict]:
    """Jalankan query, kembalikan baris sebagai dict (Decimal → float)."""
    with conn.cursor() as cur:
        cur.execute(sql)
        names = [d[0] for d in cur.description]
        return [dict(zip(names, (float(v) if hasattr(v, "as_tuple") else v for v in row))) for row in cur.fetchall()]


def n(x, d=0) -> str:
    """Format angka gaya Indonesia: 1.234,5"""
    s = f"{float(x):,.{d}f}"
    return s.replace(",", "_").replace(".", ",").replace("_", ".")


def _q1_monthly(r):
    a, b = r[0], r[-1]
    return (f"Pendapatan bulanan naik dari R$ {n(a['revenue'])} ({a['year_month']}) menjadi R$ {n(b['revenue'])} "
            f"({b['year_month']}); total {n(sum(x['revenue'] for x in r))} dari {n(sum(x['items'] for x in r))} item, "
            f"dan uang terbayar R$ {n(sum(x['paid_value'] for x in r))} melebihi nilai pesanan R$ {n(sum(x['ordered_value'] for x in r))} "
            f"(bunga cicilan/voucher).")


def _q1_category(r):
    tot = sum(x["revenue"] for x in r)
    return f"Kategori teratas {r[0]['category']} menyumbang R$ {n(r[0]['revenue'])}; 10 kategori teratas total R$ {n(tot)}."


def _q2_state(r):
    w, b = r[0], r[-1]
    return (f"{w['customer_state']} paling sering terlambat ({n(w['late_rate_pct'], 2)} %, {n(w['late'])} dari {n(w['delivered'])} order), "
            f"sedangkan {b['customer_state']} terbaik ({n(b['late_rate_pct'], 2)} %).")


def _q2_review(r):
    x = r[0]
    return (f"Order tepat waktu rata-rata diberi skor {n(x['avg_on_time'], 2)} ({n(x['n_on_time'])} ulasan), order terlambat "
            f"{n(x['avg_late'], 2)} ({n(x['n_late'])} ulasan); keterlambatan keseluruhan {n(x['late_rate_pct'], 2)} %.")


def _q3(r):
    x = r[0]
    return (f"{n(x['needs_action'])} dari {n(x['rated'])} seller yang dinilai (≥ 30 order) perlu tindakan "
            f"({n(x['flag_late'])} terlambat, {n(x['flag_canceled'])} batal, {n(x['flag_review'])} skor rendah); "
            f"seller yang dinilai mencakup {n(100 * x['rated_orders'] / x['seller_orders'], 1)} % order.")


def _q4(r):
    t = sum(x["people"] for x in r)
    a = r[0]
    return (f"Dari {n(t)} pembeli, segmen terbesar \"{a['segment']}\" berisi {n(a['people'])} orang ({n(a['pct'], 2)} %); "
            f"hanya {n(sum(x['people'] for x in r if x['segment'].startswith('Pelanggan setia')))} orang yang setia.")


def _q4_freq(r):
    one = next(x for x in r if x["frequency"] == 1)
    t = sum(x["people"] for x in r)
    return f"{n(100 * one['people'] / t, 1)} % pembeli ({n(one['people'])} dari {n(t)}) hanya membeli satu kali."


def _recon(r):
    c = next(x for x in r if x["class"] == "cocok")
    return (f"{n(c['pct'], 2)} % order ({n(c['orders'])}) nilai pesanannya cocok dengan uang terbayar (syarat ≥ 98 %); "
            f"sisanya dijelaskan: " + ", ".join(f"{x['class']} {n(x['orders'])}" for x in r if x["class"] != "cocok") + ".")


def _fresh(r):
    if not r:
        return "Belum ada metrik stream (jalankan `make stream` lalu `make dbt-build`)."
    k = max(len(r) // 4, 1)
    first = sum(float(x["fresh_p50_ms"] or 0) for x in r[:k]) / k
    last = sum(float(x["fresh_p50_ms"] or 0) for x in r[-k:]) / k
    return (f"Dari {n(len(r))} batch (p95 tertinggi {n(max(float(x['fresh_p95_ms'] or 0) for x in r))} ms), p50 batch awal {n(first)} ms → "
            f"batch akhir {n(last)} ms: lag {'tumbuh' if last > 1.5 * first else 'stabil'} selama replay.")


QUESTIONS: list[Question] = [
    Question("q1_monthly", "Q1 · Pendapatan vs uang terbayar per bulan", f"""
        SELECT s.year_month, s.revenue, s.items, v.orders_compared, v.ordered_value, v.paid_value, v.paid_minus_ordered
        FROM (SELECT year_month, bool_and(is_complete_month) AS is_complete_month, sum(revenue) AS revenue, sum(items) AS items
              FROM mart.sales_monthly GROUP BY year_month) s
        JOIN mart.revenue_vs_paid_monthly v USING (year_month)
        WHERE s.is_complete_month ORDER BY s.year_month""", "line", _q1_monthly,
             {"graph.dimensions": ["year_month"], "graph.metrics": ["revenue", "paid_value"]}),
    Question("q1_category", "Q1 · 10 kategori dengan pendapatan tertinggi", f"""
        SELECT category, sum(revenue) AS revenue, sum(items) AS items FROM mart.sales_monthly
        {CM} GROUP BY category ORDER BY revenue DESC LIMIT 10""", "row", _q1_category,
             {"graph.dimensions": ["category"], "graph.metrics": ["revenue"]}),
    Question("q2_state", "Q2 · Tingkat keterlambatan per negara bagian", f"""
        SELECT customer_state, sum(delivered_orders) AS delivered, sum(late_orders) AS late,
               round(100.0 * sum(late_orders) / sum(delivered_orders), 2) AS late_rate_pct,
               round(sum(avg_delivery_days * delivered_orders) / sum(delivered_orders), 1) AS avg_delivery_days
        FROM mart.delivery_monthly {CM} GROUP BY customer_state ORDER BY late_rate_pct DESC""", "bar", _q2_state,
             {"graph.dimensions": ["customer_state"], "graph.metrics": ["late_rate_pct"]}),
    Question("q2_review", "Q2 · Skor ulasan: tepat waktu vs terlambat", f"""
        SELECT sum(reviewed_on_time) AS n_on_time, sum(reviewed_late) AS n_late,
               round(sum(coalesce(avg_review_on_time, 0) * reviewed_on_time) / nullif(sum(reviewed_on_time), 0), 2) AS avg_on_time,
               round(sum(coalesce(avg_review_late, 0) * reviewed_late) / nullif(sum(reviewed_late), 0), 2) AS avg_late,
               sum(delivered_orders) AS delivered, sum(late_orders) AS late,
               round(100.0 * sum(late_orders) / sum(delivered_orders), 2) AS late_rate_pct
        FROM mart.delivery_monthly {CM}""", "table", _q2_review),
    Question("q3_sellers", "Q3 · Seller yang perlu tindakan", """
        SELECT seller_id, state, orders, revenue, late_rate_pct, canceled_rate_pct, avg_review_score,
               flag_late, flag_canceled, flag_review
        FROM mart.seller_scorecard WHERE needs_action
        ORDER BY (flag_late::int + flag_canceled::int + flag_review::int) DESC, late_rate_pct DESC NULLS LAST LIMIT 50""",
             "table", lambda r: "", None),   # insight dari q3_summary
    Question("q3_summary", "Q3 · Ringkasan scorecard seller", """
        SELECT count(*) AS sellers, count(*) FILTER (WHERE is_rated) AS rated,
               count(*) FILTER (WHERE needs_action) AS needs_action, count(*) FILTER (WHERE flag_late) AS flag_late,
               count(*) FILTER (WHERE flag_canceled) AS flag_canceled, count(*) FILTER (WHERE flag_review) AS flag_review,
               sum(orders) FILTER (WHERE is_rated) AS rated_orders, sum(orders) AS seller_orders
        FROM mart.seller_scorecard""", "table", _q3),
    Question("q4_segments", "Q4 · Segmen pelanggan (RFM)", """
        SELECT segment, count(*) AS people, round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct,
               sum(monetary) AS monetary, round(avg(monetary), 0) AS avg_monetary, round(avg(recency_days)) AS avg_recency_days
        FROM mart.customer_rfm GROUP BY segment ORDER BY people DESC""", "row", _q4,
             {"graph.dimensions": ["segment"], "graph.metrics": ["people"]}),
    Question("q4_frequency", "Q4 · Berapa kali orang membeli", """
        SELECT frequency, count(*) AS people FROM mart.customer_rfm GROUP BY frequency ORDER BY frequency""", "bar", _q4_freq,
             {"graph.dimensions": ["frequency"], "graph.metrics": ["people"]}),
    Question("recon", "Rekonsiliasi · nilai pesanan vs uang terbayar", """
        SELECT class, count(*) AS orders, round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct,
               round(sum(abs(diff)), 2) AS abs_diff_brl, round(sum(diff), 2) AS net_diff_brl
        FROM mart.reconciliation GROUP BY class ORDER BY orders DESC""", "row", _recon,
             {"graph.dimensions": ["class"], "graph.metrics": ["orders"]}),
    Question("fresh_batch", "Freshness stream · p50/p95 per batch consumer", """
        SELECT batch_no, consumed_at, n_messages, n_new, n_duplicate, n_out_of_order, n_dlq, fresh_p50_ms, fresh_p95_ms
        FROM mart.stream_freshness_batch ORDER BY consumed_at, batch_no""", "line", _fresh,
             {"graph.dimensions": ["batch_no"], "graph.metrics": ["fresh_p50_ms", "fresh_p95_ms"]}),
]
