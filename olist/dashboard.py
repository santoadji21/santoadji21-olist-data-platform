"""T5 — Dashboard: grafik dari mart.* (Postgres). Dipakai notebook (notebooks/dashboard.ipynb) dan `make dashboard` (PNG).

Prinsip (panduan dataviz): satu sumbu per grafik (tanpa dual-axis), warna menurut fungsi (biru = utama, oranye = perlu
perhatian/terlambat, abu = netral), tanda tipis, grid pudar, teks memakai warna teks (bukan warna seri), label langsung
secukupnya, dan tiap grafik memuat satuan, periode, jumlah observasi, serta satu kalimat insight yang dihitung dari data.
Palet biru/oranye divalidasi dengan validate_palette.js (ΔE CVD 9,2; normal 24,0).
"""
from __future__ import annotations

import os
import textwrap
from pathlib import Path

import matplotlib.pyplot as plt

from .marts import frames, insights

BLUE, ORANGE, GRAY = "#2a78d6", "#eb6834", "#a8a7a2"
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e6e5e1"
PERIOD = "bulan lengkap Jan-2017 – Agu-2018"


def connect():
    """Koneksi ke Postgres: dari environment (container Jupyter) atau dari .env di root proyek (laptop)."""
    import psycopg

    env = dict(os.environ)
    dotenv = Path(__file__).resolve().parent.parent / ".env"
    if dotenv.exists():
        for line in dotenv.read_text(encoding="utf-8").splitlines():
            if line.strip() and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env.setdefault(k.strip(), v.strip())
    return psycopg.connect(host=env.get("POSTGRES_HOST", "localhost"), port=env.get("POSTGRES_PORT", "5432"),
                           dbname=env.get("POSTGRES_DB", "olist"), user=env.get("POSTGRES_USER", "olist"),
                           password=env.get("POSTGRES_PASSWORD", ""), connect_timeout=5)


def _style(ax, grid="y"):
    ax.set_facecolor(SURFACE)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=8, length=0)
    ax.grid(axis=grid, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)


def _fig(title, insight, ncols=1, w=11, h=5.2):
    fig, axes = plt.subplots(1, ncols, figsize=(w, h), facecolor=SURFACE, squeeze=False)
    fig.text(0.02, 0.965, title, color=INK, fontsize=12.5, fontweight="bold", va="top")
    fig.text(0.02, 0.915, "\n".join(textwrap.wrap(insight, 125)), color=INK2, fontsize=8.8, va="top", linespacing=1.4)
    return fig, axes[0]


def _finish(fig, top=0.80):
    fig.tight_layout(rect=(0, 0, 1, top))
    return fig


def fig_q1(f, ins):
    m, c = f["q1_monthly"], f["q1_category"]
    fig, (a, b) = _fig("Q1 — Pendapatan per bulan dan per kategori",
                       ins["q1"], 2)
    a.bar(m.year_month, m.revenue.astype(float) / 1e6, color=BLUE, width=0.62)
    a.set_title("Pendapatan per bulan (juta BRL)", loc="left", fontsize=9.5, color=INK)
    a.set_xticks(range(len(m)))
    a.set_xticklabels(m.year_month, rotation=60, ha="right")
    a.set_ylabel(f"juta BRL · {PERIOD} · n = {int(m['items'].sum()):,} item", color=INK2, fontsize=8)
    last = m.iloc[-1]
    a.annotate(f"{float(last.revenue) / 1e6:.2f}", (len(m) - 1, float(last.revenue) / 1e6), ha="center", va="bottom", fontsize=8, color=INK)
    _style(a)
    cc = c.iloc[::-1]
    b.barh(cc.category, cc.revenue.astype(float) / 1e6, color=BLUE, height=0.6)
    for y, v in enumerate(cc.revenue.astype(float) / 1e6):
        b.text(v, y, f" {v:.2f}", va="center", fontsize=8, color=INK)
    b.set_title("10 kategori teratas (juta BRL)", loc="left", fontsize=9.5, color=INK)
    b.set_xlabel(f"juta BRL · {PERIOD}", color=INK2, fontsize=8)
    _style(b, "x")
    return _finish(fig)


def fig_q2(f, ins):
    s, r = f["q2_state"], f["q2_review"].iloc[0]
    fig, (a, b) = _fig("Q2 — Keterlambatan pengiriman dan skor review", ins["q2"], 2)
    s = s.iloc[::-1]
    a.barh(s.customer_state, s.late_rate_pct.astype(float), color=BLUE, height=0.62)
    top = s.tail(3)
    for y, (_, row) in enumerate(s.iterrows()):
        if row.name in top.index:
            a.text(float(row.late_rate_pct), y, f" {float(row.late_rate_pct):.1f}% (n={int(row.delivered):,})", va="center", fontsize=7.5, color=INK)
    a.set_title("Late rate per negara bagian pelanggan (%)", loc="left", fontsize=9.5, color=INK)
    a.set_xlabel(f"% order terkirim yang terlambat · {PERIOD} · n = {int(r.delivered):,}", color=INK2, fontsize=8)
    a.tick_params(axis="y", labelsize=7)
    _style(a, "x")
    b.bar(["Tepat waktu", "Terlambat"], [float(r.avg_on_time), float(r.avg_late)], color=[BLUE, ORANGE], width=0.5)
    for x, (v, n) in enumerate([(r.avg_on_time, r.n_on_time), (r.avg_late, r.n_late)]):
        b.text(x, float(v), f"{float(v):.2f}\nn={int(n):,}", ha="center", va="bottom", fontsize=9, color=INK)
    b.set_ylim(0, 5.2)
    b.set_title("Rata-rata skor review (1–5)", loc="left", fontsize=9.5, color=INK)
    b.set_ylabel("skor review (1–5)", color=INK2, fontsize=8)
    _style(b)
    return _finish(fig)


def fig_q3(f, ins):
    s = f["q3_sellers"]
    fig, (a,) = _fig("Q3 — Seller yang perlu ditindak", ins["q3"], 1)
    ok, flag = s[~s.needs_action], s[s.needs_action]
    a.scatter(ok.late_rate_pct.astype(float), ok.avg_review_score.astype(float), s=26, color=BLUE, alpha=0.55,
              edgecolors=SURFACE, linewidths=0.6, label=f"Dinilai, aman (n={len(ok):,})")
    a.scatter(flag.late_rate_pct.astype(float), flag.avg_review_score.astype(float), s=34, color=ORANGE,
              edgecolors=SURFACE, linewidths=0.6, label=f"Perlu ditindak (n={len(flag):,})")
    a.axvline(15, color=GRAY, linewidth=1, linestyle=(0, (4, 3)))
    a.axhline(3.5, color=GRAY, linewidth=1, linestyle=(0, (4, 3)))
    a.text(15.4, a.get_ylim()[0] + 0.05, "late rate 15%", fontsize=7.5, color=INK2)
    a.text(a.get_xlim()[1], 3.55, "skor 3,5 ", fontsize=7.5, color=INK2, ha="right")
    a.set_xlabel("late rate seller (% order terkirim yang terlambat)", color=INK2, fontsize=8)
    a.set_ylabel("rata-rata skor review (1–5)", color=INK2, fontsize=8)
    a.legend(frameon=False, fontsize=8, labelcolor=INK2, loc="lower left")
    _style(a, "both")
    return _finish(fig)


def fig_q4(f, ins):
    s = f["q4_segments"].iloc[::-1]
    fig, (a, b) = _fig("Q4 — Siapa pelanggan kita (RFM per orang)", ins["q4"], 2)
    a.barh(s.segment, s.people.astype(float), color=BLUE, height=0.6)
    for y, (_, r) in enumerate(s.iterrows()):
        a.text(float(r.people), y, f" {int(r.people):,} ({float(r.pct):.1f}%)", va="center", fontsize=8, color=INK)
    a.set_title("Orang per segmen", loc="left", fontsize=9.5, color=INK)
    a.set_xlabel(f"jumlah orang · n = {int(s.people.sum()):,} · acuan recency = tanggal beli terakhir di data", color=INK2, fontsize=8)
    a.set_xlim(0, float(s.people.max()) * 1.3)
    _style(a, "x")
    b.barh(s.segment, s.monetary.astype(float) / 1e6, color=BLUE, height=0.6)
    for y, v in enumerate(s.monetary.astype(float) / 1e6):
        b.text(v, y, f" {v:.2f}", va="center", fontsize=8, color=INK)
    b.set_yticklabels([])
    b.set_title("Nilai belanja per segmen (juta BRL)", loc="left", fontsize=9.5, color=INK)
    b.set_xlabel("juta BRL (Σ price item bernilai)", color=INK2, fontsize=8)
    b.set_xlim(0, float(s.monetary.astype(float).max()) / 1e6 * 1.25)
    _style(b, "x")
    return _finish(fig)


def fig_fresh(f, ins):
    b = f["fresh_batch"]
    fig, (a,) = _fig("Freshness stream — keterlambatan consumer", ins["fresh"], 1)
    if len(b):
        x = range(1, len(b) + 1)
        a.plot(x, b.fresh_p50_ms.astype(float), color=BLUE, linewidth=2, label="p50 per batch")
        a.plot(x, b.fresh_p95_ms.astype(float), color=ORANGE, linewidth=2, label="p95 per batch")
        a.legend(frameon=False, fontsize=8, labelcolor=INK2, loc="upper left")
    a.set_xlabel(f"urutan batch consumer (n = {len(b):,} batch, consumed_at − produced_at)", color=INK2, fontsize=8)
    a.set_ylabel("milidetik (ms)", color=INK2, fontsize=8)
    _style(a)
    return _finish(fig)


FIGURES = {"q1_pendapatan": fig_q1, "q2_pengiriman": fig_q2, "q3_seller": fig_q3, "q4_pelanggan": fig_q4,
           "freshness_stream": fig_fresh}


def build(conn, complete_only: bool = True):
    f = frames(conn, complete_only)
    ins = insights(f)
    return {name: fn(f, ins) for name, fn in FIGURES.items()}, ins


def save_all(conn, out_dir: Path) -> list[Path]:
    figs, _ = build(conn)
    out_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for name, fig in figs.items():
        p = out_dir / f"{name}.png"
        fig.savefig(p, dpi=140, facecolor=SURFACE)
        plt.close(fig)
        paths.append(p)
    return paths
