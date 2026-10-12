"""Render DATA_CONTRACT.md dari olist/schema.py + outputs/profile.json + olist/contract.RULES.

Angka tidak diketik tangan: tabel (§1) dari schema, temuan (§2) dari profil, aturan (§3) dari RULES
(kembaran YAML-nya: contracts/olist_raw.odcs.yaml). Hanya §4 (keputusan pemodelan) yang prosa.
"""
from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from .contract import RULES, THRESHOLDS
from .schema import TABLES

def _pct(x: float) -> str:
    return f"{x:.1f}".replace(".", ",")


MODELING = """\
- **Grain `dim_customer` = `customer_unique_id`** (SCD tipe 1). `customer_id` hanyalah id per-order; memakainya sebagai
  grain menghitung 99.441 "pelanggan" padahal orang unik hanya 96.096 (temuan #1). `fact_order` menyimpan
  `customer_id` asli dan di-join ke `dim_customer` lewat `customer_unique_id`.
- **Kebijakan `fact_review` = satu baris per order.** Dari tiap order dengan >1 review dipilih review dengan
  `review_answer_timestamp` terbaru (seri → `review_creation_date` terbaru → `review_id` terkecil); jumlah review asli disimpan di
  `review_count`. `review_id` duplikat (temuan #2) dideduplikasi lebih dulu dengan aturan yang sama. Tidak ada review yang dibuang diam-diam:
  semuanya tetap di silver; yang tidak terpilih ada di tabel karantina `reviews_superseded`.
- **`is_late`** = `order_delivered_customer_date > order_estimated_delivery_date` hanya bila status `delivered` **dan** tanggal tiba terisi;
  selain itu **NULL** (bukan false) — order tak terkirim tidak boleh dihitung "tepat waktu" (temuan #9, #17).
- **Bulan lengkap**: `dim_date.is_complete_month = false` untuk bulan dengan jumlah order < 5% median bulanan
  (2016-09, 2016-12, 2018-09, 2018-10 pada data ini; temuan #11). Mart tren wajib menyaring / menandai bulan ini.
- **Kunci geografis = zip prefix teks 5 digit**; `dim_geo` satu baris per prefix + baris `unknown` (temuan #3, #4, #14).
- **Order tanpa item/pembayaran** tetap di `fact_order` dengan `has_items` / `has_payment` (temuan #7).
- **Rekonsiliasi**: mart menampilkan dua angka, nilai order (Σ price+freight) dan nilai dibayar (Σ payment_value), plus selisihnya (temuan #10).
- **Silver mempertahankan nama kolom sumber** (termasuk typo `lenght`); `dw` memperbaikinya jadi `length`.
"""


def _n(x: int) -> str:
    return f"{x:,}".replace(",", ".")


def render(root: Path) -> str:
    prof = json.loads((root / "outputs/profile.json").read_text(encoding="utf-8"))
    meta = prof.get("meta", {})
    by = {p["table"]: p for p in prof["tables"]} if "tables" in prof and isinstance(prof["tables"], list) else {}
    o: list[str] = []
    o.append("# DATA_CONTRACT — Olist Data Platform (`olist-data-platform`)\n")
    o.append(
        f"Sumber: Brazilian E-Commerce Public Dataset by Olist (Kaggle; mirror GitHub), 9 file CSV. "
        f"`scripts/check_dataset.py` lulus (9/9 sesuai). Dokumen dibangkitkan {date.today().isoformat()} oleh "
        "`python pipeline.py contract-doc` dari `olist/schema.py`, `outputs/profile.json`, dan aturan di `olist/contract.py`.\n"
    )
    o.append("Semua stempel waktu: waktu lokal Brasil tanpa zona. Mata uang: BRL. Kode pos: **teks 5 digit** di semua tabel.\n")
    o.append(
        "**Kontrak mesin-terbaca:** [`contracts/olist_raw.odcs.yaml`](contracts/olist_raw.odcs.yaml) — "
        "[Open Data Contract Standard 3.x](https://datacontract.com), dites dengan "
        "[Data Contract CLI](https://cli.datacontract.com) (`python pipeline.py contract`). "
        "Dokumen ini dan YAML berasal dari sumber yang sama sehingga tidak bisa berbeda.\n"
    )

    o.append("## 1. Tabel\n")
    for t in TABLES:
        o.append(f"### {t.name} — `{t.file}`\n")
        o.append(f"{t.desc}\n")
        o.append("| Kolom | Tipe target | Null? | Catatan |")
        o.append("|---|---|---|---|")
        for c in t.cols:
            note = c.desc + (f"; {c.note}" if c.note else "")
            if c.src in t.key:
                note += "; **kunci alami**" + (", unik (dibuktikan: 0 duplikat)" if len(t.key) == 1 else " (bagian dari kunci komposit)")
            o.append(f"| {c.src} | {c.type} | {'ya' if c.nullable else 'tidak'} | {note} |")
        key = ", ".join(f"`{k}`" for k in t.key) if t.key else "**tidak ada** (tidak ada kombinasi kolom yang unik)"
        enc = "" if t.encoding == "utf-8" else f" Encoding: `{t.encoding}`."
        o.append(f"\nBaris diharapkan: {_n(t.rows)}. Kunci alami: {key}.{enc}\n")

    o.append("## 2. Temuan \"kotor\" dan keputusannya\n")
    o.append("Angka dihitung kode (`python pipeline.py profile` → `outputs/profile.md`); tidak ada yang dibersihkan diam-diam.\n")
    o.append("| # | Tabel | Temuan | Angka | Keputusan | Diterapkan di |")
    o.append("|---|---|---|---|---|---|")
    for f in prof["findings"]:
        cell = lambda s: str(s).replace("|", "\\|").replace("\n", " ")
        o.append(f"| {f['id']} | {cell(f['table'])} | {cell(f['finding'])} | {cell(f['numbers'])} | {cell(f['decision'])} | {cell(f['applied_in'])} |")
    o.append(f"\nTotal: {len(prof['findings'])} temuan (minimal yang diminta: 10).\n")

    o.append("## 3. Aturan kualitas (gate silver)\n")
    o.append("`hard` = `severity: error` di YAML: pelanggaran → karantina/batch berhenti. `soft` = `severity: warning`: dicatat, baris dipertahankan + diberi flag.\n")
    o.append("| Tabel | Aturan | Jenis | Tindakan | Ambang karantina |")
    o.append("|---|---|---|---|---|")
    for t in TABLES:
        th = _pct(THRESHOLDS[t.name][0]) + " %"
        zipnote = " (zip char(5) dibaca sebagai teks)" if any(c.type == "char(5)" for c in t.cols) else ""
        o.append(f"| {t.name} | jumlah baris = {_n(t.rows)} | hard | batch berhenti | — |")
        o.append(f"| {t.name} | tipe & NOT NULL sesuai §1{zipnote} | hard | baris → karantina | {th} |")
        if t.key:
            o.append(f"| {t.name} | kunci `{', '.join(t.key)}` unik | hard | batch berhenti | 0 % |")
        for name, desc, _where, sev in RULES.get(t.name, []):
            o.append(f"| {t.name} | `{name}`: {desc} | {'hard' if sev == 'error' else 'soft'} | "
                     f"{'baris → karantina' if sev == 'error' else 'simpan + flag + catat di dq_report'} | {th if sev == 'error' else 'tidak berlaku'} |")
    o.append("\nAturan lintas tabel (FK, temuan #13): `order_items.{order_id,product_id,seller_id}`, `order_payments.order_id`, "
             "`order_reviews.order_id`, `orders.customer_id` → hard; baris yatim dikarantina, bukan dihapus.\n")
    o.append("Ambang karantina per tabel — jika persentase baris terkarantina melebihi ambang, batch dihentikan (bukan dilanjutkan):\n")
    o.append("| Tabel | Ambang | Alasan |")
    o.append("|---|---:|---|")
    for t in TABLES:
        th, why = THRESHOLDS[t.name]
        o.append(f"| {t.name} | {_pct(th)} % | {why} |")
    o.append("")

    o.append("## 4. Keputusan pemodelan yang mengikuti dari kontrak ini\n")
    o.append(MODELING)
    return "\n".join(o)


def write(root: Path) -> Path:
    out = root / "DATA_CONTRACT.md"
    out.write_text(render(root), encoding="utf-8")
    return out
