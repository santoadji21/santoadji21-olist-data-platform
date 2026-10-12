"""Profiling otomatis dataset Olist (T0) → outputs/profile.md + outputs/profile.json.

Semua angka di DATA_CONTRACT.md berasal dari sini. Dijalankan lewat `python pipeline.py profile`.
Prinsip: semua kolom dibaca sebagai TEKS (dtype=str); konversi tipe adalah keputusan kontrak, bukan default library.
"""
from __future__ import annotations

import csv
import json
from datetime import datetime
from pathlib import Path

import pandas as pd

from olist.schema import BRAZIL_LAT, BRAZIL_LNG, TABLES, TIME_TYPES, Table

csv.field_size_limit(1 << 30)


# ───────────────────────────── loading ─────────────────────────────
def load_all(raw: Path) -> dict[str, pd.DataFrame]:
    return {
        t.name: pd.read_csv(raw / t.file, dtype=str, encoding=t.encoding, keep_default_na=True)
        for t in TABLES
    }


def lpad_zip(s: pd.Series) -> pd.Series:
    return s.str.strip().str.zfill(5)


# ───────────────────────────── per-table profile ─────────────────────────────
def profile_table(t: Table, df: pd.DataFrame) -> dict:
    cols = []
    for c in t.cols:
        s = df[c.src]
        nulls = int(s.isna().sum())
        info = {
            "col": c.src, "type": c.type, "nullable_in_contract": c.nullable,
            "nulls": nulls, "null_pct": round(100 * nulls / len(df), 2),
            "distinct": int(s.nunique(dropna=True)),
            "min": None, "max": None, "unparseable": 0,
        }
        nn = s.dropna()
        if c.type in TIME_TYPES:
            dt = pd.to_datetime(nn, errors="coerce", format="%Y-%m-%d %H:%M:%S")
            info["unparseable"] = int(dt.isna().sum())
            if dt.notna().any():
                info["min"], info["max"] = str(dt.min()), str(dt.max())
        elif c.type.startswith(("integer", "smallint", "numeric", "double")):
            num = pd.to_numeric(nn, errors="coerce")
            info["unparseable"] = int(num.isna().sum())
            if num.notna().any():
                info["min"], info["max"] = float(num.min()), float(num.max())
        elif c.type.startswith("char"):
            ln = nn.str.len()
            info["min"], info["max"] = f"len={int(ln.min())}", f"len={int(ln.max())}"
        cols.append(info)

    key = list(t.key)
    key_proof = None
    if key:
        dups = int(df.duplicated(subset=key).sum())
        key_proof = {"key": key, "duplicate_rows": dups, "unique": dups == 0}
    return {
        "table": t.name, "file": t.file, "rows": len(df), "rows_expected": t.rows,
        "rows_ok": len(df) == t.rows,
        "columns": cols, "key": key_proof,
        "exact_duplicate_rows": int(df.duplicated().sum()),
    }


# ───────────────────────────── findings ─────────────────────────────
def _pct(n: int, d: int) -> str:
    return f"{100 * n / d:.2f}%"


def compute_findings(d: dict[str, pd.DataFrame], raw: Path) -> list[dict]:
    c, o, it = d["customers"], d["orders"], d["order_items"]
    pay, rv, g = d["order_payments"], d["order_reviews"], d["geolocation"]
    p, s, tr = d["products"], d["sellers"], d["product_category_translation"]
    F: list[dict] = []

    def add(table, title, numbers, decision, applied, severity):
        F.append({
            "id": len(F) + 1, "table": table, "finding": title, "numbers": numbers,
            "decision": decision, "applied_in": applied, "severity": severity,
        })

    # 1. dua id pelanggan
    n_cid, n_uid = c.customer_id.nunique(), c.customer_unique_id.nunique()
    per_person = c.groupby("customer_unique_id").customer_id.nunique()
    repeaters = int((per_person > 1).sum())
    add("customers",
        "Dua id pelanggan: `customer_id` per-ORDER, `customer_unique_id` = ORANG",
        f"customer_id unik={n_cid:,}; customer_unique_id unik={n_uid:,}; selisih={n_cid - n_uid:,} "
        f"({_pct(n_cid - n_uid, n_cid)} jika salah pilih); orang belanja >1 kali={repeaters:,}",
        "Grain `dim_customer` = `customer_unique_id`; join orders→customers tetap lewat `customer_id`; "
        "atribut yang berubah antar order (zip/kota) ambil yang terbaru (SCD tipe 1), hilangnya riwayat dicatat",
        "T4 `dim_customer`", "silent-wrong")

    # 2. review_id & order_id tidak unik
    dup_rid = len(rv) - rv.review_id.nunique()
    multi = rv.groupby("order_id").size()
    pair_dups = int(rv.duplicated(subset=["review_id", "order_id"]).sum())
    orders_multi = int((multi > 1).sum())
    add("order_reviews",
        "`review_id` tidak unik dan satu `order_id` bisa punya >1 ulasan",
        f"baris dengan review_id berulang={dup_rid:,}; review_id unik={rv.review_id.nunique():,}; "
        f"order dengan >1 ulasan={orders_multi:,}; order unik={rv.order_id.nunique():,}; "
        f"pasangan (review_id, order_id) berulang={pair_dups:,}",
        "`fact_review` satu baris per order: pilih ulasan dengan `review_answer_timestamp` terbaru "
        "(seri → `review_creation_date` terbaru → `review_id`); simpan `review_count` asli",
        "T4 `fact_review`", "silent-wrong")

    # 3. kode pos: aman sebagai teks, tapi rusak bila dibaca sebagai angka (tiga tabel)
    zparts, zn, lens = [], 0, set()
    for name, df, col in (("customers", c, "customer_zip_code_prefix"),
                          ("sellers", s, "seller_zip_code_prefix"),
                          ("geolocation", g, "geolocation_zip_code_prefix")):
        lead0 = int(df[col].str.startswith("0").sum())
        zn += lead0
        lens |= {int(x) for x in df[col].str.len().unique()}
        zparts.append(f"{name}={lead0:,} dari {len(df):,} ({_pct(lead0, len(df))})")
    add("customers, sellers, geolocation",
        "Kode pos prefix berawalan `0` (`'01003'`) — kehilangan nol di depan bila dibaca sebagai angka",
        "baris berawalan '0' (akan jadi 4 digit jika di-cast ke integer): " + "; ".join(zparts)
        + f"; panjang string di file sumber={sorted(lens)}",
        "Baca sebagai teks (`dtype=str`); tipe target `char(5)`; `lpad(…, 5, '0')` sebagai pengaman di silver; "
        "join zip hanya antar kolom bertipe teks 5 digit", "T2 silver (typing), T4 `dim_geo`", "silent-wrong")

    # 4. geolocation duplikat & banyak baris per prefix
    exact = int(g.duplicated().sum())
    per_zip = g.groupby("geolocation_zip_code_prefix").size()
    add("geolocation",
        "Satu zip prefix punya banyak baris + duplikat persis (bukan kunci)",
        f"baris={len(g):,}; prefix unik={per_zip.size:,}; maks baris/prefix={int(per_zip.max()):,}; "
        f"duplikat persis={exact:,} ({_pct(exact, len(g))})",
        "Agregasi ke satu baris per prefix: `avg(lat/lng)`, `mode(city/state)`; "
        "baris mentah dipertahankan di bronze/silver", "T4 `dim_geo`", "silent-wrong")

    # 5. koordinat di luar Brasil
    lat = pd.to_numeric(g.geolocation_lat, errors="coerce")
    lng = pd.to_numeric(g.geolocation_lng, errors="coerce")
    out = ~(lat.between(*BRAZIL_LAT) & lng.between(*BRAZIL_LNG))
    add("geolocation",
        f"Koordinat di luar Brasil (lat −34…+5, lng −74…−35)",
        f"titik di luar Brasil={int(out.sum()):,} ({_pct(int(out.sum()), len(g))}); "
        f"lat [{lat.min():.2f}, {lat.max():.2f}]; lng [{lng.min():.2f}, {lng.max():.2f}]",
        "Soft rule: baris tetap di silver dengan flag `is_outside_brazil` (tidak menggagalkan batch, tidak dibuang); dikecualikan dari agregasi `dim_geo`",
        "T2 gate kualitas, T4 `dim_geo`", "keep-and-flag")

    # 6. BOM di file terjemahan
    with open(raw / "product_category_name_translation.csv", "rb") as fh:
        head = fh.read(3)
    with open(raw / "product_category_name_translation.csv", encoding="utf-8", newline="") as fh:
        first_col_utf8 = next(csv.reader(fh))[0]
    add("product_category_translation",
        "File diawali BOM UTF-8 → nama kolom pertama tak terlihat berbeda, join kategori diam-diam gagal",
        f"3 byte pertama={head.hex()}; repr kolom pertama (utf-8)={first_col_utf8!r}; "
        f"repr dengan utf-8-sig={'product_category_name'!r}",
        "Baca file ini dengan `encoding='utf-8-sig'`; kontrak mencatat encoding per file",
        "T1 bronze (checksum apa adanya), T2 silver (decode)", "silent-wrong")

    # 7. order tanpa item / tanpa pembayaran
    no_item = int((~o.order_id.isin(it.order_id)).sum())
    no_pay = int((~o.order_id.isin(pay.order_id)).sum())
    st_noitem = o.loc[~o.order_id.isin(it.order_id), "order_status"].value_counts().to_dict()
    add("orders",
        "Order tanpa item dan order tanpa pembayaran",
        f"order tanpa item={no_item:,} (status: {st_noitem}); order tanpa pembayaran={no_pay:,}",
        "Tetap masuk `fact_order` (99.441 baris) dengan flag `has_items` / `has_payment`; tidak dibuang",
        "T4 `fact_order`", "silent-wrong")

    # 8. pembayaran bernilai nol / not_defined / cicilan 0
    pv = pd.to_numeric(pay.payment_value, errors="coerce")
    pi = pd.to_numeric(pay.payment_installments, errors="coerce")
    add("order_payments",
        "Pembayaran bernilai 0, `payment_type='not_defined'`, dan cicilan 0",
        f"payment_value=0: {int((pv == 0).sum()):,}; payment_type not_defined: "
        f"{int((pay.payment_type == 'not_defined').sum()):,}; payment_installments=0: {int((pi == 0).sum()):,}",
        "Biarkan & catat di silver; flag `is_zero_payment` di `fact_payment`; "
        "tidak dihitung sebagai pendapatan terbayar", "T2 soft rule, T4 `fact_payment`", "keep-and-flag")

    # 9. anomali status/tanggal pengiriman
    dc = pd.to_datetime(o.order_delivered_customer_date, errors="coerce")
    dk = pd.to_datetime(o.order_delivered_carrier_date, errors="coerce")
    ap = pd.to_datetime(o.order_approved_at, errors="coerce")
    pu = pd.to_datetime(o.order_purchase_timestamp, errors="coerce")
    est = pd.to_datetime(o.order_estimated_delivery_date, errors="coerce")
    deliv = o.order_status == "delivered"
    add("orders",
        "Status `delivered` tanpa tanggal terkirim; tanggal terkirim lebih awal dari tanggal diserahkan ke kurir",
        f"delivered tanpa order_delivered_customer_date={int((deliv & dc.isna()).sum()):,}; "
        f"delivered tanpa order_delivered_carrier_date={int((deliv & dk.isna()).sum()):,}; "
        f"delivered_customer < delivered_carrier={int((dc < dk).sum()):,}; "
        f"approved_at < purchase={int((ap < pu).sum()):,}",
        "Soft rule (simpan + flag `has_time_anomaly`, tidak dibuang; tanggal kosong maupun urutan terbalik); "
        "`is_late` = NULL (bukan false) bila tanggal terkirim kosong",
        "T2 gate kualitas, T4 `is_late`", "keep-and-flag")

    # 10. rekonsiliasi item+freight vs payment
    it2 = it.assign(v=pd.to_numeric(it.price, errors="coerce") + pd.to_numeric(it.freight_value, errors="coerce"))
    items_sum = it2.groupby("order_id").v.sum()
    pay_sum = pay.assign(v=pv).groupby("order_id").v.sum()
    both = pd.concat([items_sum.rename("items"), pay_sum.rename("pay")], axis=1, join="inner")
    both = both.round(2)  # uang dalam sen: float mentah membuat selisih 0,01 tampak > 0,01 (derau pecahan)
    diff = (both["items"] - both["pay"]).abs().round(2)
    voucher_orders = set(pay.loc[pay.payment_type == "voucher", "order_id"])
    mism = diff[diff > 0.01]
    signed = (both["pay"] - both["items"])[diff > 0.01]
    cc_multi = set(pay.loc[(pay.payment_type == "credit_card") & (pi > 1), "order_id"])
    over = signed[signed > 0]
    add("order_items vs order_payments",
        "Σ(price+freight) item ≠ Σ(payment_value) — penyebabnya lebih dari satu",
        f"order dgn item & bayar={len(both):,}; selisih >0,01={len(mism):,}; selisih >1,00={int((diff > 1).sum()):,}; "
        f"dari yang >0,01: dibayar LEBIH dari nilai item={len(over):,} (kartu kredit cicilan >1 di "
        f"{int(over.index.isin(cc_multi).sum()):,} di antaranya), dibayar KURANG={len(signed) - len(over):,}, "
        f"memakai voucher={int(mism.index.isin(voucher_orders).sum()):,}; "
        f"total selisih absolut={diff.sum():,.2f} BRL",
        "Jangan dipaksa sama: mart pendapatan menampilkan dua angka (nilai pesanan vs terbayar) + "
        "tabel rekonsiliasi yang menjelaskan selisih per sebab", "T5 mart rekonsiliasi", "keep-and-flag")

    # 11. kelengkapan bulan
    m = pu.dt.to_period("M").value_counts().sort_index()
    sparse = {str(k): int(v) for k, v in m.items() if v < 0.10 * m.median()}
    add("orders",
        "Bulan pertama dan terakhir dataset hanya berisi segelintir order",
        f"rentang purchase={pu.min()} … {pu.max()}; "
        f"bulan pertama {m.index[0]}={int(m.iloc[0]):,}; kedua {m.index[1]}={int(m.iloc[1]):,}; "
        f"bulan terakhir {m.index[-1]}={int(m.iloc[-1]):,}; median per bulan={int(m.median()):,}; "
        f"bulan <10% median={sparse}",
        "Jangan dibuang; `dim_date.is_complete_month` = false untuk bulan tak lengkap agar mart/dashboard bisa memilih",
        "T4 `dim_date`, T5 dashboard", "keep-and-flag")

    # 12. produk: kategori null, tanpa terjemahan, typo kolom
    cat_null = int(p.product_category_name.isna().sum())
    known = set(tr.product_category_name)
    has_cat = p.dropna(subset=["product_category_name"])
    no_tr = has_cat[~has_cat.product_category_name.isin(known)]
    add("products",
        "Produk tanpa kategori, kategori tanpa terjemahan, dan typo nama kolom (`lenght`)",
        f"kategori null={cat_null:,}; produk dgn kategori tak terjemahkan={len(no_tr):,} "
        f"(kategori: {sorted(no_tr.product_category_name.unique())}); "
        f"dimensi null={int(p.product_weight_g.isna().sum())}; "
        "kolom typo: product_name_lenght, product_description_lenght",
        "Kategori Inggris dengan fallback (`'unknown'` untuk null, nama Portugis untuk tanpa terjemahan); "
        "typo diperbaiki hanya di dw (`*_length`), silver tetap nama sumber",
        "T4 `dim_product`", "keep-and-flag")

    # 13. integritas referensial (FK)
    def orphan(child, ccol, parent, pcol):
        return int((~child[ccol].isin(parent[pcol])).sum())
    fk = {
        "order_items.order_id→orders": orphan(it, "order_id", o, "order_id"),
        "order_items.product_id→products": orphan(it, "product_id", p, "product_id"),
        "order_items.seller_id→sellers": orphan(it, "seller_id", s, "seller_id"),
        "order_payments.order_id→orders": orphan(pay, "order_id", o, "order_id"),
        "order_reviews.order_id→orders": orphan(rv, "order_id", o, "order_id"),
        "orders.customer_id→customers": orphan(o, "customer_id", c, "customer_id"),
    }
    add("semua",
        "Integritas referensial antar tabel (yatim/orphan)",
        "; ".join(f"{k}={v:,}" for k, v in fk.items()),
        "Semua FK sumber dinyatakan HARD di silver; bila >0 baris yatim → karantina, bukan dihapus diam-diam",
        "T2 gate kualitas", "hard-rule")

    # 14. zip tak ada di geolocation (setelah lpad)
    gz = set(lpad_zip(g.geolocation_zip_code_prefix))
    c_missing = int((~lpad_zip(c.customer_zip_code_prefix).isin(gz)).sum())
    s_missing = int((~lpad_zip(s.seller_zip_code_prefix).isin(gz)).sum())
    add("customers, sellers → geolocation",
        "Kode pos pelanggan/seller yang tidak ada di tabel geolocation",
        f"customers={c_missing:,} baris; sellers={s_missing:,} baris (setelah lpad 5 digit)",
        "Jangan hapus FK: sediakan baris `unknown` di `dim_geo`; FK ke `dim_geo` tetap valid 100%",
        "T4 `dim_geo`", "keep-and-flag")

    # 15. anomali ulasan
    sc = pd.to_numeric(rv.review_score, errors="coerce")
    ca = pd.to_datetime(rv.review_creation_date, errors="coerce")
    an = pd.to_datetime(rv.review_answer_timestamp, errors="coerce")
    add("order_reviews",
        "Skor di luar 1–5 dan jawaban lebih awal dari pembuatan survei; komentar kosong dominan",
        f"skor di luar 1–5={int((~sc.between(1, 5)).sum()):,}; answer<creation={int((an < ca).sum()):,}; "
        f"judul kosong={_pct(int(rv.review_comment_title.isna().sum()), len(rv))}; "
        f"pesan kosong={_pct(int(rv.review_comment_message.isna().sum()), len(rv))}",
        "`review_score` CHECK 1..5 (hard); urutan waktu = soft; komentar kosong ≠ error (NULL, bukan '')",
        "T2 gate kualitas", "keep-and-flag")

    # 16. harga/ongkir non-positif & dimensi produk nol
    pr = pd.to_numeric(it.price, errors="coerce")
    fr = pd.to_numeric(it.freight_value, errors="coerce")
    wg = pd.to_numeric(p.product_weight_g, errors="coerce")
    add("order_items, products",
        "Harga ≤ 0, ongkir 0, dan berat produk 0",
        f"price<=0={int((pr <= 0).sum()):,}; freight_value=0={int((fr == 0).sum()):,}; "
        f"freight_value<0={int((fr < 0).sum()):,}; product_weight_g=0={int((wg == 0).sum()):,}",
        "price/freight ≥ 0 sebagai hard rule; ongkir 0 sah (gratis ongkir); berat 0 → NULL di `dim_product`",
        "T2 gate kualitas, T4 `dim_product`", "keep-and-flag")

    # 17. order batal/unavailable yang punya tanggal terkirim
    bad = o.order_status.isin(["canceled", "unavailable"]) & dc.notna()
    add("orders",
        "Status canceled/unavailable tetapi punya tanggal tiba di pelanggan",
        f"canceled/unavailable dengan delivered_customer_date={int(bad.sum()):,}; "
        f"sebaran status={o.order_status.value_counts().to_dict()}",
        "Biarkan & catat; `is_late` hanya dihitung untuk status `delivered`", "T4 `is_late`", "keep-and-flag")

    # 18. ejaan kota tak seragam
    bad_city = s.seller_city.str.contains(r"[^a-zA-ZÀ-ÿ\s'\-]", regex=True, na=False)
    multi_city = g.groupby("geolocation_zip_code_prefix").geolocation_city.nunique()
    non_ascii = int(g.geolocation_city.map(lambda s: not s.isascii()).sum())
    add("sellers, geolocation",
        "Nama kota tidak seragam: satu zip prefix punya beberapa ejaan, karakter non-ASCII/simbol",
        f"seller_city berisi karakter non-huruf={int(bad_city.sum()):,} dari {len(s):,}; "
        f"prefix geolocation dengan >1 ejaan kota={int((multi_city > 1).sum()):,} dari {multi_city.size:,}; "
        f"baris geolocation_city non-ASCII={non_ascii:,} dari {len(g):,}",
        "Kota bukan kunci join (kunci geografis = zip prefix). Di `dim_geo` kota = modus per prefix "
        "(trim + lower); ejaan lain dibiarkan di bronze/silver", "T4 `dim_geo`", "keep-and-flag")

    return F


# ───────────────────────────── markdown ─────────────────────────────
def render_md(profiles: list[dict], findings: list[dict], meta: dict) -> str:
    L = ["# Profil Dataset Olist (T0)", "",
         f"Dibangkitkan otomatis oleh `python pipeline.py profile` — {meta['generated_at']}.",
         "Semua kolom dibaca sebagai teks (`dtype=str`); angka di bawah adalah temuan, bukan konversi.", "",
         "## 1. Ringkasan tabel", "",
         "| Tabel | File | Baris | Diharapkan | OK | Kunci alami | Duplikat kunci | Duplikat persis |",
         "|---|---|---:|---:|:-:|---|---:|---:|"]
    for p in profiles:
        k = p["key"]
        L.append(f"| {p['table']} | `{p['file']}` | {p['rows']:,} | {p['rows_expected']:,} | "
                 f"{'✅' if p['rows_ok'] else '❌'} | "
                 f"{', '.join(k['key']) if k else '— (tidak ada)'} | "
                 f"{(format(k['duplicate_rows'], ',') if k else 'n/a')} | {p['exact_duplicate_rows']:,} |")
    L += ["", "## 2. Profil kolom", ""]
    for p in profiles:
        L += [f"### {p['table']}", "",
              "| Kolom | Tipe target | Null | Null % | Distinct | Min | Max | Tak terparse |",
              "|---|---|---:|---:|---:|---|---|---:|"]
        for c in p["columns"]:
            L.append(f"| {c['col']} | {c['type']} | {c['nulls']:,} | {c['null_pct']} | {c['distinct']:,} | "
                     f"{c['min'] if c['min'] is not None else ''} | {c['max'] if c['max'] is not None else ''} | "
                     f"{c['unparseable']:,} |")
        L.append("")
    L += ["## 3. Temuan \"kotor\" (dengan angka)", "",
          "| # | Tabel | Temuan | Angka | Jenis |", "|---:|---|---|---|---|"]
    for f in findings:
        L.append(f"| {f['id']} | {f['table']} | {f['finding']} | {f['numbers']} | {f['severity']} |")
    L.append("")
    return "\n".join(L)


# ───────────────────────────── entry point ─────────────────────────────
def run(root: Path, raw_dir: str = "data/raw") -> int:
    raw = root / raw_dir
    out = root / "outputs"
    out.mkdir(exist_ok=True)
    d = load_all(raw)
    profiles = [profile_table(t, d[t.name]) for t in TABLES]
    findings = compute_findings(d, raw)
    meta = {"generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "raw_dir": raw_dir}
    (out / "profile.json").write_text(
        json.dumps({"meta": meta, "tables": profiles, "findings": findings}, indent=2, ensure_ascii=False,
                   default=str), encoding="utf-8")
    (out / "profile.md").write_text(render_md(profiles, findings, meta), encoding="utf-8")
    bad = [p["table"] for p in profiles if not p["rows_ok"]]
    print(f"profil → {out/'profile.md'} ({len(profiles)} tabel, {len(findings)} temuan)")
    if bad:
        print("BARIS TIDAK SESUAI:", bad)
        return 1
    return 0
