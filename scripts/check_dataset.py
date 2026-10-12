#!/usr/bin/env python3
"""Cek dataset Olist di data/raw: 9 file ada, header sesuai, jumlah baris sama dengan versi yang dipakai penilai.

    python scripts/check_dataset.py            # folder default data/raw
    python scripts/check_dataset.py /path/ke/folder

Tidak butuh library apa pun selain Python. Jumlah baris dihitung dengan parser CSV (bukan menghitung '\n'),
karena komentar review mengandung baris baru di dalam tanda kutip.
"""
import csv
import hashlib
import sys
from pathlib import Path

EXPECTED = {
    "olist_customers_dataset.csv": (99_441, ["customer_id", "customer_unique_id", "customer_zip_code_prefix", "customer_city", "customer_state"]),
    "olist_geolocation_dataset.csv": (1_000_163, ["geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng", "geolocation_city", "geolocation_state"]),
    "olist_order_items_dataset.csv": (112_650, ["order_id", "order_item_id", "product_id", "seller_id", "shipping_limit_date", "price", "freight_value"]),
    "olist_order_payments_dataset.csv": (103_886, ["order_id", "payment_sequential", "payment_type", "payment_installments", "payment_value"]),
    "olist_order_reviews_dataset.csv": (99_224, ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message", "review_creation_date", "review_answer_timestamp"]),
    "olist_orders_dataset.csv": (99_441, ["order_id", "customer_id", "order_status", "order_purchase_timestamp", "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date", "order_estimated_delivery_date"]),
    "olist_products_dataset.csv": (32_951, ["product_id", "product_category_name", "product_name_lenght", "product_description_lenght", "product_photos_qty", "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]),
    "olist_sellers_dataset.csv": (3_095, ["seller_id", "seller_zip_code_prefix", "seller_city", "seller_state"]),
    "product_category_name_translation.csv": (71, ["product_category_name", "product_category_name_english"]),
}


def main(folder: Path) -> int:
    csv.field_size_limit(1 << 30)
    ok = True
    print(f"folder: {folder}\n")
    print(f"{'file':42s} {'baris':>10s} {'harap':>10s}  status")
    for name, (exp_rows, exp_cols) in EXPECTED.items():
        p = folder / name
        if not p.exists():
            print(f"{name:42s} {'-':>10s} {exp_rows:>10,}  HILANG")
            ok = False
            continue
        with open(p, encoding="utf-8-sig", newline="") as f:   # utf-8-sig: salah satu file diawali BOM
            r = csv.reader(f)
            header = next(r)
            n = sum(1 for _ in r)
        md5 = hashlib.md5(p.read_bytes()).hexdigest()[:8]
        status = []
        if header != exp_cols:
            status.append(f"KOLOM BEDA: {header}")
        if n != exp_rows:
            status.append(f"BARIS BEDA ({n - exp_rows:+d})")
        ok = ok and not status
        print(f"{name:42s} {n:>10,} {exp_rows:>10,}  {'OK' if not status else '; '.join(status)}  md5={md5}")
    print("\n" + ("SEMUA SESUAI — lanjut ke Tahap 0." if ok else "ADA YANG TIDAK SESUAI — unduh ulang dari Kaggle sebelum lanjut."))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main(Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent / "data" / "raw"))
