#!/usr/bin/env python3
"""Ambil 9 CSV Olist ke data/raw.

Urutan yang dicoba:
  1. Kaggle CLI (`kaggle datasets download -d olistbr/brazilian-ecommerce`) — butuh ~/.kaggle/kaggle.json
  2. Mirror GitHub yang berisi file yang sama persis (jumlah baris & md5 sama dengan Kaggle v2):
     https://github.com/dineshmanubolu/Resources_Datasets_Brazilin_Ecommerce_Public_Dataset_by_olist
Setelah selesai, jalankan `python pipeline.py check`.
"""
import shutil
import subprocess
import sys
import tempfile
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
MIRROR = "https://github.com/dineshmanubolu/Resources_Datasets_Brazilin_Ecommerce_Public_Dataset_by_olist.git"
FILES = ["olist_customers_dataset.csv", "olist_geolocation_dataset.csv", "olist_order_items_dataset.csv",
         "olist_order_payments_dataset.csv", "olist_order_reviews_dataset.csv", "olist_orders_dataset.csv",
         "olist_products_dataset.csv", "olist_sellers_dataset.csv", "product_category_name_translation.csv"]


def have_all() -> bool:
    return all((RAW / f).exists() for f in FILES)


def via_kaggle(tmp: Path) -> bool:
    if not shutil.which("kaggle"):
        return False
    r = subprocess.run(["kaggle", "datasets", "download", "-d", "olistbr/brazilian-ecommerce", "-p", str(tmp)], capture_output=True, text=True)
    if r.returncode != 0:
        print("  kaggle gagal:", r.stderr.strip()[:200])
        return False
    for z in tmp.glob("*.zip"):
        with zipfile.ZipFile(z) as zf:
            zf.extractall(tmp)
    return True


def via_mirror(tmp: Path) -> bool:
    r = subprocess.run(["git", "clone", "--depth", "1", MIRROR, str(tmp / "mirror")], capture_output=True, text=True)
    if r.returncode != 0:
        print("  git clone gagal:", r.stderr.strip()[:200])
        return False
    for f in (tmp / "mirror").glob("*.csv"):
        shutil.move(str(f), tmp / f.name)
    return True


def main() -> int:
    RAW.mkdir(parents=True, exist_ok=True)
    if have_all():
        print(f"sudah lengkap di {RAW}")
        return 0
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        ok = False
        for name, fn in [("Kaggle CLI", via_kaggle), ("mirror GitHub", via_mirror)]:
            print(f"mencoba {name}…")
            if fn(tmp):
                ok = True
                break
        if not ok:
            print("tidak bisa mengunduh otomatis — unduh manual dari Kaggle dan ekstrak ke", RAW)
            return 1
        for f in FILES:
            src = next(tmp.rglob(f), None)
            if src:
                shutil.copy(src, RAW / f)
                print("  ", f)
    print("selesai →", RAW, "| lanjutkan: python pipeline.py check")
    return 0 if have_all() else 1


if __name__ == "__main__":
    sys.exit(main())
