"""Unduh dataset Olist dari Kaggle ke data/raw (di dalam olist-v2; tidak mengambil apa pun dari repo lain).

  * Idempoten: bila 9 file sudah ada dan jumlah barisnya cocok kontrak, tidak mengunduh ulang (status `present`).
  * Aman: ekstraksi hanya untuk nama file yang dikenal (tanpa path), ke folder sementara, lalu dipindah — unduhan
    yang gagal tidak meninggalkan data/raw setengah terisi.
  * Terverifikasi: jumlah baris tiap file dibandingkan dengan kontrak ODCS (parser CSV). Versi dataset yang berbeda
    menghentikan pipeline sebelum bronze (tugas: "jangan lanjut dengan data yang berbeda").
  * Kredensial Kaggle TIDAK pernah ada di repo: `.kaggle/kaggle.json` (di-ignore) di-mount read-only ke container.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from .bronze import file_facts
from .contract import REPO, Table, load_tables

DATASET = "olistbr/brazilian-ecommerce"


class DatasetError(RuntimeError):
    pass


def check_credentials(env=os.environ) -> Path:
    cfg = Path(env.get("KAGGLE_CONFIG_DIR", Path.home() / ".kaggle"))
    key = cfg / "kaggle.json"
    if not key.exists() and not env.get("KAGGLE_API_TOKEN") and not (env.get("KAGGLE_USERNAME") and env.get("KAGGLE_KEY")):
        raise DatasetError(
            f"Kredensial Kaggle tidak ditemukan ({key}). Taruh kaggle.json di olist-v2/.kaggle/ "
            "(salin dari ~/.kaggle/kaggle.json; chmod 600) lalu ulangi. Berkas itu di-ignore git dan tidak boleh ter-commit.")
    return key


def kaggle_fetch(dest: Path, env=os.environ) -> None:
    """Unduh ZIP dataset ke `dest` memakai Kaggle CLI (venv terpisah di image Airflow)."""
    check_credentials(env)
    exe = env.get("KAGGLE_BIN", "kaggle")
    r = subprocess.run([exe, "datasets", "download", "-d", DATASET, "-p", str(dest)], capture_output=True, text=True)
    if r.returncode != 0:
        raise DatasetError(f"kaggle datasets download gagal (exit {r.returncode}): {(r.stderr or r.stdout).strip()[:400]}")


def extract_known(zip_path: Path, dest: Path, expected: set[str]) -> list[str]:
    """Ekstrak HANYA file bernama dikenal (basename), menolak path berbahaya (zip-slip) dan file tak dikenal."""
    got = []
    with zipfile.ZipFile(zip_path) as z:
        for info in z.infolist():
            name = Path(info.filename).name
            if info.is_dir() or name not in expected or name != info.filename:
                continue
            with z.open(info) as src, (dest / name).open("wb") as out:
                shutil.copyfileobj(src, out)
            got.append(name)
    return got


def verify(raw_dir: Path, tables: list[Table]) -> list[dict]:
    rows, problems = [], []
    for t in tables:
        p = raw_dir / t.file
        if not p.exists():
            problems.append(f"{t.file}: hilang")
            continue
        f = file_facts(p, t.encoding)
        rows.append({"table": t.name, "file": t.file, "rows": f.rows, "expected_rows": t.rows, "size_bytes": f.size_bytes, "sha256": f.sha256})
        if f.rows != t.rows:
            problems.append(f"{t.file}: {f.rows:,} baris, kontrak {t.rows:,}")
    if problems:
        raise DatasetError("dataset tidak sesuai kontrak (versi berbeda?): " + "; ".join(problems))
    return rows


def download_dataset(raw_dir: Path, tables: list[Table], fetch: Callable[[Path], None] = kaggle_fetch, force: bool = False) -> dict:
    expected = {t.file for t in tables}
    raw_dir.mkdir(parents=True, exist_ok=True)
    if not force and all((raw_dir / f).exists() for f in expected):
        return {"status": "present", "files": verify(raw_dir, tables), "source": DATASET}
    # folder sementara DI DALAM raw_dir: induknya (/opt/airflow/data) milik root di image dan tidak bisa ditulis user airflow
    with tempfile.TemporaryDirectory(dir=raw_dir, prefix=".dl-") as tmp:
        tmp = Path(tmp)
        fetch(tmp)
        zips = list(tmp.glob("*.zip"))
        if not zips:
            raise DatasetError("Kaggle tidak menghasilkan berkas ZIP")
        stage = tmp / "stage"
        stage.mkdir()
        got = extract_known(zips[0], stage, expected)
        missing = sorted(expected - set(got))
        if missing:
            raise DatasetError(f"ZIP tidak memuat file yang dibutuhkan: {', '.join(missing)}")
        verify(stage, tables)                       # periksa SEBELUM memindahkan ke data/raw
        for name in got:
            os.replace(stage / name, raw_dir / name)
    return {"status": "downloaded", "files": verify(raw_dir, tables), "source": DATASET}


def write_outputs(outputs: Path, result: dict) -> None:
    outputs.mkdir(parents=True, exist_ok=True)
    (outputs / "download.json").write_text(json.dumps(
        {"stage": "download", "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"), **result}, indent=2), encoding="utf-8")


def run_and_report(env=os.environ, force: bool = False) -> dict:
    raw_dir = Path(env.get("OLIST_RAW_DIR", REPO / "data" / "raw"))
    result = download_dataset(raw_dir, load_tables(), force=force)
    write_outputs(Path(env.get("OLIST_OUTPUTS", REPO / "outputs")), result)
    return result


def main() -> int:
    try:
        r = run_and_report(force="--force" in sys.argv)
    except DatasetError as e:
        print(f"download berhenti: {e}", file=sys.stderr)
        return 1
    print(f"download: {r['status']} — {len(r['files'])} file cocok kontrak (sumber {r['source']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
