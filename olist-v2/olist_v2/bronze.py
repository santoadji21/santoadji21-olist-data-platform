"""Bronze: CSV sumber → MinIO apa adanya, dengan manifest append-only (ketentuan T1, dibawa dari T0–T6).

  * Bronze TIDAK mengubah isi (tanpa rename kolom, ganti encoding, buang baris).
  * Tata letak objek: olist/<tabel>/ingest_date=YYYY-MM-DD/sha256=<12 hex>/<file> (sha256 di kunci: isi berbeda
    tidak menimpa, termasuk di hari yang sama).
  * Manifest per tabel DI BUCKET (olist/_manifest/<tabel>.json): sumber, riwayat versi (sha256, md5, ukuran byte,
    jumlah baris, waktu ingest, kunci objek, ETag), dan `latest`. Ditulis TERAKHIR.
  * Checksum: sha256 menentukan `unchanged` (md5 lemah terhadap tabrakan); md5 disimpan juga dan dibandingkan dengan
    ETag S3 — hanya bermakna untuk upload tunggal (ETag multipart berbentuk "<hash>-<n>" dan tidak sama dengan md5).
  * Status: uploaded | unchanged | new_version.
"""
from __future__ import annotations

import csv
import hashlib
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .contract import REPO, Table, load_tables

BUCKET = "bronze"
PREFIX = "olist"
MANIFEST_VERSION = 1
CHUNK = 1024 * 1024


@dataclass(frozen=True)
class FileFacts:
    sha256: str
    md5: str
    size_bytes: int
    rows: int


def file_facts(path: Path, encoding: str = "utf-8-sig") -> FileFacts:
    """sha256 + md5 + ukuran dalam satu lintasan, dan jumlah baris data lewat parser CSV (bukan `wc -l`)."""
    s, m, size = hashlib.sha256(), hashlib.md5(usedforsecurity=False), 0
    with path.open("rb") as f:
        while chunk := f.read(CHUNK):
            s.update(chunk)
            m.update(chunk)
            size += len(chunk)
    with path.open("r", encoding=encoding, newline="") as f:
        records = sum(1 for _ in csv.reader(f))
    return FileFacts(s.hexdigest(), m.hexdigest(), size, max(records - 1, 0))


def object_key(table: str, filename: str, sha256: str, ingest_date: str) -> str:
    return f"{PREFIX}/{table}/ingest_date={ingest_date}/sha256={sha256[:12]}/{filename}"


def manifest_key(table: str) -> str:
    return f"{PREFIX}/_manifest/{table}.json"


def _is_missing(err: Exception) -> bool:
    code = (getattr(err, "response", None) or {}).get("Error", {}).get("Code", "")
    return code in ("404", "NoSuchKey", "NoSuchBucket", "NotFound")


def read_manifest(s3, bucket: str, table: str) -> dict | None:
    try:
        body = s3.get_object(Bucket=bucket, Key=manifest_key(table))["Body"].read()
    except Exception as e:
        if _is_missing(e):
            return None
        raise
    return json.loads(body)


def write_manifest(s3, bucket: str, manifest: dict) -> None:
    s3.put_object(Bucket=bucket, Key=manifest_key(manifest["table"]),
                  Body=json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8"), ContentType="application/json")


def head(s3, bucket: str, key: str) -> dict | None:
    try:
        return s3.head_object(Bucket=bucket, Key=key)
    except Exception as e:
        if _is_missing(e):
            return None
        raise


def ensure_bucket(s3, bucket: str) -> None:
    try:
        s3.head_bucket(Bucket=bucket)
    except Exception as e:
        if not _is_missing(e):
            raise
        s3.create_bucket(Bucket=bucket)


def ingest_table(s3, bucket: str, t: Table, raw_dir: Path, now: datetime | None = None) -> dict:
    """Satu tabel → satu hasil: status `uploaded` | `unchanged` | `new_version` (+ fakta file)."""
    t0 = time.time()
    facts = file_facts(raw_dir / t.file, t.encoding)
    when = now or datetime.now(timezone.utc)
    manifest = read_manifest(s3, bucket, t.name)
    latest = manifest["versions"][-1] if manifest and manifest["versions"] else None
    if latest and latest["sha256"] == facts.sha256:
        return _result(t, "unchanged", facts, latest, time.time() - t0)

    key = object_key(t.name, t.file, facts.sha256, when.strftime("%Y-%m-%d"))
    info = head(s3, bucket, key)   # ada bila run lalu mati sebelum manifest, atau isi kembali ke versi lama
    if info is None:
        s3.upload_file(str(raw_dir / t.file), bucket, key,
                       ExtraArgs={"Metadata": {"sha256": facts.sha256, "md5": facts.md5, "source": t.file, "rows": str(facts.rows)}})
        info = head(s3, bucket, key) or {}
    etag = str(info.get("ETag", "")).strip('"')
    entry = {
        "version": (latest["version"] + 1) if latest else 1, "object_key": key, "sha256": facts.sha256, "md5": facts.md5,
        "size_bytes": facts.size_bytes, "rows": facts.rows, "ingested_at": when.isoformat(timespec="seconds"),
        "etag": etag, "etag_matches_md5": (etag == facts.md5) if etag and "-" not in etag else None,  # None = multipart
    }
    manifest = manifest or {"manifest_version": MANIFEST_VERSION, "table": t.name, "source_name": t.file,
                            "bucket": bucket, "versions": []}
    manifest["versions"].append(entry)
    manifest["latest"] = {"version": entry["version"], "object_key": key}
    write_manifest(s3, bucket, manifest)   # TERAKHIR: objek ada dulu, baru diumumkan
    return _result(t, "uploaded" if entry["version"] == 1 else "new_version", facts, entry, time.time() - t0)


def _result(t: Table, status: str, f: FileFacts, entry: dict, secs: float) -> dict:
    return {"table": t.name, "file": t.file, "status": status, "version": entry["version"], "object_key": entry["object_key"],
            "rows": f.rows, "expected_rows": t.rows, "rows_match": f.rows == t.rows, "size_bytes": f.size_bytes,
            "sha256": f.sha256, "md5": f.md5, "ingested_at": entry["ingested_at"], "seconds": round(secs, 2)}


def run_ingest(s3, raw_dir: Path, tables: list[Table], bucket: str = BUCKET) -> dict:
    """Ingest semua tabel. Berhenti sebelum menyentuh bucket bila ada file sumber yang hilang."""
    missing = [t.file for t in tables if not (raw_dir / t.file).exists()]
    if missing:
        raise FileNotFoundError(f"file sumber tidak ada di {raw_dir}: {', '.join(missing)}")
    ensure_bucket(s3, bucket)
    t0 = time.time()
    files = [ingest_table(s3, bucket, t, raw_dir) for t in tables]
    return {"stage": "bronze", "bucket": bucket, "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "seconds": round(time.time() - t0, 2), "files": files}


def make_s3(env=os.environ):
    import boto3
    from botocore.config import Config

    return boto3.client("s3", endpoint_url=env.get("MINIO_ENDPOINT", "http://localhost:19000"),
                        aws_access_key_id=env["MINIO_ROOT_USER"], aws_secret_access_key=env["MINIO_ROOT_PASSWORD"],
                        region_name="us-east-1", config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}))


def report_md(run: dict) -> str:
    o = ["# Laporan Bronze\n", f"Bucket `{run['bucket']}` · dijalankan {run['ran_at']} · total {run['seconds']} s\n",
         "| Tabel | Status | Versi | Baris | Ukuran (byte) | Diingest | sha256 | md5 | Waktu (s) |",
         "|---|---|---:|---:|---:|---|---|---|---:|"]
    for r in run["files"]:
        flag = "" if r["rows_match"] else f" ⚠ kontrak {r['expected_rows']:,}"
        o.append(f"| {r['table']} | {r['status']} | {r['version']} | {r['rows']:,}{flag} | {r['size_bytes']:,} | {r['ingested_at']} | "
                 f"`{r['sha256'][:12]}` | `{r['md5'][:12]}` | {r['seconds']} |")
    counts: dict[str, int] = {}
    for r in run["files"]:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    o.append("\nRingkasan: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) + ".")
    o.append("\nManifest per tabel: `olist/_manifest/<tabel>.json` di bucket yang sama (JSON biasa; `latest` menunjuk objek terbaru).")
    return "\n".join(o) + "\n"


def write_outputs(outputs: Path, run: dict) -> Path:
    outputs.mkdir(parents=True, exist_ok=True)
    (outputs / "bronze.json").write_text(json.dumps(run, indent=2, ensure_ascii=False), encoding="utf-8")
    md = outputs / "bronze_report.md"
    md.write_text(report_md(run), encoding="utf-8")
    return md


def run_and_report(env=os.environ) -> dict:
    """Dipakai CLI dan task Airflow: ingest + laporan. Lokasi dibaca dari environment (default untuk jalan di host)."""
    raw_dir = Path(env.get("OLIST_RAW_DIR", REPO / "data" / "raw"))
    run = run_ingest(make_s3(env), raw_dir, load_tables())
    write_outputs(Path(env.get("OLIST_OUTPUTS", REPO / "outputs")), run)
    return run


def main() -> int:
    try:
        run = run_and_report()
    except FileNotFoundError as e:
        print(f"bronze berhenti: {e}", file=sys.stderr)
        return 1
    for r in run["files"]:
        print(f"  {r['table']:30s} {r['status']:12s} v{r['version']}  {r['rows']:>9,} baris  {r['seconds']:>5} s")
    print(f"selesai {run['seconds']} s → outputs/bronze_report.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
