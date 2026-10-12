"""T1 — Bronze: CSV sumber → MinIO apa adanya, dengan manifest append-only.

Aturan:
  * Bronze TIDAK mengubah isi (tanpa rename kolom, ganti encoding, buang baris).
  * Idempotensi = bandingkan sha256 isi dengan versi terakhir di manifest; sama → `unchanged`, tanpa upload.
  * Kunci objek memuat sha256 (content-addressed): isi berbeda tidak pernah menimpa objek lama,
    walau diunggah di hari yang sama.
  * Manifest disimpan di bucket (bukan laptop): `olist/_manifest/<tabel>.json`, riwayat versi hanya bertambah.
"""
from __future__ import annotations

import csv
import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from .schema import TABLES, Table

PREFIX = "olist"
MANIFEST_VERSION = 1
CHUNK = 1024 * 1024


@dataclass(frozen=True)
class FileFacts:
    sha256: str
    size_bytes: int
    rows: int


def file_facts(path: Path) -> FileFacts:
    """sha256 + ukuran + jumlah baris data (parser CSV: newline di dalam tanda kutip tidak dihitung ganda)."""
    h = hashlib.sha256()
    size = 0
    with path.open("rb") as f:
        while chunk := f.read(CHUNK):
            h.update(chunk)
            size += len(chunk)
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        records = sum(1 for _ in csv.reader(f))
    return FileFacts(h.hexdigest(), size, max(records - 1, 0))


def object_key(table: str, filename: str, sha256: str, ingest_date: str) -> str:
    return f"{PREFIX}/{table}/ingest_date={ingest_date}/sha256={sha256[:12]}/{filename}"


def manifest_key(table: str) -> str:
    return f"{PREFIX}/_manifest/{table}.json"


def _is_missing(err: Exception) -> bool:
    code = getattr(err, "response", {}).get("Error", {}).get("Code", "")
    return code in ("404", "NoSuchKey", "NoSuchBucket", "NotFound")


def read_manifest(s3, bucket: str, table: str) -> dict | None:
    try:
        body = s3.get_object(Bucket=bucket, Key=manifest_key(table))["Body"].read()
    except Exception as e:  # botocore ClientError
        if _is_missing(e):
            return None
        raise
    return json.loads(body)


def write_manifest(s3, bucket: str, manifest: dict) -> None:
    s3.put_object(
        Bucket=bucket, Key=manifest_key(manifest["table"]),
        Body=json.dumps(manifest, indent=2, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )


def object_exists(s3, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as e:
        if _is_missing(e):
            return False
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
    path = raw_dir / t.file
    facts = file_facts(path)
    when = now or datetime.now(timezone.utc)
    ingest_date = when.strftime("%Y-%m-%d")
    manifest = read_manifest(s3, bucket, t.name)

    latest = manifest["versions"][-1] if manifest and manifest["versions"] else None
    if latest and latest["sha256"] == facts.sha256:
        return _result(t, "unchanged", facts, latest["object_key"], latest["version"], time.time() - t0)

    key = object_key(t.name, t.file, facts.sha256, ingest_date)
    # Objek dengan kunci ini ada bila run sebelumnya mati sebelum manifest tertulis, atau isi kembali ke versi lama.
    if not object_exists(s3, bucket, key):
        s3.upload_file(
            str(path), bucket, key,
            ExtraArgs={"Metadata": {"sha256": facts.sha256, "source": t.file, "rows": str(facts.rows)}},
        )

    version = (latest["version"] + 1) if latest else 1
    entry = {
        "version": version, "object_key": key, "sha256": facts.sha256, "size_bytes": facts.size_bytes,
        "rows": facts.rows, "ingested_at": when.isoformat(timespec="seconds"),
    }
    manifest = manifest or {
        "manifest_version": MANIFEST_VERSION, "table": t.name, "source_name": t.file,
        "bucket": bucket, "versions": [],
    }
    manifest["versions"].append(entry)
    manifest["latest"] = {"version": version, "object_key": key}
    write_manifest(s3, bucket, manifest)  # manifest ditulis TERAKHIR: objek ada dulu, baru diumumkan
    return _result(t, "uploaded" if version == 1 else "new_version", facts, key, version, time.time() - t0)


def _result(t: Table, status: str, f: FileFacts, key: str, version: int, secs: float) -> dict:
    return {
        "table": t.name, "file": t.file, "status": status, "version": version, "object_key": key,
        "rows": f.rows, "expected_rows": t.rows, "rows_match": f.rows == t.rows,
        "size_bytes": f.size_bytes, "sha256": f.sha256, "seconds": round(secs, 2),
    }


def run_ingest(s3, bucket: str, raw_dir: Path, tables=TABLES) -> dict:
    """Ingest semua tabel. Berhenti lebih awal bila file sumber hilang (sebelum menyentuh bucket)."""
    missing = [t.file for t in tables if not (raw_dir / t.file).exists()]
    if missing:
        raise FileNotFoundError(f"file sumber tidak ada di {raw_dir}: {', '.join(missing)}")
    ensure_bucket(s3, bucket)
    t0 = time.time()
    results = [ingest_table(s3, bucket, t, raw_dir) for t in tables]
    return {"stage": "bronze", "bucket": bucket, "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "seconds": round(time.time() - t0, 2), "files": results}


def make_s3(env: dict[str, str]):
    import boto3
    from botocore.config import Config

    port = env.get("MINIO_API_PORT", "9000")
    return boto3.client(
        "s3", endpoint_url=env.get("MINIO_ENDPOINT", f"http://localhost:{port}"),
        aws_access_key_id=env["MINIO_ROOT_USER"], aws_secret_access_key=env["MINIO_ROOT_PASSWORD"],
        region_name="us-east-1", config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 3}),
    )


def report_md(run: dict) -> str:
    o = ["# Laporan Bronze (T1)\n",
         f"Bucket `{run['bucket']}` · dijalankan {run['ran_at']} · total {run['seconds']} s\n",
         "| Tabel | Status | Versi | Baris | Ukuran (byte) | Waktu (s) | sha256 | Objek |",
         "|---|---|---:|---:|---:|---:|---|---|"]
    for r in run["files"]:
        flag = "" if r["rows_match"] else f" ⚠ kontrak {r['expected_rows']:,}"
        o.append(f"| {r['table']} | {r['status']} | {r['version']} | {r['rows']:,}{flag} | {r['size_bytes']:,} | "
                 f"{r['seconds']} | `{r['sha256'][:12]}` | `{r['object_key']}` |")
    counts: dict[str, int] = {}
    for r in run["files"]:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    o.append("\nRingkasan: " + ", ".join(f"{k}={v}" for k, v in sorted(counts.items())) + ".")
    o.append("\nManifest per tabel: `olist/_manifest/<tabel>.json` di bucket yang sama (JSON biasa; `latest` menunjuk objek terbaru).")
    return "\n".join(o) + "\n"


def write_outputs(root: Path, run: dict) -> Path:
    out = root / "outputs"
    out.mkdir(exist_ok=True)
    (out / "bronze.json").write_text(json.dumps(run, indent=2, ensure_ascii=False), encoding="utf-8")
    md = out / "bronze_report.md"
    md.write_text(report_md(run), encoding="utf-8")
    return md
