"""Silver (Spark): bronze → data bertipe + gate kualitas → Parquet di bucket silver (+ karantina + laporan DQ).

Ketentuan dibawa dari T0–T6: aturan hard → karantina (bukan dibuang) dengan kolom alasan; aturan soft → dihitung dan
diberi flag (baris tetap lewat); ambang per tabel; verdict PASS | PASS+KARANTINA | FAIL; kolom hilang berhenti SEBELUM
menulis apa pun; tabel FAIL tidak mendapat silver. Semua aturan dibaca dari kontrak ODCS (satu sumber).

Struktur: `process_table` murni Spark (diuji dengan Spark lokal di data kecil); `run_silver` menangani S3.
Spark berjalan di mode lokal (SPARK_MASTER_URL=local[2]) atau cluster dengan mengganti satu nilai itu.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from . import rules
from .bronze import BUCKET as BRONZE, PREFIX, ensure_bucket, make_s3, read_manifest
from .contract import REPO, Model, load_models
from .rules import RuleResult, SourceError

SILVER = "silver"
DQ_KEY = f"{PREFIX}/_dq/dq.json"


def silver_prefix(table: str) -> str:
    return f"{PREFIX}/{table}/"


def quarantine_prefix(table: str) -> str:
    return f"{PREFIX}/_quarantine/{table}/"


@dataclass
class TableResult:
    model: Model
    source_rows: int
    quarantined: int
    clean: int
    verdict: str
    fail_reasons: list[str]
    rules: list[RuleResult]
    silver: object = None          # DataFrame Spark (baris lolos + kolom flag)
    quarantine: object = None      # DataFrame Spark (baris sumber teks + _row + _reasons)
    parent_keys: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        m = self.model
        pct = 100.0 * self.quarantined / self.source_rows if self.source_rows else 0.0
        return {"table": m.name, "source_rows": self.source_rows, "expected_rows": m.rows, "quarantined": self.quarantined,
                "clean": self.clean, "quarantine_pct": round(pct, 4), "threshold_pct": m.threshold_pct, "verdict": self.verdict,
                "fail_reasons": self.fail_reasons, "rules": [r.as_dict() for r in self.rules]}


# ------------------------------------------------------------------------------------------------ Spark
def spark_session(env=os.environ):
    from pyspark.sql import SparkSession

    master = env.get("SPARK_MASTER_URL", "local[2]")
    b = (SparkSession.builder.appName("olist-v2-silver").master(master)
         .config("spark.sql.session.timeZone", "UTC")      # timestamp sumber tanpa zona: jam dinding dipertahankan
         .config("spark.ui.enabled", "false"))
    if not master.startswith("local"):
        b = b.config("spark.driver.host", env.get("SPARK_DRIVER_HOST", socket.gethostname()))
    # S3A ke MinIO dari environment (berlaku di container maupun di host); jar hadoop-aws sudah ada di image Airflow.
    # Di host (tanpa jar) isi SPARK_JARS_PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262".
    if env.get("MINIO_ENDPOINT"):
        b = (b.config("spark.hadoop.fs.s3a.endpoint", env["MINIO_ENDPOINT"])
             .config("spark.hadoop.fs.s3a.path.style.access", "true")
             .config("spark.hadoop.fs.s3a.connection.ssl.enabled", str(env["MINIO_ENDPOINT"].startswith("https")).lower())
             .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
             .config("spark.hadoop.fs.s3a.access.key", env["MINIO_ROOT_USER"])
             .config("spark.hadoop.fs.s3a.secret.key", env["MINIO_ROOT_PASSWORD"]))
    if env.get("SPARK_JARS_PACKAGES"):
        b = b.config("spark.jars.packages", env["SPARK_JARS_PACKAGES"])
    return b.getOrCreate()


def read_raw(spark, path: str):
    """Semua kolom sebagai TEKS (tanpa inferSchema); nilai kosong → NULL; nama kolom dibersihkan dari BOM."""
    from pyspark.sql import functions as F

    df = (spark.read.option("header", True).option("multiLine", True).option("quote", '"').option("escape", '"')
          .option("encoding", "UTF-8").csv(path))
    df = df.toDF(*rules.clean_header(df.columns))
    return df.select(*[F.when(F.col(f"`{c}`") == "", None).otherwise(F.col(f"`{c}`")).alias(c) for c in df.columns])


def typed_frame(raw, m: Model):
    """Teks → tipe kontrak. Menambah `_row`, `_cast` (alasan gagal-cast / null pada kolom wajib, dipisah ';')."""
    from pyspark.sql import functions as F

    raw = raw.withColumn("_row", F.monotonically_increasing_id())
    exprs, reasons = [], []
    for c in m.columns:
        s = F.col(f"`{c.name}`")
        kind = rules.cast_kind(c.pg_type)
        if kind == "text":
            val, bad = s, F.lit(False)
        elif kind == "zip5":                     # kode pos: teks 5 digit, nol di depan dipulihkan
            ok = s.rlike(r"^\d{1,5}$")
            val, bad = F.when(ok, F.lpad(s, 5, "0")), s.isNotNull() & ~ok
        elif kind == "int":
            d = s.cast("double")
            ok = d.isNotNull() & (d == F.floor(d))
            val, bad = F.when(ok, d.cast("int")), s.isNotNull() & ~ok
        elif kind == "decimal":
            val = s.cast(rules.decimal_spec(c.pg_type))
            bad = s.isNotNull() & val.isNull()
        elif kind == "double":
            val = s.cast("double")
            bad = s.isNotNull() & val.isNull()
        else:                                     # timestamp
            val = F.to_timestamp(s, "yyyy-MM-dd HH:mm:ss")
            bad = s.isNotNull() & val.isNull()
        exprs.append(val.alias(c.name))
        reasons.append(F.when(bad, F.lit(f"cast:{c.name}:{c.pg_type}")))
        if c.required:
            reasons.append(F.when(s.isNull(), F.lit(f"not_null:{c.name}")))
    return raw, raw.select("_row", *exprs, F.concat_ws(";", *reasons).alias("_cast"))


def process_table(raw, m: Model, parents: dict | None = None) -> TableResult:
    """Gate untuk satu tabel. `raw` = DataFrame teks (kolom sudah dibersihkan); `parents` = {nama: DataFrame lolos}."""
    from pyspark.sql import functions as F

    parents = parents or {}
    raw, typed = typed_frame(raw, m)

    # FK (hard): baris anak yatim dikarantina, bukan dibuang
    fk_reasons = []
    for i, fk in enumerate(m.fks):
        if fk.parent not in parents:
            raise SourceError(f"{m.name}: tabel induk {fk.parent} belum diproses (urutan FK salah)")
        keys = parents[fk.parent].select(F.col(fk.parent_column).alias(f"_pk{i}")).distinct()
        typed = typed.join(keys, typed[fk.column] == keys[f"_pk{i}"], "left")
        fk_reasons.append(F.when(F.col(fk.column).isNotNull() & F.col(f"_pk{i}").isNull(), F.lit(f"fk:{fk.column}→{fk.parent}.{fk.parent_column}")))

    hard_exprs = [F.when(F.expr(r.where), F.lit(f"rule:{r.name}")) for r in m.hard]
    typed = typed.withColumn("_reasons", F.concat_ws(";", F.col("_cast"), *hard_exprs, *fk_reasons)).persist()

    soft_cols = [F.coalesce(F.expr(r.where), F.lit(False)) for r in m.soft]
    agg = [F.count(F.lit(1)).alias("_n"),
           F.sum(F.when(F.col("_reasons") != "", 1).otherwise(0)).alias("_bad"),
           F.sum(F.when(F.col("_cast") != "", 1).otherwise(0)).alias("_cast_bad")]
    agg += [F.sum(F.when(F.expr(r.where), 1).otherwise(0)).alias(f"h_{i}") for i, r in enumerate(m.hard)]
    agg += [F.sum(F.when(F.expr(r.where), 1).otherwise(0)).alias(f"s_{i}") for i, r in enumerate(m.soft)]
    row = typed.agg(*agg).collect()[0]
    n, bad = int(row["_n"]), int(row["_bad"] or 0)

    dup = 0
    if m.key:
        dup = typed.groupBy(*m.key).count().where("count > 1").agg(F.sum("count")).collect()[0][0] or 0

    def pct(x):
        return round(100.0 * x / n, 4) if n else 0.0

    res: list[RuleResult] = [RuleResult(m.name, "row_count", "hard", "batch berhenti", abs(n - m.rows), pct(abs(n - m.rows)))]
    if m.key:
        res.append(RuleResult(m.name, f"key_unique({','.join(m.key)})", "hard", "batch berhenti", int(dup), pct(int(dup))))
    res.append(RuleResult(m.name, "type_and_not_null", "hard", "baris → karantina", int(row["_cast_bad"] or 0), pct(int(row["_cast_bad"] or 0))))
    for i, r in enumerate(m.hard):
        v = int(row[f"h_{i}"] or 0)
        res.append(RuleResult(m.name, r.name, "hard", "baris → karantina", v, pct(v)))
    for i, r in enumerate(m.soft):
        v = int(row[f"s_{i}"] or 0)
        res.append(RuleResult(m.name, r.name, "soft", "simpan + flag", v, pct(v)))

    verdict = rules.decide(m, n, bad, int(dup))
    good = typed.where(F.col("_reasons") == "")
    flag = rules.flag_column(m)
    cols = [F.col(c) for c in m.col_names]
    if flag:
        cols.append(F.coalesce(F.expr(" OR ".join(f"({r.where})" for r in m.soft)), F.lit(False)).alias(flag))
    silver = good.select(*cols)
    quarantine = raw.join(typed.where(F.col("_reasons") != "").select("_row", "_reasons"), "_row")
    return TableResult(m, n, bad, n - bad, verdict.verdict, verdict.fail_reasons, res, silver, quarantine)


# ------------------------------------------------------------------------------------------------ S3 / alur
def verified_entry(s3, table: str) -> dict:
    """Versi terbaru dari manifest bronze, dengan sha256 objek DIVERIFIKASI (deteksi objek yang diubah di bucket)."""
    m = read_manifest(s3, BRONZE, table)
    if not m or not m.get("versions"):
        raise SourceError(f"{table}: belum ada di bronze — jalankan `make bronze` dulu")
    v = m["versions"][-1]
    h = hashlib.sha256()
    body = s3.get_object(Bucket=BRONZE, Key=v["object_key"])["Body"]
    for chunk in iter(lambda: body.read(1024 * 1024), b""):
        h.update(chunk)
    if h.hexdigest() != v["sha256"]:
        raise SourceError(f"{table}: isi objek bronze tidak cocok dengan sha256 di manifest ({v['object_key']})")
    return v


def delete_prefix(s3, bucket: str, prefix: str) -> int:
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3.delete_object(Bucket=bucket, Key=obj["Key"])
            n += 1
    return n


def run_silver(s3, spark, models: list[Model] | None = None) -> dict:
    models = models or load_models()
    by = {m.name: m for m in models}
    t0 = time.time()

    # 1) integritas bronze + cek kolom SEMUA tabel sebelum menulis apa pun
    entries, raws = {}, {}
    for name in rules.process_order(models):
        m = by[name]
        entries[name] = verified_entry(s3, name)
        raws[name] = read_raw(spark, f"s3a://{BRONZE}/{entries[name]['object_key']}")
        rules.check_header(m, raws[name].columns)

    ensure_bucket(s3, SILVER)
    parents, results = {}, []
    for name in rules.process_order(models):
        m = by[name]
        r = process_table(raws[name], m, parents)
        results.append(r)
        parents[name] = r.silver
        if r.quarantined:
            r.quarantine.write.mode("overwrite").parquet(f"s3a://{SILVER}/{quarantine_prefix(name)}")
        else:
            delete_prefix(s3, SILVER, quarantine_prefix(name))        # buang sisa karantina run lama
        if r.verdict == "FAIL":
            delete_prefix(s3, SILVER, silver_prefix(name))             # sumber rusak: jangan sediakan silver
        else:
            r.silver.write.mode("overwrite").parquet(f"s3a://{SILVER}/{silver_prefix(name)}")

    run = {"stage": "silver", "ran_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "engine": f"spark {spark.version} ({spark.sparkContext.master})", "seconds": round(time.time() - t0, 2),
           "tables": [{**r.as_dict(), "source": {"version": entries[r.model.name]["version"], "sha256": entries[r.model.name]["sha256"]}}
                      for r in results]}
    s3.put_object(Bucket=SILVER, Key=DQ_KEY, Body=json.dumps(run, indent=2).encode(), ContentType="application/json")
    return run


def report_md(run: dict) -> str:
    o = ["# Laporan Kualitas Data (silver, Spark)\n", f"Dijalankan {run['ran_at']} · {run['seconds']} s · {run['engine']}\n",
         "Verdict: `PASS` · `PASS+KARANTINA` (ada baris dikarantina, di bawah ambang) · `FAIL` (di atas ambang / kunci tidak unik / "
         "jumlah baris menyimpang → silver tidak disediakan).\n", "## Ringkasan per tabel\n",
         "| Tabel | Baris sumber | Karantina | Lolos (silver) | Karantina % | Ambang % | Verdict |", "|---|---:|---:|---:|---:|---:|---|"]
    for t in run["tables"]:
        o.append(f"| {t['table']} | {t['source_rows']:,} | {t['quarantined']:,} | {t['clean']:,} | {t['quarantine_pct']:.3f} | "
                 f"{t['threshold_pct']} | **{t['verdict']}** |")
    fails = [t for t in run["tables"] if t["fail_reasons"]]
    if fails:
        o.append("\n## Alasan FAIL\n")
        o += [f"- `{t['table']}`: {w}" for t in fails for w in t["fail_reasons"]]
    o += ["\n## Aturan\n", "| Tabel | Aturan | Jenis | Tindakan | Pelanggaran | % |", "|---|---|---|---|---:|---:|"]
    for t in run["tables"]:
        for r in t["rules"]:
            o.append(f"| {r['table']} | `{r['name']}` | {r['kind']} | {r['action']} | {r['violations']:,} | {r['pct']:.3f} |")
    hard = sum(1 for t in run["tables"] for r in t["rules"] if r["kind"] == "hard")
    soft = sum(1 for t in run["tables"] for r in t["rules"] if r["kind"] == "soft")
    o.append(f"\nTotal aturan: {hard} hard, {soft} soft (minimal tugas: 10 hard, 8 soft).")
    o.append("\nKarantina: `silver/olist/_quarantine/<tabel>/` (kolom sumber apa adanya + `_row` + `_reasons`).")
    return "\n".join(o) + "\n"


def write_outputs(outputs: Path, run: dict) -> Path:
    outputs.mkdir(parents=True, exist_ok=True)
    (outputs / "dq.json").write_text(json.dumps(run, indent=2, ensure_ascii=False), encoding="utf-8")
    md = outputs / "dq_report.md"
    md.write_text(report_md(run), encoding="utf-8")
    return md


def main(env=os.environ) -> int:
    spark = spark_session(env)
    try:
        run = run_silver(make_s3(env), spark)
    except SourceError as e:
        print(f"silver berhenti (Postgres/silver tidak disentuh): {e}", file=sys.stderr)
        return 1
    finally:
        spark.stop()
    write_outputs(Path(env.get("OLIST_OUTPUTS", REPO / "outputs")), run)
    for t in run["tables"]:
        print(f"  {t['table']:30s} {t['verdict']:15s} {t['source_rows']:>9,} sumber  {t['quarantined']:>6,} karantina")
    print(f"selesai {run['seconds']} s → outputs/dq_report.md")
    return 1 if any(t["verdict"] == "FAIL" for t in run["tables"]) else 0


if __name__ == "__main__":
    sys.exit(main())
