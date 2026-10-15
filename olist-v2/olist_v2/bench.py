"""Benchmark silver: Spark (local[2]) vs DuckDB (2 thread) pada pekerjaan YANG SAMA — baca CSV teks, cast tipe, evaluasi aturan hard/soft
dari kontrak ODCS, FK ke induk, tulis Parquet bersih + karantina. Keduanya membaca dan menulis DISK LOKAL (tanpa MinIO) agar yang diukur adalah mesinnya.

  python -m olist_v2.bench run --engine spark|duckdb --scale N [--out hasil.json]   # satu proses = satu pengukuran
  python -m olist_v2.bench report                                                     # gabungkan outputs/benchmark.json -> benchmark_report.md

`make benchmark` menjalankan semuanya di container scheduler (Spark: python Airflow; DuckDB: venv datacontract). Skala N > 1 = isi tiap CSV
diulang N kali (hanya untuk mengukur waktu/memori; aturan kunci akan melanggar, jadi verdict tidak dibandingkan di skala > 1).
Paritas (jumlah baris sumber, karantina, pelanggaran tiap aturan) dibandingkan antar mesin pada skala 1.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import statistics
import sys
import time
from pathlib import Path

from . import rules
from .contract import REPO, Model, load_models

RAW = Path(os.environ.get("OLIST_RAW_DIR", REPO / "data" / "raw"))
WORK = Path(os.environ.get("BENCH_WORK", "/tmp/olist-bench"))


# ---------------------------------------------------------------- data skala
def prepare(scale: int) -> Path:
    """Folder CSV skala N di dalam data/raw (agar ter-mount). N=1: symlink relatif; N>1: header sekali + isi N kali."""
    d = RAW / f".bench-{scale}x"
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    for m in load_models():
        src, dst = RAW / m.file, d / m.file
        if scale == 1:
            os.symlink(Path("..") / m.file, dst)
            continue
        with open(src, "rb") as f:
            header = f.readline()
            body = f.read()
        if not body.endswith(b"\n"):                       # akhir berkas tanpa newline: pakai pengakhir baris milik berkas itu (CRLF/LF)
            body += b"\r\n" if header.endswith(b"\r\n") else b"\n"
        with open(dst, "wb") as g:
            g.write(header)
            for _ in range(scale):
                g.write(body)
    return d


def peak_rss_mb() -> float:
    """Puncak RSS proses ini + anak langsungnya (JVM Spark adalah anak proses Python), dari VmHWM."""
    me, total = os.getpid(), 0
    for p in Path("/proc").glob("[0-9]*"):
        try:
            st = (p / "status").read_text()
        except OSError:
            continue
        f = dict(l.split(":", 1) for l in st.splitlines() if ":" in l)
        if int(p.name) == me or int(f.get("PPid", "0").strip()) == me:
            total += int(f.get("VmHWM", "0 kB").split()[0])
    return round(total / 1024, 1)


# ---------------------------------------------------------------- Spark
def run_spark(raw_dir: Path, out: Path) -> dict:
    from .silver import process_table, read_raw, spark_session

    models = {m.name: m for m in load_models()}
    t_sess = time.time()
    spark = spark_session({"SPARK_MASTER_URL": "local[2]"})
    sess_s = round(time.time() - t_sess, 2)
    mem = spark.sparkContext.getConf().get("spark.driver.memory", "(bawaan JVM)")
    t0 = time.time()
    tables, parents = {}, {}
    try:
        for name in rules.process_order(list(models.values())):
            m = models[name]
            raw = read_raw(spark, str(raw_dir / m.file))
            rules.check_header(m, raw.columns)
            r = process_table(raw, m, parents)
            parents[name] = r.silver
            r.silver.write.mode("overwrite").parquet(str(out / f"silver_{name}"))
            if r.quarantined:
                r.quarantine.write.mode("overwrite").parquet(str(out / f"quarantine_{name}"))
            tables[name] = {"source_rows": r.source_rows, "quarantined": r.quarantined, "clean": r.clean, "verdict": r.verdict,
                            "rules": {x.name: x.violations for x in r.rules}}
        peak = peak_rss_mb()
    finally:
        spark.stop()
    return {"engine": f"spark {spark.version} local[2]", "driver_memory": mem, "session_seconds": sess_s,
            "seconds": round(time.time() - t0, 2), "peak_rss_mb": peak, "tables": tables}


# ---------------------------------------------------------------- DuckDB
def _typed_exprs(m: Model) -> tuple[list[str], list[str]]:
    """(ekspresi kolom bertipe, ekspresi alasan gagal) dari tipe kontrak — padanan SQL dari silver.typed_frame."""
    cols, reasons = [], []
    for c in m.columns:
        s = f'"{c.name}"'
        kind = rules.cast_kind(c.pg_type)
        if kind == "text":
            val, bad = s, "false"
        elif kind == "zip5":
            ok = f"regexp_matches({s}, '^\\d{{1,5}}$')"
            val, bad = f"CASE WHEN {ok} THEN lpad({s}, 5, '0') END", f"({s} IS NOT NULL AND NOT {ok})"
        elif kind == "int":
            d = f"try_cast({s} AS DOUBLE)"
            ok = f"({d} IS NOT NULL AND {d} = floor({d}))"
            val, bad = f"CASE WHEN {ok} THEN CAST({d} AS INTEGER) END", f"({s} IS NOT NULL AND NOT coalesce({ok}, false))"
        elif kind == "decimal":
            val = f"try_cast({s} AS {rules.decimal_spec(c.pg_type).upper()})"
            bad = f"({s} IS NOT NULL AND {val} IS NULL)"
        elif kind == "double":
            val = f"try_cast({s} AS DOUBLE)"
            bad = f"({s} IS NOT NULL AND {val} IS NULL)"
        else:
            val = f"try_strptime({s}, '%Y-%m-%d %H:%M:%S')"
            bad = f"({s} IS NOT NULL AND {val} IS NULL)"
        cols.append(f'{val} AS "{c.name}"')
        reasons.append(f"CASE WHEN {bad} THEN 'cast:{c.name}:{c.pg_type}' END")
        if c.required:
            reasons.append(f"CASE WHEN {s} IS NULL THEN 'not_null:{c.name}' END")
    return cols, reasons


def run_duckdb(raw_dir: Path, out: Path) -> dict:
    import duckdb

    models = {m.name: m for m in load_models()}
    con = duckdb.connect()
    con.execute("PRAGMA threads=2")
    t0 = time.time()
    tables = {}
    for name in rules.process_order(list(models.values())):
        m = models[name]
        path = str(raw_dir / m.file).replace("'", "''")
        con.execute(f"CREATE TABLE src AS SELECT * FROM read_csv('{path}', header=true, all_varchar=true, quote='\"', escape='\"')")
        orig = [r[0] for r in con.execute("DESCRIBE src").fetchall()]
        clean = rules.clean_header(orig)
        rules.check_header(m, clean)
        con.execute("CREATE TABLE raw AS SELECT row_number() OVER () AS _row, " +
                    ", ".join(f'NULLIF("{o}", \'\') AS "{c}"' for o, c in zip(orig, clean)) + " FROM src")
        con.execute("DROP TABLE src")
        cols, reasons = _typed_exprs(m)
        con.execute(f"CREATE TABLE typed AS SELECT _row, {', '.join(cols)}, concat_ws(';', {', '.join(reasons)}) AS _cast FROM raw")
        fk = [f"CASE WHEN t.\"{f.column}\" IS NOT NULL AND NOT EXISTS (SELECT 1 FROM silver_{f.parent} p WHERE p.\"{f.parent_column}\" = t.\"{f.column}\") "
              f"THEN 'fk:{f.column}→{f.parent}.{f.parent_column}' END" for f in m.fks]
        hard = [f"CASE WHEN ({r.where}) THEN 'rule:{r.name}' END" for r in m.hard]
        con.execute(f"CREATE TABLE tr AS SELECT t.*, concat_ws(';', t._cast{''.join(', ' + x for x in hard + fk)}) AS _reasons FROM typed t")
        aggs = ["count(*) AS _n", "sum(CASE WHEN _reasons != '' THEN 1 ELSE 0 END) AS _bad", "sum(CASE WHEN _cast != '' THEN 1 ELSE 0 END) AS _cast_bad"]
        aggs += [f"sum(CASE WHEN ({r.where}) THEN 1 ELSE 0 END) AS h_{i}" for i, r in enumerate(m.hard)]
        aggs += [f"sum(CASE WHEN ({r.where}) THEN 1 ELSE 0 END) AS s_{i}" for i, r in enumerate(m.soft)]
        row = dict(zip([d[0] for d in con.execute(f"SELECT {', '.join(aggs)} FROM tr").description], con.fetchone()))
        n, bad = int(row["_n"]), int(row["_bad"] or 0)
        dup = 0
        if m.key:
            k = ", ".join(f'"{c}"' for c in m.key)
            dup = int(con.execute(f"SELECT coalesce(sum(c), 0) FROM (SELECT count(*) AS c FROM tr GROUP BY {k} HAVING count(*) > 1)").fetchone()[0])
        flag = rules.flag_column(m)
        sel = ", ".join(f'"{c}"' for c in m.col_names)
        if flag:
            sel += ", coalesce(" + " OR ".join(f"({r.where})" for r in m.soft) + f", false) AS \"{flag}\""
        con.execute(f"CREATE TABLE silver_{name} AS SELECT {sel} FROM tr WHERE _reasons = ''")
        con.execute(f"COPY silver_{name} TO '{out}/silver_{name}.parquet' (FORMAT parquet)")
        if bad:
            con.execute(f"COPY (SELECT * FROM tr WHERE _reasons != '') TO '{out}/quarantine_{name}.parquet' (FORMAT parquet)")
        res = {"row_count": abs(n - m.rows)}
        if m.key:
            res[f"key_unique({','.join(m.key)})"] = dup
        res["type_and_not_null"] = int(row["_cast_bad"] or 0)
        res.update({r.name: int(row[f"h_{i}"] or 0) for i, r in enumerate(m.hard)})
        res.update({r.name: int(row[f"s_{i}"] or 0) for i, r in enumerate(m.soft)})
        tables[name] = {"source_rows": n, "quarantined": bad, "clean": n - bad, "verdict": rules.decide(m, n, bad, dup).verdict, "rules": res}
        con.execute("DROP TABLE raw; DROP TABLE typed; DROP TABLE tr")
    return {"engine": f"duckdb {duckdb.__version__} threads=2", "session_seconds": 0.0, "seconds": round(time.time() - t0, 2),
            "peak_rss_mb": peak_rss_mb(), "tables": tables}


# ---------------------------------------------------------------- CLI
def run_one(engine: str, scale: int) -> dict:
    raw_dir = prepare(scale)
    out = WORK / f"{engine}-{scale}x"
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    try:
        r = run_spark(raw_dir, out) if engine == "spark" else run_duckdb(raw_dir, out)
        r["output_mb"] = round(sum(f.stat().st_size for f in out.rglob("*") if f.is_file()) / 1e6, 1)
        r["status"] = "ok"
    except Exception as e:     # OOM dll. adalah HASIL ukur ("tidak layak"), bukan kegagalan benchmark
        r = {"status": "error", "error": f"{type(e).__name__}: {str(e).splitlines()[0][:300] if str(e) else ''}"}
    finally:
        shutil.rmtree(out, ignore_errors=True)
        shutil.rmtree(raw_dir, ignore_errors=True)
    r["input_mb"] = round(sum((RAW / m.file).stat().st_size for m in load_models()) * scale / 1e6, 1)
    return {"engine_key": engine, "scale": scale, **r}


def report(runs: list[dict]) -> str:
    o = ["# Benchmark silver — Spark vs DuckDB\n",
         "Pekerjaan sama: baca CSV teks → cast tipe → aturan hard/soft dari kontrak ODCS → FK ke induk → tulis Parquet bersih + karantina, "
         "9 tabel. Disk lokal (tanpa MinIO), di container scheduler (8 CPU Docker, batas memori 3 GB); Spark `local[2]`, DuckDB 2 thread. "
         "Skala N = isi tiap CSV diulang N kali.\n",
         "| Skala | Input (MB) | Mesin | Waktu (s), median | Rentang | Start sesi (s) | Puncak RSS (MB) | Output (MB) | Status |", "|---|---:|---|---:|---|---:|---:|---:|---|"]
    by: dict = {}
    for r in runs:
        by.setdefault((r["scale"], r["engine_key"]), []).append(r)
    for (scale, eng), rs in sorted(by.items(), key=lambda kv: (kv[0][0], kv[0][1])):
        ok = [r for r in rs if r["status"] == "ok"]
        if ok:
            t = [r["seconds"] for r in ok]
            o.append(f"| {scale}× | {rs[0]['input_mb']:,} | {ok[0]['engine']} | {statistics.median(t):.1f} | {min(t):.1f}–{max(t):.1f} (n={len(t)}) | "
                     f"{statistics.median([r['session_seconds'] for r in ok]):.1f} | {max(r['peak_rss_mb'] for r in ok):,.0f} | {ok[0]['output_mb']:,} | ok |")
        else:
            o.append(f"| {scale}× | {rs[0]['input_mb']:,} | {eng} | — | — | — | — | — | **GAGAL**: {rs[0]['error']} |")
    # paritas pada skala 1
    s1 = {k: next((r for r in v if r["status"] == "ok"), None) for k, v in by.items() if k[0] == 1}
    sp, du = s1.get((1, "spark")), s1.get((1, "duckdb"))
    if sp and du:
        diffs = []
        for t, a in sp["tables"].items():
            b = du["tables"][t]
            for k in ("source_rows", "quarantined", "clean", "verdict"):
                if a[k] != b[k]:
                    diffs.append(f"{t}.{k}: spark={a[k]} duckdb={b[k]}")
            for rule, v in a["rules"].items():
                if b["rules"].get(rule) != v:
                    diffs.append(f"{t}.{rule}: spark={v} duckdb={b['rules'].get(rule)}")
        n_rules = sum(len(a["rules"]) for a in sp["tables"].values())
        o += ["", "## Paritas hasil (skala 1×)\n",
              f"{'**Identik**' if not diffs else '**BERBEDA**'}: {len(sp['tables'])} tabel, {n_rules} hitungan aturan, jumlah baris sumber/karantina/bersih, dan verdict "
              f"dibandingkan antara Spark dan DuckDB." + ("" if not diffs else "\n\n" + "\n".join(f"- {d}" for d in diffs[:20]))]
    # kesimpulan dari angka
    o += ["", "## Kesimpulan (dihitung dari tabel di atas)\n"]
    for scale in sorted({r["scale"] for r in runs}):
        a, b = by.get((scale, "spark"), []), by.get((scale, "duckdb"), [])
        a_ok, b_ok = [r for r in a if r["status"] == "ok"], [r for r in b if r["status"] == "ok"]
        if a_ok and b_ok:
            ta, tb = statistics.median(r["seconds"] for r in a_ok), statistics.median(r["seconds"] for r in b_ok)
            o.append(f"- {scale}×: DuckDB {tb:.1f} s vs Spark {ta:.1f} s (+ start sesi {statistics.median(r['session_seconds'] for r in a_ok):.1f} s) → "
                     f"DuckDB {ta / tb:.1f}× lebih cepat; memori puncak DuckDB {max(r['peak_rss_mb'] for r in b_ok):,.0f} MB vs Spark {max(r['peak_rss_mb'] for r in a_ok):,.0f} MB.")
        elif b_ok and not a_ok:
            o.append(f"- {scale}×: Spark GAGAL ({a[0]['error']}), DuckDB selesai dalam {statistics.median(r['seconds'] for r in b_ok):.1f} s.")
        elif a_ok and not b_ok:
            o.append(f"- {scale}×: DuckDB GAGAL ({b[0]['error']}), Spark selesai dalam {statistics.median(r['seconds'] for r in a_ok):.1f} s.")
    return "\n".join(o) + "\n"


def suite(a) -> int:
    import subprocess

    duck_py = os.environ.get("BENCH_DUCKDB_PY", "/opt/airflow/venvs/datacontract/bin/python")
    runs = []
    for spec in a.scales.split(","):
        scale, reps = (int(x) for x in spec.split(":"))
        for rep in range(reps):
            for engine, py in (("duckdb", duck_py), ("spark", sys.executable)):
                print(f"[bench] {engine} {scale}× (ulangan {rep + 1}/{reps}) …", flush=True)
                try:
                    p = subprocess.run([py, "-m", "olist_v2.bench", "run", "--engine", engine, "--scale", str(scale)], capture_output=True,
                                       text=True, timeout=a.timeout, env={**os.environ, "PYTHONPATH": str(REPO)})
                    line = next((l for l in reversed(p.stdout.splitlines()) if l.startswith("{")), None)
                    r = json.loads(line) if line else {"engine_key": engine, "scale": scale, "status": "error", "input_mb": 0,
                                                       "error": f"proses mati exit {p.returncode} (mis. dibunuh karena memori): {p.stderr.strip().splitlines()[-1][:200] if p.stderr.strip() else ''}"}
                except subprocess.TimeoutExpired:
                    r = {"engine_key": engine, "scale": scale, "status": "error", "input_mb": 0, "error": f"melewati batas {a.timeout} s"}
                if "input_mb" not in r or not r["input_mb"]:
                    r["input_mb"] = round(sum((RAW / m.file).stat().st_size for m in load_models()) * scale / 1e6, 1)
                print(f"        → {r['status']}" + (f" {r['seconds']} s, RSS {r['peak_rss_mb']} MB" if r["status"] == "ok" else f": {r['error']}"), flush=True)
                runs.append(r)
    out = REPO / "outputs"
    (out / "benchmark.json").write_text(json.dumps({"runs": runs}, indent=2))
    (out / "benchmark_report.md").write_text(report(runs), encoding="utf-8")
    print("ditulis outputs/benchmark_report.md")
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="olist_v2.bench")
    sub = ap.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run")
    r.add_argument("--engine", choices=["spark", "duckdb"], required=True)
    r.add_argument("--scale", type=int, default=1)
    r.add_argument("--out", help="tambahkan hasil ke berkas JSON ini")
    s = sub.add_parser("suite", help="semua kombinasi mesin × skala × ulangan, tiap pengukuran di proses terpisah, lalu laporan")
    s.add_argument("--scales", default="1:3,5:1,10:1", help="skala:ulangan, dipisah koma")
    s.add_argument("--timeout", type=int, default=1500, help="batas detik per pengukuran")
    p = sub.add_parser("report")
    p.add_argument("--in", dest="inp", default=str(REPO / "outputs" / "benchmark.json"))
    a = ap.parse_args(argv)
    if a.cmd == "run":
        res = run_one(a.engine, a.scale)
        print(json.dumps(res))
        if a.out:
            f = Path(a.out)
            data = json.loads(f.read_text()) if f.exists() else {"runs": []}
            data["runs"].append(res)
            f.write_text(json.dumps(data, indent=2))
        return 0
    if a.cmd == "suite":
        return suite(a)
    data = json.loads(Path(a.inp).read_text())
    Path(a.inp).with_name("benchmark_report.md").write_text(report(data["runs"]), encoding="utf-8")
    print("ditulis outputs/benchmark_report.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
