"""DAG olist_v2_pipeline. Diperluas bertahap: sekarang `download_kaggle` → `ingest_bronze` → `spark_silver` → `contract_gate` → `load_raw` → `dbt_build` → `marts_report`; stream, evidence menyusul.
Airflow hanya merangkai: logikanya ada di paket `olist_v2` (bisa dijalankan tanpa Airflow: `make bronze`)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def download_kaggle():
    from olist_v2.download import run_and_report

    r = run_and_report()
    print(f"download: {r['status']} — {len(r['files'])} file cocok kontrak")


def spark_silver():
    from airflow.exceptions import AirflowException
    from olist_v2.bronze import make_s3
    from olist_v2.rules import SourceError
    from olist_v2.silver import run_silver, spark_session, write_outputs
    import os
    from pathlib import Path

    spark = spark_session()
    try:
        run = run_silver(make_s3(), spark)
    except SourceError as e:
        raise AirflowException(f"silver berhenti (tidak ada yang ditulis): {e}")   # kolom hilang / objek bronze rusak
    finally:
        spark.stop()
    write_outputs(Path(os.environ.get("OLIST_OUTPUTS", "/opt/airflow/outputs")), run)
    failed = [t["table"] for t in run["tables"] if t["verdict"] == "FAIL"]
    if failed:
        raise AirflowException(f"tabel FAIL (tidak diberi silver): {', '.join(failed)}")
    print(f"silver selesai {run['seconds']} s: " + ", ".join(f"{t['table']}={t['verdict']}" for t in run["tables"]))


def contract_gate():
    from pathlib import Path
    import os
    from airflow.exceptions import AirflowException
    from olist_v2.gate import run_gate, write_outputs

    s = run_gate()
    write_outputs(Path(os.environ.get("OLIST_OUTPUTS", "/opt/airflow/outputs")), s)
    if s["failed"] or s["result"] in ("failed", "error"):
        for x in s["failed"][:5]:
            print(f"GAGAL: {x['model']} | {x['name']} | {x['reason']}")
        why = "; ".join(f"{x['name']}: {x['reason']}" for x in s["failed"][:3])
        raise AirflowException(f"gate kontrak GAGAL — {why}")   # DAG berhenti: tidak ada load_raw
    print(f"gate: {s['result']} — {s['passed']}/{s['checks']} lolos, {len(s['soft_violations'])} soft")


def marts_report():
    from airflow.exceptions import AirflowException
    from olist_v2.marts import main

    if main() != 0:
        raise AirflowException("pemeriksaan mart gagal — lihat log task")


def load_raw():
    import os
    from pathlib import Path

    import psycopg2
    from airflow.exceptions import AirflowException
    from olist_v2.bronze import make_s3
    from olist_v2.load import run_load, write_outputs

    conn = psycopg2.connect(os.environ["WAREHOUSE_DSN"], connect_timeout=5)
    try:
        run = run_load(make_s3(), conn)      # satu transaksi: gagal di tengah = rollback
    finally:
        conn.close()
    write_outputs(Path(os.environ.get("OLIST_OUTPUTS", "/opt/airflow/outputs")), run)
    if run["skipped_fail"]:
        raise AirflowException(f"tabel tidak dimuat (FAIL): {', '.join(run['skipped_fail'])}")
    print(f"load selesai {run['seconds']} s: " + ", ".join(f"{t['table']}={t['loaded']:,}" for t in run["tables"]))


def ingest_bronze():
    from olist_v2.bronze import run_and_report

    run = run_and_report()
    statuses = sorted({f["status"] for f in run["files"]})
    print(f"bronze selesai {run['seconds']} s; status: {statuses}")


with DAG(
    dag_id="olist_v2_pipeline",
    description="CSV Olist → bronze (MinIO); tahap berikutnya menyusul",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "olist-v2", "retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["olist-v2"],
) as dag:
    download = PythonOperator(task_id="download_kaggle", python_callable=download_kaggle, execution_timeout=timedelta(minutes=15))
    ingest = PythonOperator(task_id="ingest_bronze", python_callable=ingest_bronze, execution_timeout=timedelta(minutes=10))
    silver = PythonOperator(task_id="spark_silver", python_callable=spark_silver, execution_timeout=timedelta(minutes=20), retries=0)
    gate = PythonOperator(task_id="contract_gate", python_callable=contract_gate, execution_timeout=timedelta(minutes=10), retries=0)
    load = PythonOperator(task_id="load_raw", python_callable=load_raw, execution_timeout=timedelta(minutes=10), retries=0)
    # dbt di venv terpisah ($DBT_BIN). target/ dan logs/ ke /tmp agar tidak menulis ke folder sumber yang di-mount.
    dbt = BashOperator(
        task_id="dbt_build",
        bash_command='"$DBT_BIN" build --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROFILES_DIR"',
        env={"DBT_TARGET_PATH": "/tmp/dbt_target", "DBT_LOG_PATH": "/tmp/dbt_logs", "DBT_SEND_ANONYMOUS_USAGE_STATS": "False"},
        append_env=True, retries=0, execution_timeout=timedelta(minutes=15),
    )
    marts = PythonOperator(task_id="marts_report", python_callable=marts_report, execution_timeout=timedelta(minutes=10), retries=0)
    download >> ingest >> silver >> gate >> load >> dbt >> marts
