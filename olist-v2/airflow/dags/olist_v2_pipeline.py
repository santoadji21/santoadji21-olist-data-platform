"""DAG olist_v2_pipeline. Diperluas bertahap: sekarang `download_kaggle` → `ingest_bronze` → `spark_silver` → `contract_gate`; load, gate, load, dbt, publish menyusul.
Airflow hanya merangkai: logikanya ada di paket `olist_v2` (bisa dijalankan tanpa Airflow: `make bronze`)."""
from datetime import datetime, timedelta

from airflow import DAG
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
        raise AirflowException(f"gate kontrak GAGAL: {[x['name'] for x in s['failed']][:5]}")   # DAG berhenti: tidak ada load_raw
    print(f"gate: {s['result']} — {s['passed']}/{s['checks']} lolos, {len(s['soft_violations'])} soft")


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
    download >> ingest >> silver >> gate
