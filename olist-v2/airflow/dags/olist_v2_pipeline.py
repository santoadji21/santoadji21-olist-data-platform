"""DAG olist_v2_pipeline. Diperluas bertahap: sekarang `download_kaggle` → `ingest_bronze`; silver, gate, load, dbt, publish menyusul.
Airflow hanya merangkai: logikanya ada di paket `olist_v2` (bisa dijalankan tanpa Airflow: `make bronze`)."""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def download_kaggle():
    from olist_v2.download import run_and_report

    r = run_and_report()
    print(f"download: {r['status']} — {len(r['files'])} file cocok kontrak")


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
    download >> ingest
