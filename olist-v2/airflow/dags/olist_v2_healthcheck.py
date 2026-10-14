"""DAG pertama olist-v2: memastikan MinIO (bucket bronze/silver/gold) dan Postgres warehouse (schema raw/staging/dw/mart) siap.
Hanya memakai pustaka bawaan image Airflow dan kredensial dari environment."""
import os
import urllib.request
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def check_minio():
    url = os.environ["MINIO_ENDPOINT"] + "/minio/health/live"
    with urllib.request.urlopen(url, timeout=5) as r:
        assert r.status == 200, f"MinIO tidak sehat: {r.status}"
    print("MinIO sehat:", url)


def check_warehouse():
    import psycopg2

    with psycopg2.connect(os.environ["WAREHOUSE_DSN"], connect_timeout=5) as conn, conn.cursor() as cur:
        cur.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name IN ('raw','staging','dw','mart')")
        found = sorted(r[0] for r in cur.fetchall())
    assert found == ["dw", "mart", "raw", "staging"], f"schema warehouse tidak lengkap: {found}"
    print("Schema warehouse lengkap:", found)


with DAG(
    dag_id="olist_v2_healthcheck",
    description="Fase 1: infrastruktur inti siap (MinIO + Postgres warehouse)",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["olist-v2", "infra"],
) as dag:
    minio = PythonOperator(task_id="check_minio", python_callable=check_minio)
    warehouse = PythonOperator(task_id="check_warehouse", python_callable=check_warehouse)
    minio >> warehouse
