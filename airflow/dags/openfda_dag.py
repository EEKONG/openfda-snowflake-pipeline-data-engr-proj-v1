import os
import subprocess
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------------------------------------------------------------
# PROJECT PATHS
# ----------------------------------------------------------------------
INGEST_SCRIPT = "/opt/airflow/panda_jobs/Ingest_push_data.py"
DBT_PROJECT = "/opt/airflow/openfda_dbt_project"
DBT = "/home/airflow/.local/bin/dbt"

# ----------------------------------------------------------------------
# TASK FUNCTIONS
# ----------------------------------------------------------------------

def run_ingestion():
    """
    Execute ingestion script to fetch OpenFDA data and upload to Snowflake.
    """
    if not os.path.exists(INGEST_SCRIPT):
        raise FileNotFoundError(f"Ingestion script not found: {INGEST_SCRIPT}")

    subprocess.run(["python", INGEST_SCRIPT], check=True)


def run_dbt():
    """
    Run dbt models and tests using the correct executable path.
    """
    # Run dbt models
    subprocess.run(
        [DBT, "run", "--project-dir", DBT_PROJECT],
        check=True
    )

    # Run dbt tests (must use the full path!)
    subprocess.run(
        [DBT, "test", "--project-dir", DBT_PROJECT],
        check=True
    )

# ----------------------------------------------------------------------
# DAG CONFIG
# ----------------------------------------------------------------------

default_args = {
    "owner": "eddie",
    "depends_on_past": False,
    "email_on_retry": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="openfda_drug_shortages_pipeline",
    description="Daily ETL + dbt pipeline for OpenFDA Drug Shortages Data",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 11, 1),
    catchup=False,
    tags=["openfda", "snowflake", "dbt", "etl"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_openfda_data",
        python_callable=run_ingestion,
    )

    dbt_task = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt,
    )

    ingest_task >> dbt_task
