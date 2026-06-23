# Airflow DAG for r_ausd_bp_ta_apn_carmen.ksh
# Replaces vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_apn_carmen.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from datetime import datetime, timedelta

# Placeholder for your GCP project and dataset.
# IMPORTANT: Replace these with your actual project and dataset IDs.
GCP_PROJECT_ID = "your_project"
BIGQUERY_DATASET = "your_dataset"
BIGQUERY_SQL_PATH = "sql/d_ausd_bp_ta_apn_carmen.sql"


def _get_stichtag_and_yesterday(**context):
    """
    Replaces the functionality of gestern.ksh and parameter parsing.
    Determines the reference date (Stichtag) for the BigQuery SQL.
    """
    # For now, we'll use yesterday's date as the Stichtag as per typical ETL patterns
    # and the original script's use of 'gestern.ksh'.
    # In a real scenario, this might be a DAG run parameter or derived from a control table.
    execution_date = pendulum.parse(context["ds_nodash"]) # 'ds_nodash' is YYYYMMDD
    stichtag_date = execution_date.subtract(days=1)
    stichtag_yyyymmdd = stichtag_date.format("YYYYMMDD")

    context["ti"].xcom_push(key="stichtag_yyyymmdd", value=stichtag_yyyymmdd)
    print(f"Determined Stichtag (YYYYMMDD): {stichtag_yyyymmdd}")


with DAG(
    dag_id="r_ausd_bp_ta_apn_carmen_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=timedelta(days=1),  # Daily schedule, adjust as needed
    catchup=False,
    tags=["isbert", "bert", "apn", "bigquery"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    # Task to determine the Stichtag (reference date)
    get_stichtag_task = PythonOperator(
        task_id="get_stichtag",
        python_callable=_get_stichtag_and_yesterday,
        provide_context=True,
    )

    # Task to execute the BigQuery SQL transformation
    # The SQL script itself determines `v_datum` from `dwtk_meldungen`,
    # so we don't need to pass `stichtag_yyyymmdd` directly as a template,
    # but the placeholder in the SQL ensures we know it's there.
    execute_bigquery_sql_task = BigQueryOperator(
        task_id="execute_bigquery_sql",
        sql=BIGQUERY_SQL_PATH,
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        params={
            "project_id": GCP_PROJECT_ID,
            "dataset_id": BIGQUERY_DATASET,
            # If the SQL were to directly use a parameter for v_datum, it would look like this:
            # "stichtag_yyyymmdd": "{{ ti.xcom_pull(task_ids='get_stichtag', key='stichtag_yyyymmdd') }}"
        },
    )

    get_stichtag_task >> execute_bigquery_sql_task