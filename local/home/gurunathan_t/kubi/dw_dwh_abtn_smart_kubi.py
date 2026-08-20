from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Global Environment Variables
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_LOCATION = Variable.get("BQ_LOCATION", default_var="EU")
SQL_TEMPLATE_PATH = Variable.get("SQL_TEMPLATE_PATH", default_var="/home/airflow/gcs/dags/sql")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def calculate_monatsid(logical_date):
    """
    Replicates the legacy UC4 dynamic date calculation logic:
    - If execution day < 15, use the previous month (YYYYMM).
    - Otherwise, use the current month (YYYYMM).
    Calculated relative to logical_date for DAG idempotency.
    """
    day = logical_date.day
    if day < 15:
        first_of_this_month = logical_date.replace(day=1)
        last_day_prev_month = first_of_this_month - timedelta(days=1)
        return last_day_prev_month.strftime("%Y%m")
    else:
        return logical_date.strftime("%Y%m")

def log_berichtsmonat(**context):
    """
    To preserve logging parity, all printed outputs are kept verbatim from the source.
    Logs 'Berichtsmonat:  <MONATSID>' using the exact original literal text, character-for-character.
    """
    logical_date = context["logical_date"]
    monatsid_value = calculate_monatsid(logical_date)
    logging.info(f"Berichtsmonat:  {monatsid_value}")

with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=default_args,
    description="Populate temp table - Migrated from UC4 DW.DWH_ABTN_SMART_KUBI",
    schedule=None,  # Externally triggered or manual
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[SQL_TEMPLATE_PATH],
    user_defined_macros={
        "calculate_monatsid": calculate_monatsid
    },
) as dag:

    log_monatsid = PythonOperator(
        task_id="log_monatsid",
        python_callable=log_berichtsmonat,
    )

    monatsid_param = "{{ calculate_monatsid(logical_date) }}"

    execute_smart_kubi_sql = BigQueryInsertJobOperator(
        task_id="execute_d_abtn_x_smart_kubi",
        configuration={
            "query": {
                "query": "d_abtn_x_smart_kubi.sql",
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "param_monats_id",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": monatsid_param}
                    },
                    {
                        "name": "param_eintrags_nr",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {"value": "1"}
                    }
                ]
            }
        },
        gcp_conn_id="google_cloud_default",
        location=BQ_LOCATION,
    )

    log_monatsid >> execute_smart_kubi_sql