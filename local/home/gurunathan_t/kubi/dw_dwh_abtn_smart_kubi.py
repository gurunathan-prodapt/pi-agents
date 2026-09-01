from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ── GLOBAL CONFIGURATION ─────────────────────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET")

# ── JOB-SPECIFIC CONFIGURATION ───────────────────────────────────────────────
GCP_CONN_ID = Variable.get("GCP_CONN_ID", default_var=None)
SQL_FILE_PATH = Variable.get("SQL_FILE_PATH")

# ── DEFAULT ARGS ─────────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── HELPER FUNCTIONS ─────────────────────────────────────────────────────────
def log_and_get_monatsid(logical_date, **context):
    """
    Calculates the reporting month (MONATSID) based on the execution date.
    If the execution day is before the 15th, it uses the previous month;
    otherwise, it uses the current month.
    """
    dt = logical_date.in_timezone('Europe/Berlin')
    if dt.day < 15:
        first_of_month = dt.replace(day=1)
        prev_month = first_of_month - timedelta(days=1)
        monatsid = prev_month.strftime('%Y%m')
    else:
        monatsid = dt.strftime('%Y%m')
    
    # OUTPUT/PRINT LITERAL RULE: Preserve the literal text exactly
    print(f"Berichtsmonat:  {monatsid}")
    return monatsid

def get_sql_query(**context):
    """
    Reads the SQL query from the configured path (GCS or local filesystem).
    """
    sql_path = SQL_FILE_PATH
    if sql_path.startswith("gs://"):
        from airflow.providers.google.cloud.hooks.gcs import GCSHook
        bucket, blob = sql_path.replace("gs://", "").split("/", 1)
        hook = GCSHook(gcp_conn_id=GCP_CONN_ID)
        query = hook.download_as_byte_array(bucket, blob).decode("utf-8")
    else:
        with open(sql_path, "r", encoding="utf-8") as f:
            query = f.read()
    return query

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=DEFAULT_ARGS,
    description="Populate temp table - Standalone Migration wrapper",
    schedule=None,  # Externally triggered
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # Task 1: Calculate and log MONATSID
    log_monatsid = PythonOperator(
        task_id="log_monatsid",
        python_callable=log_and_get_monatsid,
    )

    # Task 2: Retrieve SQL query content
    get_query = PythonOperator(
        task_id="get_sql_query",
        python_callable=get_sql_query,
    )

    # Task 3: Execute BigQuery SQL script
    dwh_abtn_smart_kubi = BigQueryInsertJobOperator(
        task_id="dwh_abtn_smart_kubi",
        gcp_conn_id=GCP_CONN_ID,
        project_id=GCP_PROJECT,
        location=GCP_REGION,
        configuration={
            "query": {
                "query": "{{ task_instance.xcom_pull(task_ids='get_sql_query') }}",
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "monats_id",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {"value": "{{ task_instance.xcom_pull(task_ids='log_monatsid') }}"}
                    },
                    {
                        "name": "eintrags_nr",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {"value": "0"}
                    }
                ]
            }
        }
    )

    # ── DEPENDENCIES ─────────────────────────────────────────────────────────
    [log_monatsid, get_query] >> dwh_abtn_smart_kubi