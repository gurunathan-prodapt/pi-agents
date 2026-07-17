"""
DAG: dw_dwh_rechnung_export_taeglich_js
Orchestration workflow for extracting and exporting daily invoice data.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Import custom reusable helper logic
from dw_rechnung.bin.dwh_rechnung_export_taeglich_bin import GcsExportHelper

# ─── ENVIRONMENT VALUES (GLOBAL POLICY CONFORMING) ───────────────────────────
GCP_PROJECT = os.environ.get("GCP_PROJECT", Variable.get("GCP_PROJECT", default_var="gcp-dwh-prod"))
GCS_BUCKET = Variable.get("GCS_EXPORT_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="DWH_KERN")

# ─── JOB-SPECIFIC PARAMETERS ──────────────────────────────────────────────────
JOB_KENNUNG = "RECHNUNG_EXPORT_TAEGLICH"
SQL_FILE_PATH = "sql/d_exp_rechnung_taeglich.sql"

# Output log templates matching original UC4 definitions
LOG_MESSAGE_TEMPLATE = "Rechnungsexport fuer Stichtag {stichtag} angestossen"

default_args = {
    'owner': 'dwh-operations',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def get_sql_query(file_path: str) -> str:
    """Reads SQL query from file path relative to DAG root."""
    dag_dir = os.path.dirname(os.path.abspath(__file__))
    full_path = os.path.join(dag_dir, file_path)
    with open(full_path, 'r') as file:
        return file.read()

def run_consolidation(**kwargs):
    """Orchestrates validation and consolidation via GcsExportHelper."""
    stichtag = kwargs.get('ds_nodash')
    helper = GcsExportHelper(project_id=GCP_PROJECT)
    
    source_prefix = f"exports/rechnung/shards_{stichtag}/rechnung_export_"
    destination_blob = f"rechnung/ausgang/rechnung_export_{stichtag}.dat"
    
    line_count = helper.validate_and_consolidate_shards(
        bucket_name=GCS_BUCKET,
        source_prefix=source_prefix,
        destination_blob_name=destination_blob,
        expected_delimiter="|"
    )
    
    # Push line count metric to XCom
    kwargs['ti'].xcom_push(key='exported_rows', value=line_count)

def log_trigger_success(**kwargs):
    """Emits UC4 execution context logs in German language format."""
    stichtag_val = kwargs['ds_nodash']
    print(LOG_MESSAGE_TEMPLATE.format(stichtag=stichtag_val))


with DAG(
    dag_id='dw_dwh_rechnung_export_taeglich_js',
    default_args=default_args,
    description='Executes extract queries and outputs consolidated pipe-separated data to GCS',
    schedule_interval='0 6 * * *',  # Triggers daily at 06:00 AM
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # Read base query and dynamic environment placeholder injection
    raw_sql = get_sql_query(SQL_FILE_PATH)
    resolved_sql = raw_sql.replace("@gcp_project", GCP_PROJECT).replace("@bq_dataset", BQ_DATASET)

    # Wrap the query inside a native BigQuery EXPORT DATA structure
    export_query_wrapper = f"""
    EXPORT DATA OPTIONS(
      uri=CONCAT('gs://{GCS_BUCKET}/exports/rechnung/shards_', @p_Stichtag, '/rechnung_export_*.csv'),
      format='CSV',
      overwrite=true,
      header=false,
      field_delimiter='|'
    ) AS
    {resolved_sql}
    """

    # Task 1: Initialize status log trace
    print_status = PythonOperator(
        task_id='print_status',
        python_callable=log_trigger_success,
    )

    # Task 2: Trigger BigQuery Export Query Job with dynamic parameters
    execute_export_query = BigQueryInsertJobOperator(
        task_id='execute_export_query',
        configuration={
            "query": {
                "query": export_query_wrapper,
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "p_Stichtag",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": "{{ ds_nodash }}"}
                    }
                ]
            }
        }
    )

    # Task 3: Consolidate GCS Shards & Validate Row Counts
    consolidate_files = PythonOperator(
        task_id='consolidate_files',
        python_callable=run_consolidation,
    )

    # Execution Topology
    print_status >> execute_export_query >> consolidate_files