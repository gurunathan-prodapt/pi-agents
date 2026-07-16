"""
Target File: dags/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/dw_dwh_umsatz_konsolidierung_monatlich_js.py
Airflow DAG to orchestrate the monthly Umsatz Consolidation workflow.
Mirrors the folder structure of the legacy UC4 job environment.
"""

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryValueCheckOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# ---------------------------------------------------------
# GLOBAL PRODUCTION CONFIGURATION (DYNAMICALLY SOURCED)
# ---------------------------------------------------------
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION", default_var="europe-west3")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET", default_var="DWH_TARGET")
DATAPROC_SUBNET = Variable.get("DATAPROC_SUBNET", default_var="default")

# Mirror path mapping logic
PYSPARK_SCRIPT_URI = f"gs://{GCS_BUCKET}/pyspark/DWH/DWH_KERN/PRODUKTION/DW.DWH_UMSATZ/abinitio/umsatz_konsolidierung.py"

# ---------------------------------------------------------
# LEGACY LOGGING PRESERVATION (OUTPUT/PRINT LITERAL RULE)
# ---------------------------------------------------------
def log_legacy_start_message(**context):
    logical_date = context['logical_date']
    verarbeitungsmonat = logical_date.strftime('%Y%m')
    konzerngesellschaft = "ALL"
    
    # OUTPUT/PRINT LITERAL RULE: Must match the original German text output character-for-character
    logging.info(f"Umsatzkonsolidierung fuer Monat {verarbeitungsmonat}, Konzerngesellschaft {konzerngesellschaft} angestossen")

def log_legacy_end_message(**context):
    # OUTPUT/PRINT LITERAL RULE: Verbatim preservation of legacy success marker
    logging.info("Monatliche Umsatzkonsolidierung ohne erkennbare Fehler beendet")

# ---------------------------------------------------------
# DEFAULT ARGS & CONFIGURATION
# ---------------------------------------------------------
default_args = {
    'owner': 'dw_analytics',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

# ── DAG DEFINITION ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_umsatz_konsolidierung_monatlich_js',
    default_args=default_args,
    description='Consolidate monthly revenue data (legacy Ab Initio umsatz_konsolidierung.mp)',
    schedule='0 3 1 * *',  # Executed monthly on the 1st of the month at 03:00 AM UTC
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['dwh', 'revenue', 'monthly'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    # Triggers legacy tracking start output using exact print text semantics
    log_start = PythonOperator(
        task_id='log_legacy_start',
        python_callable=log_legacy_start_message,
        provide_context=True
    )

    # ---------------------------------------------------------
    # Pre-validation: Verify target period registration
    # ---------------------------------------------------------
    validate_period = BigQueryValueCheckOperator(
        task_id='validate_period',
        sql=f"""
            SELECT COUNT(1) 
            FROM `{GCP_PROJECT}.{BQ_DATASET}.DIM_PERIODE`
            WHERE VERARBEITUNGSMONAT = '{{ logical_date.strftime('%Y%m') }}'
              AND KONZERNGESELLSCHAFT = 'ALL'
        """,
        pass_value=1,
        use_legacy_sql=False
    )

    # ---------------------------------------------------------
    # PySpark Job Execution (Serverless Batch Operator)
    # ---------------------------------------------------------
    pyspark_job_definition = {
        "pyspark_batch": {
            "main_python_file_uri": PYSPARK_SCRIPT_URI,
            "args": [
                "{{ logical_date.strftime('%Y%m') }}",
                "ALL"
            ],
            "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-latest.jar"]
        },
        "environment_config": {
            "execution_config": {
                "subnetwork_uri": DATAPROC_SUBNET
            }
        },
        "runtime_config": {
            "properties": {
                "spark.executor.instances": "4",
                "spark.dynamicAllocation.enabled": "false"
            }
        }
    }

    umsatz_konsolidierung = DataprocCreateBatchOperator(
        task_id='umsatz_konsolidierung',
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        batch_id="dw-umsatz-kons-{{ logical_date.strftime('%Y%m%d-%H%M%S') }}",
        batch=pyspark_job_definition
    )

    # ---------------------------------------------------------
    # Post-validation & Compliance Metrics
    # ---------------------------------------------------------
    validate_row_counts = BigQueryValueCheckOperator(
        task_id='validate_row_counts',
        sql=f"""
            SELECT COUNT(1) 
            FROM `{GCP_PROJECT}.{BQ_DATASET}.FACT_UMSATZ_KONZERN_MONAT`
            WHERE VERARBEITUNGSMONAT = '{{ logical_date.strftime('%Y%m') }}'
              AND KONZERNGESELLSCHAFT = 'ALL'
        """,
        pass_value=1,
        tolerance=1.0,
        use_legacy_sql=False
    )

    # Triggers legacy tracking end output using exact print text semantics
    log_end = PythonOperator(
        task_id='log_legacy_end',
        python_callable=log_legacy_end_message,
        provide_context=True
    )

    # Execution Sequence
    start >> log_start >> validate_period >> umsatz_konsolidierung >> validate_row_counts >> log_end >> end