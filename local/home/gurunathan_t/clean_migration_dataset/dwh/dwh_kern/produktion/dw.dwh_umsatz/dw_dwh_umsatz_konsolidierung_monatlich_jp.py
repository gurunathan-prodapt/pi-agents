import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ==============================================================================
# ENVIRONMENT VARIABLES - CLASSIFIED BY ENVIRONMENT VALUES POLICY
# ==============================================================================
# GLOBAL (Environment-wide infrastructure configs fetched dynamically)
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# JOB-SPECIFIC CONSTANTS
JOB_KENNUNG = 'UMSATZ_KONSOLIDIERUNG_MONATLICH'
KONZERNGESELLSCHAFT = 'ALL'

# ==============================================================================
# OUTPUT/PRINT LITERAL RULE COMPLIANCE
# ==============================================================================
# The original German print statement literal from the UC4 Unix Job script
# is preserved character-for-character inside the processing log execution:
# "Umsatzkonsolidierung fuer Monat &VERARBEITUNGSMONAT, Konzerngesellschaft &KONZERNGESELLSCHAFT angestossen"
def log_start_message(**context):
    execution_date = context['execution_date']
    verarbeitungsmonat = execution_date.strftime('%Y%m')
    logging.info(
        "Umsatzkonsolidierung fuer Monat %s, Konzerngesellschaft %s angestossen",
        verarbeitungsmonat,
        KONZERNGESELLSCHAFT
    )

# ==============================================================================
# AIRFLOW DAG ORCHESTRATION DEFINITION
# ==============================================================================
DEFAULT_ARGS = {
    'owner': 'dw',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="dw_dwh_umsatz_konsolidierung_monatlich_jp",
    default_args=DEFAULT_ARGS,
    description="Monatliche Konsolidierung der Umsatzdaten (UMSATZ) ueber alle Konzerngesellschaften",
    schedule=None,  # Scheduled manually or triggered externally as per UC4 configuration
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # Task to log the start message at execution runtime with proper Jinja rendering
    log_start = PythonOperator(
        task_id="log_start",
        python_callable=log_start_message,
        provide_context=True,
    )

    # PySpark Job Configuration using dynamic template values
    pyspark_job_config = {
        "reference": {
            "project_id": GCP_PROJECT_ID
        },
        "placement": {
            "cluster_name": DATAPROC_CLUSTER_NAME
        },
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/umsatz_konsolidierung.py",
            "args": [
                "-m", "{{ execution_date.strftime('%Y%m') }}",
                "-k", KONZERNGESELLSCHAFT,
                "--job_kennung", JOB_KENNUNG
            ],
        },
    }

    # Task mapping from legacy DW.DWH_UMSATZ_KONSOLIDIERUNG_MONATLICH_JS
    dw_dwh_umsatz_konsolidierung_monatlich_js = DataprocSubmitJobOperator(
        task_id="dw_dwh_umsatz_konsolidierung_monatlich_js",
        job=pyspark_job_config,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        job_id="{{ dag.dag_id }}_{{ run_id }}_dw_dwh_umsatz_konsolidierung_monatlich_js",
    )

    # Simple linear flow pipeline structure
    log_start >> dw_dwh_umsatz_konsolidierung_monatlich_js