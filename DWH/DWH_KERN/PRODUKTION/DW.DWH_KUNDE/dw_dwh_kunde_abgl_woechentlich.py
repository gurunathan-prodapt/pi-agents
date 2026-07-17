"""
Orchestration DAG for weekly customer address alignment.
Migrated from: DW.DWH_KUNDE_ABGL_WOECHENTLICH_JP.xml
"""

from datetime import datetime, timedelta
import logging
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataform import DataformRunOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# ───────────────────────────────────────────────────────────────────────
# CONFIGURATION & ENVIRONMENT VARIABLES
# ───────────────────────────────────────────────────────────────────────
# Standardized global variable retrieval to prevent environment hardcoding.
GCP_PROJECT: str = Variable.get("GCP_PROJECT")
GCP_LOCATION: str = Variable.get("GCP_LOCATION")
DATAFORM_REPOSITORY: str = Variable.get(
    "dw_dwh_kunde_dataform_repo", 
    default_var="dwh-kunde-repo"
)

DEFAULT_ARGS: Dict[str, Any] = { 
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# ───────────────────────────────────────────────────────────────────────
# REUSABLE PIPELINE FUNCTIONS (MODULAR LOGIC)
# ───────────────────────────────────────────────────────────────────────

def log_start_message(**context: Any) -> None:
    """
    Logs the exact literal start message using the context-derived date.
    Replaces legacy variable inheritance for &LAUF_WOCHE.
    """
    l_stichtag: str = context['ds_nodash']
    logging.info(f"Starte Adressabgleich Kundenstammdaten fuer Stichtag {l_stichtag}")


def verify_and_log_results(**context: Any) -> None:
    """
    Queries the Dataform output table in BigQuery to count inconsistencies,
    then logs verbatim alerts or success parameters based on findings.
    """
    l_stichtag: str = context['ds_nodash']
    bq_hook = BigQueryHook()
    
    # Securely query the alignment table generated during the current run
    query = f"""
        SELECT COUNT(1) as cnt 
        FROM `{GCP_PROJECT}.dw_dwh_kunde.d_abgl_kunde_woech_result`
        WHERE run_date = PARSE_DATE('%Y%m%d', '{l_stichtag}')
    """
    
    try:
        records = bq_hook.get_first(sql=query)
        l_abweichungen = records[0] if records else 0
    except Exception as e:
        logging.error(f"Failed to query verification table: {str(e)}")
        raise e

    # Simulated path matching structural design rules
    protokoll_datei = f"gs://{GCP_PROJECT}-logs/dw_dwh_kunde/{l_stichtag}/reconciliation_report.log"

    if l_abweichungen > 0:
        # Verbatim migration requirement logic
        logging.warning(
            f"[W] {l_abweichungen} Abweichungen im Kundenadressabgleich gefunden, siehe {protokoll_datei}"
        )
    else:
        logging.info("No discrepancies found. Customer address data matches the reference system.")


def log_completion_message(**context: Any) -> None:
    """
    Logs the exact completion message confirming execution dispatch.
    """
    lauf_woche: str = context['ds_nodash']
    logging.info(f"Kundenadressabgleich fuer Lauf {lauf_woche} angestossen")


# ───────────────────────────────────────────────────────────────────────
# DAG DEFINITION
# ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id='dw_dwh_kunde_abgl_woechentlich_jp',
    description='Woechentlicher Adressabgleich der Kundenstammdaten (KUNDE) gegen das Referenzsystem',
    default_args=DEFAULT_ARGS,
    schedule_interval='0 3 * * 0',  # Every Sunday at 03:00 AM
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['dwh', 'kunde', 'migration_legacy']
) as dag:

    # 1. Log initialization step
    task_start_log = PythonOperator(
        task_id='start_log',
        python_callable=log_start_message,
    )

    # 2. Trigger BigQuery Dataform dynamic validation transformation
    task_run_dataform_reconciliation = DataformRunOperator(
        task_id='run_dataform_reconciliation',
        project_id=GCP_PROJECT,
        location=GCP_LOCATION,
        repository_id=DATAFORM_REPOSITORY,
    )

    # 3. Pull metrics and execute custom logging actions
    task_check_anomalies = PythonOperator(
        task_id='check_anomalies',
        python_callable=verify_and_log_results,
    )

    # 4. Log completion step
    task_end_log = PythonOperator(
        task_id='end_log',
        python_callable=log_completion_message,
    )

    # Linear Task Execution Pattern
    task_start_log >> task_run_dataform_reconciliation >> task_check_anomalies >> task_end_log