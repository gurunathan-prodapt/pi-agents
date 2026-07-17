"""
DAG: dw_dwh_umsatz_konsolidierung_monatlich_jp
Orchestrates the monthly consolidation of revenue data across all group companies,
replacing the legacy Automic/UC4 Jobplan configurations.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# Environment Constants
GCP_PROJECT = Variable.get("GCP_PROJECT")
DAGS_ROOT = f"/home/airflow/gcs/dags/dwh/dwh_kern/produktion/dw_dwh_umsatz"

default_args = {
    'owner': 'dwh_team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': True,
    'email': ['dwh_alerts@company.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dw_dwh_umsatz_konsolidierung_monatlich_jp',
    default_args=default_args,
    description='Monatliche Umsatzkonsolidierung ueber alle Konzerngesellschaften',
    schedule='0 3 1 * *',  # Run on the 1st of every month at 03:00 AM
    catchup=False,
    params={
        'monat': '',     # Pass target YYYYMM override if executing manually
        'konzern': 'ALL' # Defaults to processing 'ALL' companies
    },
    tags=['dwh', 'umsatz', 'konsolidierung']
) as dag:

    # Executes the converted Python wrapper module containing BigQuery API operations
    execute_consolidation = BashOperator(
        task_id='dw_dwh_umsatz_konsolidierung_monatlich_js',
        bash_command=(
            "python3 " + DAGS_ROOT + "/bin/r_umsatz_konsolidierung_monatlich.py "
            "{% if params.monat %}-m {{ params.monat }}{% endif %} "
            "-k {{ params.konzern }}"
        ),
        env={
            'GCP_PROJECT': GCP_PROJECT,
            'BQ_DATASET': Variable.get("BQ_DATASET", default_var="dwh_umsatz_dataset"),
            'SQL_DIR': f"{DAGS_ROOT}/sql"
        }
    )

    execute_consolidation