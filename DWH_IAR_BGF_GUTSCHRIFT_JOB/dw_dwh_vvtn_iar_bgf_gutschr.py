"""
This DAG automates the legacy UC4 workflow DW.DWH_VVTN_IAR_BGF_GUTSCHR.
It transforms Gutschrift (credit) files to a unified CSV format.
The original UC4 job executed the script:
$HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
and set environment configurations including the job identifier ('VVTN_IAR_BGF_GUTSCHR')
and the previous calendar month's identifier (LASTMONTH_YYYYMM).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# --- GLOBAL Variables (sourced at runtime to avoid hardcoded environment values) ---
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=None)
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=None)
BQ_DATASET = Variable.get("BQ_DATASET", default_var=None)

# --- Default Arguments ---
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# --- DAG Definition ---
with DAG(
    dag_id="dw_dwh_vvtn_iar_bgf_gutschr",
    default_args=DEFAULT_ARGS,
    description="Transform Gutschrift files to one file CSV",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["migrated_uc4", "gutschrift"],
) as dag:

    # Task representing the UC4 job DW.DWH_VVTN_IAR_BGF_GUTSCHR
    dw_dwh_vvtn_iar_bgf_gutschr_task = BashOperator(
        task_id="dw_dwh_vvtn_iar_bgf_gutschr_task",
        bash_command="""
        . $HOME/.dw_init
        export DWH_JOB_KENNUNG="VVTN_IAR_BGF_GUTSCHR"
        export Month_ID="{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}"
        echo "Lastmonth is $Month_ID"
        $HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
        """,
    )

    dw_dwh_vvtn_iar_bgf_gutschr_task