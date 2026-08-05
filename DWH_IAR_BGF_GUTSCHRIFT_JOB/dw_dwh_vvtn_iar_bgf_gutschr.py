"""
DAG: dw_dwh_vvtn_iar_bgf_gutschr
Description: Transform Gutschrift (credit note) files into a consolidated CSV format.
             This was migrated from the UC4 UNIX job DW.DWH_VVTN_IAR_BGF_GUTSCHR.
             It runs on-demand (no direct schedule) and initializes variable context 
             for execution.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# ─── GLOBAL CONFIGURATION ─────────────────────────────────────────────────────
# Sourced dynamically from Airflow variables as per environment-wide policy
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")

# ─── DEFAULT ARGUMENTS ────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'dw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG( 
    dag_id='dw_dwh_vvtn_iar_bgf_gutschr',
    default_args=DEFAULT_ARGS,
    description='Transform Gutschrift files to one file CSV',
    schedule=None,  # Externally triggered or on-demand execution
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
) as dag:

    # Task representing the legacy UNIX Job DW.DWH_VVTN_IAR_BGF_GUTSCHR.
    # It initializes the shell environment context, resolves execution parameters, 
    # outputs the validation message, and executes the target transformation binary.
    dwh_vvtn_iar_bgf_gutschr = BashOperator(
        task_id='dwh_vvtn_iar_bgf_gutschr',
        bash_command="""
            . $HOME/.dw_init
            export DWH_JOB_KENNUNG='VVTN_IAR_BGF_GUTSCHR'
            export Month_ID="{{ (data_interval_end.in_timezone('Europe/Berlin') - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%Y%m') }}"
            echo "Lastmonth is $Month_ID"
            $HOME/aktuell/vorverarbeitung/tn/bin/r_vvtn_iar_bgf_gutschrift
        """,
    )

    dwh_vvtn_iar_bgf_gutschr