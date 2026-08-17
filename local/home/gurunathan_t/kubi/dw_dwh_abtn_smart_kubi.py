"""
DAG: dw_dwh_abtn_smart_kubi
Description: Populate temp table - Translated from UC4 DW.DWH_ABTN_SMART_KUBI.
This DAG executes a dynamic date math calculation to determine the reporting month (MONATSID).
If executed before the 15th of the month, it targets the previous month; otherwise, the current month.
The core SQL execution has been stubbed out as it is migrated in a separate pipeline.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ==============================================================================
# ── GCP Configuration ─────────────────────────────────────────────────────────
# ==============================================================================
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_CONN_ID = Variable.get("BQ_CONN_ID", default_var="google_cloud_default")

# ==============================================================================
# ── Job Configuration ─────────────────────────────────────────────────────────
# ==============================================================================
JOB_CONFIG = {
    "DWH_JOB_KENNUNG": "ABTN_SMART_KUBI",
}

# ==============================================================================
# ── Dynamic Date Logic (UC4 Script Translation) ──────────────────────────────
# ==============================================================================
def get_reporting_month(logical_date):
    """
    Translates UC4 logic:
    :set &cdate  = SYS_DATE("YYYYMMDD")
    :set &cmonth = SUBSTR(&cdate,1,6)
    :set &cday   = SUBSTR(&cdate,7,2)
    :if  &cday  < '15'
    :     set &first = '01'
    :     set &cmonth = "&cmonth&first"
    :     set &cmonth = SUB_DAYS(&cmonth,1)
    :     set &cmonth = SUBSTR(&cmonth,1,6)
    :endif
    :set &MONATSID = &cmonth
    """
    day = logical_date.day
    if day < 15:
        # Get first of current month, subtract one day to go to previous month
        first_of_month = logical_date.replace(day=1)
        last_day_prev_month = first_of_month - timedelta(days=1)
        monatsid = last_day_prev_month.strftime('%Y%m')
    else:
        monatsid = logical_date.strftime('%Y%m')
    
    # German-language print statement matching legacy UC4 log requirement
    print(f"Berichtsmonat:  {monatsid}")
    return monatsid

def log_reporting_month(**context):
    logical_date = context['logical_date']
    monatsid = get_reporting_month(logical_date)
    # Push to XCom to make it available for future actual operators (e.g. BigQueryInsertJobOperator)
    context['ti'].xcom_push(key='MONATSID', value=monatsid)

# ==============================================================================
# ── Default Args ──────────────────────────────────────────────────────────────
# ==============================================================================
default_args = {
    'owner': 'dw',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# ── DAG Definition ────────────────────────────────────────────────────────────
# ==============================================================================
# REVIEW: Standalone Workflow Assumption — since no parent JOBP workflow was provided
# in this extraction, this job has been isolated into its own DAG. If this job is actually
# a task within a larger workflow, merge its task definition into that workflow's DAG.
with DAG( 
    dag_id='dw_dwh_abtn_smart_kubi',
    default_args=default_args,
    description='Populate temp table - Translated from UC4 DW.DWH_ABTN_SMART_KUBI',
    schedule=None,  # No calendar schedule defined in UC4 extraction
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated', 'uc4', 'sql_script'],
    params={
        "dwh_job_kennung": JOB_CONFIG["DWH_JOB_KENNUNG"],
    }
) as dag:

    # Task to calculate parameters and log reporting month
    calculate_parameters = PythonOperator(
        task_id="calculate_parameters",
        python_callable=log_reporting_month,
    )

    # REVIEW-STRUCT: launcher wraps SQL script [d_abtn_x_smart_kubi.sql] converted by a separate pipeline --
    # confirm whether it produced a Python script or BigQuery SQL, then replace this stub with a
    # BashOperator/PythonOperator (Python) or BigQueryInsertJobOperator (BigQuery SQL) accordingly
    dw_dwh_abtn_smart_kubi = EmptyOperator(
        task_id="dw_dwh_abtn_smart_kubi"
    )

    # ── Dependencies ──────────────────────────────────────────────────────────
    calculate_parameters >> dw_dwh_abtn_smart_kubi