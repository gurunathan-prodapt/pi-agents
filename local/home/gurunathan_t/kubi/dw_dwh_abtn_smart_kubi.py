"""
dw_dwh_abtn_smart_kubi

This DAG represents the migrated workflow for the native UC4 Unix job `DW.DWH_ABTN_SMART_KUBI`.
Its primary function is to execute an SQL script (`d_abtn_x_smart_kubi.sql`) that populates a 
temporary database table. 

The job dynamically computes a reporting month identifier (`MONATSID`) based on the execution date:
if the execution day is before the 15th, it shifts the context back to the prior month.

Because this job was extracted standalone without an enclosing UC4 Job Plan (JOBP),
it is configured as an externally triggered or on-demand pipeline (schedule=None).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# =============================================================================
# GCP Configuration (Sourced from Airflow Variables to avoid placeholders)
# =============================================================================
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
BQ_DATASET = Variable.get("BQ_DATASET")

# Job-specific configuration
JOB_CONFIG = {
    "dwh_job_kennung": "ABTN_SMART_KUBI",
}

# =============================================================================
# Default Arguments
# =============================================================================
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# =============================================================================
# Helper Functions
# =============================================================================
def print_berichtsmonat(logical_date, **kwargs):
    """
    Calculates and prints the reporting month (MONATSID) based on the logical date.
    If the day of execution is before the 15th, it shifts back to the prior month.
    """
    if logical_date.day < 15:
        first_of_current = logical_date.replace(day=1)
        previous_month = first_of_current - timedelta(days=1)
        monatsid = previous_month.strftime("%Y%m")
    else:
        monatsid = logical_date.strftime("%Y%m")
    
    # OUTPUT/PRINT LITERAL RULE: Must match the legacy print statement exactly
    print(f"Berichtsmonat:  {monatsid}")
    return monatsid

# =============================================================================
# DAG Definition
# =============================================================================
with DAG(
    dag_id="dw_dwh_abtn_smart_kubi",
    default_args=DEFAULT_ARGS,
    description="Populate temp table - Migrated from DW.DWH_ABTN_SMART_KUBI",
    schedule=None,  # Externally triggered / Manual run
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["migrated_uc4", "jobs_unix"],
) as dag:

    # Task to calculate and print the reporting month (MONATSID)
    calculate_monatsid = PythonOperator(
        task_id="calculate_monatsid",
        python_callable=print_berichtsmonat,
    )

    # Placeholder task for the SQL execution (d_abtn_x_smart_kubi.sql)
    # # REVIEW-STRUCT: The UC4 script executes the SQL file:
    # # "$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql" 
    # # with parameter -i &MONATSID (calculated date variable).
    # # 
    # # Currently implemented as EmptyOperator placeholder until companion pipeline 
    # # determines target execution type (e.g., BigQuery SQL vs wrapper Python).
    dwh_abtn_smart_kubi = EmptyOperator(
        task_id="dwh_abtn_smart_kubi",
        doc_md="""
        ### UC4 Source Metadata
        * **Source Name:** DW.DWH_ABTN_SMART_KUBI
        * **Original Host:** |DWHDWH1P|HOST
        * **Original Login:** DW.UNIX.ISTNS
        * **Original Script Target:** `$HOME/aktuell/aufbereitung/tn/sql/d_abtn_x_smart_kubi.sql`
        """,
    )

    # Execution Order
    calculate_monatsid >> dwh_abtn_smart_kubi