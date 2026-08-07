import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ==============================================================================
# ── Global / Environment Configuration ─────────────────────────────────────────
# ==============================================================================
GCP_PROJECT = os.environ.get("GCP_PROJECT")

# Airflow Variable keys for the migrated UC4 VARA objects
DW_DWH_MONITORED_JPS_VAR = "DW_DWH_MONITORED_JPS"
DW_DWH_RUNNING_JOBS_VAR = "DW_DWH_RUNNING_JOBS"

# ==============================================================================
# ── Callback / Alerting Stubs ──────────────────────────────────────────────────
# ==============================================================================
def on_failure_callback_stub(context):
    """
    Placeholder callback to mimic UC4 error handling behavior if needed.
    """
    task_id = context.get('task_instance').task_id
    print(f"Task {task_id} failed. Executing standard alert/cleanup actions.")


# ==============================================================================
# ── DAG Definition ────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_adm_job_monitor_start',
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['utility', 'uc4_migration', 'jobi'],
) as dag:

    # ==========================================================================
    # ── Task: dwh_adm_job_monitor_start_helper ────────────────────────────────
    # ==========================================================================
    def execute_job_monitor_start(**context):
        """
        Executes the job monitoring start logic, mapping UC4 variables
        to the Airflow task execution context.
        """
        # Mapping UC4 scheduler-set variables to Airflow context
        # ADMJP: Parent JobPlan Name -> dag_id
        # ADMJOB: Job Name -> task_id
        # ADMNRJOB: Job Run Number -> run_id
        adm_jp = context['dag'].dag_id                     # UC4: SYS_ACT_JPNAME()
        adm_job = context['task_instance'].task_id          # UC4: SYS_ACT_JOBNAME()
        adm_nr_job = context['run_id']                     # UC4: SYS_ACT_JOBNR()
        dwh_job_kennung = ""                               # UC4: :SET &DWH_JOB_KENNUNG=""
        
        # Original: :IF &ADMJP NE " "
        if adm_jp and adm_jp.strip() != "":
            try:
                # Sourcing the monitored JPs configuration (equivalent of DW.DWH_MONITORED_JPS)
                # It is stored as an Airflow Variable containing a JSON dictionary of JP name -> Active Flag (e.g. {"JP_NAME": "J"})
                monitored_jps = Variable.get(DW_DWH_MONITORED_JPS_VAR, deserialize_json=True, default_var={})
                
                # Check for each monitored job plan configuration
                for adm_gb, adm_wert in monitored_jps.items():
                    if adm_wert == "J":
                        if adm_gb == adm_jp or adm_gb == "ALL":
                            # Output text literal from original code:
                            # :print "Added &ADMJOB with &ADMNRJOB"
                            print(f"Added {adm_job} with {adm_nr_job}")
                            
                            # Original: PUT_VAR DW.DWH_RUNNING_JOBS,&ADMJOB,"&ADMNRJOB"
                            # We update the shared active running jobs registry.
                            # Standard Airflow Variable approach:
                            running_jobs = Variable.get(DW_DWH_RUNNING_JOBS_VAR, deserialize_json=True, default_var={})
                            running_jobs[adm_job] = adm_nr_job
                            Variable.set(DW_DWH_RUNNING_JOBS_VAR, running_jobs, serialize_json=True)
                            
            except Exception as e:
                print(f"Error updating job monitoring status: {str(e)}")
                raise e

    dwh_adm_job_monitor_start_helper = PythonOperator(
        task_id='dwh_adm_job_monitor_start_helper',
        python_callable=execute_job_monitor_start,
        provide_context=True,
        on_failure_callback=on_failure_callback_stub,
    )

    dwh_adm_job_monitor_start_helper