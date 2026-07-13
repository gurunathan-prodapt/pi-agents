from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ── GCP Configuration ────────────────────────────────────
# Placeholders for integration purposes within the pipeline
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'dwh_admin',
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

# ── Python Callable for State Logic ──────────────────────
def log_and_update_ab_initio_status_callable(**context):
    """
    Replicates the UC4 JOBI script logic:
    - Logs context execution details (Application, JobPlan, JobName).
    - Retrieves application status metadata.
    - Writes completion status to Airflow Variable Store.
    """
    # 1. Extract context variables mimicking UC4 macro functions
    application = "DWH"
    variable_key = "dw_adm_ab_initio_var"
    
    current_time_str = context['logical_date'].strftime('%H:%M:%S')
    current_date_str = context['logical_date'].strftime('%d.%m.%Y')
    
    jobplan_name = context['dag'].dag_id
    job_name = context['task'].task_id
    betr_job = f"{jobplan_name} -> {job_name}"
    
    # Simulate: GET_VAR (&VAR, 'STATUS_&APPLIKATION')
    # Fetch existing status dict or string if it exists; default to empty if not found
    try:
        current_var_value = Variable.get(variable_key, deserialize_json=True)
    except KeyError:
        current_var_value = {}
        
    status_application = current_var_value.get(f"STATUS_{application}", "UNKNOWN")
    status_fertig = f"fertig ({current_time_str} {current_date_str})"
    
    # 2. Print diagnostic output mimicking UC4 script console prints
    print(f"Der Prüfjob {job_name} läuft im Jobplan {jobplan_name}")
    print(f"Der Status für die Applikation {application} ist: {status_application} ({current_time_str} {current_date_str})")
    print(f"Die Ab Initio Verarbeitung ist fertig. Der Status wird auf {status_fertig} umgesetzt.")
    
    # 3. Simulate: PUT_VAR &VAR, '&BETRJOB', '&STATUS_FERTIG'
    # Update status map with the complete processing log flag
    if isinstance(current_var_value, dict):
        current_var_value[betr_job] = status_fertig
        Variable.set(variable_key, current_var_value, serialize_json=True)
    else:
        # Fallback to simple string variable if JSON store is not used
        Variable.set(variable_key, f"{betr_job} : {status_fertig}")

# ── DAG Definition ───────────────────────────────────────
with DAG(
    dag_id='dw_dwh_adm_pruefe_ab_initio_ende_inc',
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    doc_md=__doc__
) as dag:

    # ── Task: log_and_update_ab_initio_status ─────────────
    log_and_update_ab_initio_status = PythonOperator(
        task_id='log_and_update_ab_initio_status',
        python_callable=log_and_update_ab_initio_status_callable,
        provide_context=True
    )

    # ── Dependencies ─────────────────────────────────────────
    # This include task runs sequentially within its parent flow wrapper
    log_and_update_ab_initio_status