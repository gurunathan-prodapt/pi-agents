from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# GLOBAL (Environment-Wide) Variables
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var=None)
DATAPROC_REGION = Variable.get("DATAPROC_REGION", default_var=None)
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER", default_var=None)
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=None)

def dwh_adm_job_monitor_start(**context):
    """
    Python implementation of the DW.DWH_ADM_JOB_MONITOR_START JOBI.
    This function contains the translated script logic from the UC4 JOBI.
    """
    # Sourced at runtime from the Airflow execution context
    dag = context.get('dag')
    admjp = dag.dag_id if dag else ""
    
    task = context.get('task')
    admjob = task.task_id if task else ""
    
    dag_run = context.get('dag_run')
    admnrjob = dag_run.run_id if dag_run else ""
    
    dwh_job_kennung = ""

    # Protokoll nur wenn Aufruf aus Jobplan!
    if admjp and admjp.strip():
        # Commented log in source, but printed as requested by Target File Plan
        print(f"Job {admjob} mit RNR {admnrjob} gestartet aus {admjp}")

        # Sourced as a shared runtime configuration lookup
        try:
            dwh_monitored_jps = Variable.get("DW_DWH_MONITORED_JPS", deserialize_json=True)
        except Exception:
            dwh_monitored_jps = {}

        should_monitor = False
        
        if isinstance(dwh_monitored_jps, dict):
            for key, val in dwh_monitored_jps.items():
                if val == "J":
                    if key == admjp or key == "ALL":
                        should_monitor = True
                        break
        elif isinstance(dwh_monitored_jps, list):
            for item in dwh_monitored_jps:
                if isinstance(item, list) and len(item) >= 2:
                    key, val = item[0], item[1]
                elif isinstance(item, dict):
                    key = item.get("key") or item.get("name") or list(item.keys())[0]
                    val = item.get("value") or item.get("val") or item.get(key)
                else:
                    continue
                
                if val == "J":
                    if key == admjp or key == "ALL":
                        should_monitor = True
                        break

        if should_monitor:
            # Eintragen in Variablencontainer!
            print(f"Added {admjob} with {admnrjob}")
            
            try:
                running_jobs = Variable.get("DW_DWH_RUNNING_JOBS", deserialize_json=True)
            except Exception:
                running_jobs = {}
                
            if not isinstance(running_jobs, dict):
                running_jobs = {}
                
            running_jobs[admjob] = admnrjob
            Variable.set("DW_DWH_RUNNING_JOBS", running_jobs, serialize_json=True)


# Default Args
DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 0,
}

# DAG Definition (Template showing JOBI integration)
with DAG(
    dag_id='dw_dwh_adm_job_monitor_start_template',
    default_args=DEFAULT_ARGS,
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['monitoring', 'template'],
) as dag:

    # Task: dwh_adm_job_monitor_start
    t_dwh_adm_job_monitor_start = PythonOperator(
        task_id='dwh_adm_job_monitor_start',
        python_callable=dwh_adm_job_monitor_start,
        provide_context=True,
    )

    t_dwh_adm_job_monitor_start