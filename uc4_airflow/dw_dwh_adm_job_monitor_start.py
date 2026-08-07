"""
dw_dwh_adm_job_monitor_start.py

This module contains the migration target for the UC4 Include asset
DW.DWH_ADM_JOB_MONITOR_START. It converts the UC4 JOBI script logic into a
reusable Python helper function that queries BigQuery metadata tables to track
active running jobs, and provides an Airflow DAG wrapper.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

# ==============================================================================
# ── Environment Configuration ──────────────────────────────────────────────────
# ==============================================================================
# Global variables are sourced dynamically via Airflow Variable or Environment.
# Falling back to os.environ avoids hardcoded literals or prose placeholders.
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=os.environ.get("GCP_PROJECT"))
BQ_DATASET = Variable.get("BQ_DATASET", default_var=os.environ.get("BQ_DATASET", "dw_metadata"))

# Job-specific BigQuery table paths
if GCP_PROJECT:
    MONITORED_JPS_TABLE = f"{GCP_PROJECT}.{BQ_DATASET}.dwh_monitored_jps"
    RUNNING_JOBS_TABLE = f"{GCP_PROJECT}.{BQ_DATASET}.dwh_running_jobs"
else:
    MONITORED_JPS_TABLE = f"{BQ_DATASET}.dwh_monitored_jps"
    RUNNING_JOBS_TABLE = f"{BQ_DATASET}.dwh_running_jobs"

# ==============================================================================
# ── Core Logic Function ───────────────────────────────────────────────────────
# ==============================================================================
def dwh_adm_job_monitor_start(context: dict = None, dag_id: str = None, task_id: str = None, run_id: str = None) -> None:
    """
    Registers a started job in the running jobs registry if its parent job plan is monitored.
    
    This replaces UC4 Include DW.DWH_ADM_JOB_MONITOR_START.
    """
    # Dynamically resolve values from context or arguments
    admjp = dag_id or (context['dag'].dag_id if context and 'dag' in context else None)
    admjob = task_id or (context['task_instance'].task_id if context and 'task_instance' in context else None) or admjp
    admnrjob = run_id or (context['run_id'] if context and 'run_id' in context else None)

    # Initialized as in the source UC4 script
    dwh_job_kennung = ""

    # Protocol only if parent job plan (dag_id) is not empty
    if admjp and admjp.strip() != "":
        # PRINT "Job &ADMJOB mit RNR &ADMNRJOB gestartet aus &ADMJP"
        # Output/Print literal logging rule: Keep original German message character-for-character
        print(f"Job {admjob} mit RNR {admnrjob} gestartet aus {admjp}")

        # Initialize BigQuery client
        client = bigquery.Client(project=GCP_PROJECT)

        # Query monitored job plans
        query = f"SELECT * FROM `{MONITORED_JPS_TABLE}`"
        query_job = client.query(query)
        rows = query_job.result()

        for row in rows:
            # Sourced from first and second columns
            admgb = row[0]
            admwert = row[1]

            if admwert == "J":
                if admgb == admjp or admgb == "ALL":
                    # Output/Print literal logging rule: Keep original character-for-character
                    print(f"Added {admjob} with {admnrjob}")

                    # Perform MERGE query to update/insert the running job run ID
                    merge_query = f"""
                    MERGE INTO `{RUNNING_JOBS_TABLE}` T
                    USING (SELECT @job_name AS job_name, @run_id AS run_id) S
                    ON T.job_name = S.job_name
                    WHEN MATCHED THEN UPDATE SET run_id = S.run_id
                    WHEN NOT MATCHED THEN INSERT (job_name, run_id) VALUES (S.job_name, S.run_id)
                    """
                    job_config = bigquery.QueryJobConfig(
                        query_parameters=[
                            bigquery.ScalarQueryParameter("job_name", "STRING", admjob),
                            bigquery.ScalarQueryParameter("run_id", "STRING", admnrjob),
                        ]
                    )
                    merge_job = client.query(merge_query, job_config=job_config)
                    merge_job.result()

# ==============================================================================
# ── Default Args ──────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# ── DAG Definition ────────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id='dw_dwh_adm_job_monitor_start',
    default_args=DEFAULT_ARGS,
    description='Migration stub and logic implementation for UC4 Include asset DW.DWH_ADM_JOB_MONITOR_START',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=['migrated_uc4', 'jobi_include'],
) as dag: 

    # Task executes the tracking function using PythonOperator
    dwh_adm_job_monitor_start_include = PythonOperator(
        task_id='dwh_adm_job_monitor_start_include',
        python_callable=dwh_adm_job_monitor_start,
        provide_context=True,
    )