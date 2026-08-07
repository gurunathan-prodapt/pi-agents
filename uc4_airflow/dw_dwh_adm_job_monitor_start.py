from datetime import datetime
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator

# ==============================================================================
# Shared JOBI Utility Code (DW.DWH_ADM_JOB_MONITOR_START)
# ==============================================================================

def execute_job_monitor_start(context: dict = None, **kwargs) -> str:
    """
    Python utility function representing the migrated DW.DWH_ADM_JOB_MONITOR_START JOBI.
    This should be called at the beginning of DAGs or via task pre_execute hooks.
    """
    from airflow.models import Variable
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

    # Environment-specific values (sourced at runtime to avoid top-level parsing execution)
    gcp_project_id = Variable.get("GCP_PROJECT")
    bq_dataset = Variable.get("BQ_DATASET")
    bq_connection_id = Variable.get("BQ_CONNECTION_ID", default_var="google_cloud_default")

    monitored_jps_table = f"{gcp_project_id}.{bq_dataset}.dwh_monitored_jps"
    running_jobs_table = f"{gcp_project_id}.{bq_dataset}.dwh_running_jobs"

    # Merge context and kwargs
    ctx = context or kwargs

    dag_run = ctx.get('dag_run')
    task_instance = ctx.get('ti')

    admjp = dag_run.dag_id if dag_run else "unknown_dag"
    admjob = task_instance.task_id if task_instance else "unknown_task"
    admnrjob = dag_run.run_id if dag_run else "unknown_run"
    dwh_job_kennung = ""

    if admjp and admjp.strip() != "":
        # logging.info(f"Job {admjob} mit RNR {admnrjob} gestartet aus {admjp}")

        bq_hook = BigQueryHook(gcp_conn_id=bq_connection_id)

        sql_check = f"""
            SELECT dag_id, monitoring_enabled_flag 
            FROM `{monitored_jps_table}` 
            WHERE dag_id = '{admjp}' OR dag_id = 'ALL'
        """

        logging.info(f"Checking monitoring status for DAG {admjp} in {monitored_jps_table}")
        
        is_monitored = False
        try:
            records = bq_hook.get_records(sql=sql_check)
            for row in records:
                dag_val = row[0]
                enabled_val = row[1]
                if enabled_val == "J":
                    if dag_val == admjp or dag_val == "ALL":
                        is_monitored = True
                        break
        except Exception as e:
            logging.warning(f"Could not query {monitored_jps_table}: {e}. Defaulting to non-monitored.")
            is_monitored = False

        if is_monitored:
            logging.info(f"Added {admjob} with {admnrjob}")
            
            sql_insert = f"""
                INSERT INTO `{running_jobs_table}` (job_name, run_id)
                VALUES ('{admjob}', '{admnrjob}')
            """
            logging.info(f"Registering job start in {running_jobs_table}")
            bq_hook.run_query(sql=sql_insert, use_legacy_sql=False)
        else:
            logging.info(f"DAG {admjp} is not configured for monitoring or check returned False.")

    return admnrjob

# ==============================================================================
# Custom Base Operator Mixin Example
# ==============================================================================

class MonitoredOperatorMixin:
    """
    A mixin to automatically trigger DW.DWH_ADM_JOB_MONITOR_START 
    pre-execution on any Operator.
    """
    def pre_execute(self, context):
        super().pre_execute(context)
        execute_job_monitor_start(context)

# ==============================================================================
# Test / Stub DAG Definition
# ==============================================================================

default_args = {
    'owner': 'data_engineering',
    'retries': 0,
}

with DAG(
    dag_id="dw_dwh_adm_job_monitor_start_test",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    default_args=default_args,
    doc_md="""
    Test/Stub DAG to verify the migrated DW.DWH_ADM_JOB_MONITOR_START utility logic.
    Originally used as a shared monitoring/audit startup script block in UC4.
    """
) as dag:

    test_monitor_start = PythonOperator(
        task_id="test_monitor_start",
        python_callable=execute_job_monitor_start
    )