"""
d_al_is_spaufruf_p0.py
Target migrated Airflow DAG representing the legacy d_alis_spaufruf_p0.sql pipeline.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Default environment variable configurations with defaults
GCP_CONN_ID = Variable.get("gcp_conn_id", default_var="google_cloud_default")
GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="your-gcp-project-id")
BQ_DATASET = Variable.get("bq_dataset", default_var="your_dataset")
DEFAULT_RETRIES = int(Variable.get("dag_default_retries", default_var="1"))
DEFAULT_RETRY_DELAY_MINS = int(Variable.get("dag_default_retry_delay_mins", default_var="5"))

# Default arguments for the pipeline
DEFAULT_ARGS = {
    'owner': 'airflow-migration-team',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': DEFAULT_RETRIES,
    'retry_delay': timedelta(minutes=DEFAULT_RETRY_DELAY_MINS),
}

# Dynamic procedure call string construction
# Target structure: dag_run.conf = {"sp_name": "my_procedure", "sp_args": "123, 'dynamic_string'"}
sql_query = f"""
-- Translated from legacy Oracle EXEC Wrapper
CALL `{GCP_PROJECT_ID}.{BQ_DATASET}.{{{{ dag_run.conf.get('sp_name', 'default_procedure') }}}}`(
    {{{{ dag_run.conf.get('sp_args', '') }}}}
);
"""

with DAG(
    dag_id='d_al_is_spaufruf_p0',
    default_args=DEFAULT_ARGS,
    description='Executes dynamically targeted BigQuery Stored Procedures mimicking legacy d_alis_spaufruf_p0',
    schedule_interval=None,  # Typically triggered on-demand or by an external scheduler (UC4)
    catchup=False,
    tags=['legacy-migration', 'oracle-sp-wrapper'],
) as dag:

    # 1. Pipeline entrypoint
    start_pipeline = EmptyOperator(task_id='start_pipeline')

    # 2. Dynamic BigQuery Stored Procedure Task
    # Uses dynamic parameters from downstream triggers (dag_run.conf) mimicking &1 and &2
    execute_stored_procedure = BigQueryExecuteQueryOperator(
        task_id='execute_migrated_stored_procedure',
        sql=sql_query,
        gcp_conn_id=GCP_CONN_ID,
        use_legacy_sql=False,
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED',
    )

    # 3. Pipeline endpoint
    end_pipeline = EmptyOperator(task_id='end_pipeline')

    # Task Execution Sequence
    start_pipeline >> execute_stored_procedure >> end_pipeline