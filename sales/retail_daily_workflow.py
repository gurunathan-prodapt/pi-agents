"""
retail_daily_workflow.py
Google Cloud Composer DAG orchestrating the Daily Retail Sales ETL sequence.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.oracle import OracleOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule

# Import modular helper packages
from gcp_dataproc_helpers import build_pyspark_job_config
from pipeline_notifications import on_failure_alarm, publish_completion_event

# ── 1. ENVIRONMENT CONFIGURATION POLICY ────────────────────────────────────────
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT", Variable.get("gcp_project", "your-gcp-project-id"))
DATAPROC_REGION = os.environ.get("DATAPROC_REGION", Variable.get("dataproc_region", "europe-west1"))
CLUSTER_NAME = os.environ.get("DATAPROC_CLUSTER", Variable.get("dataproc_cluster", "retail-dataproc-cluster"))
GCS_BUCKET = os.environ.get("GCS_BUCKET", Variable.get("gcs_bucket", "retail-data-warehouse-bucket"))

# ── 2. DEFAULT ARGUMENTS & METADATA INIT ──────────────────────────────────────
default_args = {
    'owner': 'DW_TEAM',
    'start_date': datetime(2024, 1, 10),
    'email': ['dw-alerts@company.com'],
    'email_on_failure': True,
    'on_failure_callback': on_failure_alarm,
}

dag = DAG(
    dag_id='retail_daily_workflow',
    default_args=default_args,
    schedule_interval='0 2 * * *',           # Europe/London Daily at 02:00
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    dagrun_timeout=timedelta(minutes=240),   # Source SLA mapping: 240 mins
)

# ── 3. WORKFLOW TASK DEFINITIONS ──────────────────────────────────────────────

# Job 1: Source POS Endpoint Verification Check
retail_pre_check = OracleOperator(
    task_id='retail_pre_check',
    oracle_conn_id='oracle_dw_connection',
    sql="""
        SELECT COUNT(*) FROM SOURCE_OPS.SALES_TXN
        WHERE TRUNC(TXN_DATETIME) = TO_DATE('{{ ds }}', 'YYYY-MM-DD')
    """,
    retries=2,
    retry_delay=timedelta(seconds=120),
    dag=dag,
)

# Job 2a: North Region Extract (PySpark Submit Job)
retail_stg_extract_north = DataprocSubmitJobOperator(
    task_id='retail_stg_extract_north',
    job=build_pyspark_job_config(
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        bucket_name=GCS_BUCKET,
        script_name="load_daily_sales.py",
        args=["{{ ds }}", "NORTH"]
    ),
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT_ID,
    retries=3,
    retry_delay=timedelta(seconds=60),
    dag=dag,
)

# Job 2b: South Region Extract (PySpark Submit Job)
retail_stg_extract_south = DataprocSubmitJobOperator(
    task_id='retail_stg_extract_south',
    job=build_pyspark_job_config(
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        bucket_name=GCS_BUCKET,
        script_name="load_daily_sales.py",
        args=["{{ ds }}", "SOUTH"]
    ),
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT_ID,
    retries=3,
    retry_delay=timedelta(seconds=60),
    dag=dag,
)

# Cross-Domain Synchronization Barrier
finance_gl_close_sensor = ExternalTaskSensor(
    task_id='finance_gl_close_sensor',
    external_dag_id='finance_daily_workflow',
    external_task_id='finance_daily_gl_close',
    allowed_states=['success'],
    check_existence=True,
    poke_interval=300,
    timeout=7200,
    dag=dag,
)

# Job 3: SCD Type 2 Product Master Update Processing
retail_product_master_load = DataprocSubmitJobOperator(
    task_id='retail_product_master_load',
    job=build_pyspark_job_config(
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        bucket_name=GCS_BUCKET,
        script_name="load_product_master.py",
        args=["{{ ds }}"]
    ),
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT_ID,
    retries=3,
    retry_delay=timedelta(seconds=60),
    dag=dag,
)

# Job 4: Rollup Transformation Sequence (Ab Initio migration equivalent)
retail_abinitio_transform = DataprocSubmitJobOperator(
    task_id='retail_abinitio_transform',
    job=build_pyspark_job_config(
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        bucket_name=GCS_BUCKET,
        script_name="sales_rollup.py",
        args=["--load-date", "{{ ds }}"]
    ),
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT_ID,
    retries=0,
    dag=dag,
)

# Job 5: PySpark Core Analytical Aggregation Business Logic
retail_spark_aggregation = DataprocSubmitJobOperator(
    task_id='retail_spark_aggregation',
    job=build_pyspark_job_config(
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        bucket_name=GCS_BUCKET,
        script_name="sales_aggregation.py",
        args=["--load-date", "{{ ds }}", "--env", "PROD"]
    ),
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT_ID,
    retries=0,
    dag=dag,
)

# Job 6: Python Data Quality Assurance Checks (Non-Blocking State)
retail_data_quality_check = DataprocSubmitJobOperator(
    task_id='retail_data_quality_check',
    job=build_pyspark_job_config(
        project_id=GCP_PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        bucket_name=GCS_BUCKET,
        script_name="retail_data_quality.py",
        args=["--load-date", "{{ ds }}", "--env", "PROD", "--notify-email", "dw-alerts@company.com"]
    ),
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT_ID,
    retries=0,
    on_failure_callback=None,  # DQ Warning alerts only; avoids triggering critical pipeline abort
    dag=dag,
)

# Job 7: Verbatim Completion Event Publisher Task
retail_completion_notify = PythonOperator(
    task_id='retail_completion_notify',
    python_callable=publish_completion_event,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of DQ success/failure
    dag=dag,
)

# Email Notification Pipeline Task
send_completion_email = EmailOperator(
    task_id='send_completion_email',
    to='dw-alerts@company.com',
    subject='[ETL-OK] Retail Daily Load {{ ds }}',
    html_content="""
    RETAIL_DAILY_WORKFLOW completed for LOAD_DATE={{ ds }}
    """,
    trigger_rule=TriggerRule.ALL_DONE,  # Maintain output continuity
    dag=dag,
)

# ── 4. DAG TOPOLOGY STRUCTURE ──────────────────────────────────────────────────

# Phase 1: Source Validation Check
retail_pre_check >> [retail_stg_extract_north, retail_stg_extract_south]

# Phase 2: Dependency Synchronization Barrier Integration
[retail_stg_extract_north, retail_stg_extract_south, finance_gl_close_sensor] >> retail_product_master_load

# Phase 3: Core Analytical Transformation Pipeline Execution Chain
(
    retail_product_master_load 
    >> retail_abinitio_transform 
    >> retail_spark_aggregation 
    >> retail_data_quality_check 
    >> retail_completion_notify 
    >> send_completion_email
)