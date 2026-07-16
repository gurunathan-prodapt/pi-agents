from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.providers.google.cloud.operators.oracle import OracleOperator
from airflow.providers.google.cloud.operators.email import EmailOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models import Variable

# ── GCP Configuration (GLOBAL variables fetched via Airflow Variable with fallbacks) ────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── JOB-SPECIFIC Configuration ────────────────────────────────────
DEFAULT_NOTIFY_EMAIL = "dw-alerts@company.com"

# ── on_failure_callback stubs ─────────────────────────────
def on_failure_alarm(context):
    """
    Unified failure callback stub.
    Emulates the UC4 ON_FAILURE ACTION="NOTIFY_AND_ABORT".
    """
    task_id = context['task_instance'].task_id
    dag_id = context['task_instance'].dag_id
    exception = context.get('exception')
    print(f"ALERT: Task {task_id} in DAG {dag_id} failed with exception: {exception}")

# ── Default Args ─────────────────────────────────────────
default_args = {
    'owner': 'DW_TEAM',
    'depends_on_past': False,
    'email': [DEFAULT_NOTIFY_EMAIL],
    'email_on_failure': True,
    'retries': 3,
    'retry_delay': timedelta(seconds=60),
    'start_date': datetime(2024, 1, 1),
}

# ── DAG Definition ───────────────────────────────────────
dag = DAG(
    'retail_daily_workflow',
    default_args=default_args,
    description='Daily retail sales ETL pipeline (Migrated from UC4)',
    schedule_interval='0 2 * * *',  # Europe/London 02:00 Daily
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False
)

# ── Task: pre_check ──────────────────────────────────────
query_pre_check = """
    SELECT COUNT(*) FROM SOURCE_OPS.SALES_TXN
    WHERE TRUNC(TXN_DATETIME) = TO_DATE('{{ (execution_date - macros.timedelta(days=1)).strftime("%Y-%m-%d") }}','YYYY-MM-DD');
"""

retail_pre_check = OracleOperator(
    task_id='retail_pre_check',
    oracle_conn_id='oracle_dw_login',
    sql=query_pre_check,
    retries=2,
    retry_delay=timedelta(seconds=120),
    on_failure_callback=on_failure_alarm,
    dag=dag
)

# ── Task: stg_extract_north ──────────────────────────────
stg_extract_north_job = {
    "reference": {"project_id": GCP_PROJECT},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/load_daily_sales.py",
        "args": [
            "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "NORTH",
            "{{ var.value.get('batch_mode', 'DAILY') }}"
        ]
    }
}

retail_stg_extract_north = DataprocSubmitJobOperator(
    task_id='retail_stg_extract_north',
    job=stg_extract_north_job,
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT,
    on_failure_callback=on_failure_alarm,
    dag=dag
)

# ── Task: stg_extract_south ──────────────────────────────
stg_extract_south_job = {
    "reference": {"project_id": GCP_PROJECT},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/load_daily_sales.py",
        "args": [
            "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "SOUTH",
            "{{ var.value.get('batch_mode', 'DAILY') }}"
        ]
    }
}

retail_stg_extract_south = DataprocSubmitJobOperator(
    task_id='retail_stg_extract_south',
    job=stg_extract_south_job,
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT,
    on_failure_callback=on_failure_alarm,
    dag=dag
)

# ── Task: wait_for_finance_gl_close ──────────────────────
wait_for_finance_gl_close = ExternalTaskSensor(
    task_id='wait_for_finance_gl_close',
    external_dag_id='finance_daily_workflow',
    external_task_id='finance_daily_gl_close',
    allowed_states=['success'],
    check_existence=True,
    poke_interval=300,
    timeout=7200,
    dag=dag
)

# ── Task: product_master_load ────────────────────────────
product_master_load_job = {
    "reference": {"project_id": GCP_PROJECT},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/load_product_master.py",
        "args": [
            "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "3",
            "60"
        ]
    }
}

retail_product_master_load = DataprocSubmitJobOperator(
    task_id='retail_product_master_load',
    job=product_master_load_job,
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT,
    on_failure_callback=on_failure_alarm,
    dag=dag
)

# ── Task: abinitio_transform ─────────────────────────────
abinitio_transform_job = {
    "reference": {"project_id": GCP_PROJECT},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/sales_rollup.py",
        "args": [
            "--load-date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "--region-code", "ALL",
            "--parallelism", "4"
        ]
    }
}

retail_abinitio_transform = DataprocSubmitJobOperator(
    task_id='retail_abinitio_transform',
    job=abinitio_transform_job,
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT,
    on_failure_callback=on_failure_alarm,
    dag=dag
)

# ── Task: spark_aggregation ──────────────────────────────
spark_aggregation_job = {
    "reference": {"project_id": GCP_PROJECT},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "spark_job": {
        "main_jar_file_uri": f"gs://{GCS_BUCKET}/jars/retail-etl-assembly.jar",
        "main_class": "com.company.retail.SalesAggregation",
        "args": [
            "--load-date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "--env", "{{ var.value.get('env', 'PROD') }}"
        ],
        "properties": {
            "spark.executor.instances": "8",
            "spark.executor.memory": "4g",
            "spark.submit.deployMode": "cluster"
        }
    }
}

retail_spark_aggregation = DataprocSubmitJobOperator(
    task_id='retail_spark_aggregation',
    job=spark_aggregation_job,
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT,
    on_failure_callback=on_failure_alarm,
    dag=dag
)

# ── Task: data_quality_check ─────────────────────────────
data_quality_check_job = {
    "reference": {"project_id": GCP_PROJECT},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/retail_data_quality.py",
        "args": [
            "--load-date", "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
            "--env", "{{ var.value.get('env', 'PROD') }}",
            "--notify-email", DEFAULT_NOTIFY_EMAIL
        ]
    }
}

retail_data_quality_check = DataprocSubmitJobOperator(
    task_id='retail_data_quality_check',
    job=data_quality_check_job,
    region=DATAPROC_REGION,
    project_id=GCP_PROJECT,
    on_failure_callback=on_failure_alarm,
    dag=dag
)

# ── Task: completion_notify ──────────────────────────────
# Send complete status notification exactly preserving legacy strings
retail_completion_notify_email = EmailOperator(
    task_id='retail_completion_notify_email',
    to=DEFAULT_NOTIFY_EMAIL,
    subject="[ETL-OK] Retail Daily Load {{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
    html_content="RETAIL_DAILY_WORKFLOW completed for LOAD_DATE={{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}",
    trigger_rule='all_done',
    dag=dag
)

retail_completion_publish_event = PubSubPublishMessageOperator(
    task_id='retail_completion_publish_event',
    topic='retail-daily-complete-topic',
    messages=[{
        'data': b'RETAIL_DAILY_COMPLETE',
        'attributes': {
            'date': "{{ (execution_date - macros.timedelta(days=1)).strftime('%Y-%m-%d') }}"
        }
    }],
    trigger_rule='all_done',
    dag=dag
)

# ── Dependencies ─────────────────────────────────────────
retail_pre_check >> [retail_stg_extract_north, retail_stg_extract_south]

[retail_stg_extract_north, retail_stg_extract_south, wait_for_finance_gl_close] >> retail_product_master_load

retail_product_master_load >> retail_abinitio_transform >> retail_spark_aggregation >> retail_data_quality_check

retail_data_quality_check >> [retail_completion_notify_email, retail_completion_publish_event]