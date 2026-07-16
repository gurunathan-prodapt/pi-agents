"""
DAG: crm_weekly_workflow
Author: CRM ETL Team
Description: Orchestrates Dataproc PySpark jobs processing CRM, retail, and financial
             ledger datasets, with failure alerting and cross-pipeline GCS sensors.
"""

from datetime import datetime, timedelta
import hashlib
import uuid
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.trigger_rule import TriggerRule

# ── Environment & Config Retrieval ────────────────────────────────────
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")
ENV = Variable.get("ENV", default_var="PROD")
NOTIFY_EMAIL = Variable.get("NOTIFY_EMAIL", default_var="crm-etl@company.com")

# ── Dynamic UUID Filter Definition ─────────────────────────────────────
def generate_deterministic_uuid(run_id: str, task_id: str) -> str:
    """Generates a reproducible UUID based on run context to prevent duplicate jobs."""
    hasher = hashlib.md5(f"{run_id}-{task_id}".encode("utf-8"))
    return str(uuid.UUID(bytes=hasher.digest()[:16]))

# ── Alarm and SLA Breach Callbacks ─────────────────────────────────────
def on_failure_alarm(context):
    """
    Sends execution-level failure alarms back to the notification channels.
    Replicates the literal text of the <NOTIFICATIONS> block from legacy XML.
    """
    execution_date = context["ds"]
    subject = f"[CRITICAL] CRM_WEEKLY_WORKFLOW FAILED for {execution_date}"
    body = f"CRITICAL failure detected on task {context['task_instance'].task_id}."
    
    email_op = EmailOperator(
        task_id="failure_alert_email",
        to=NOTIFY_EMAIL,
        subject=subject,
        html_content=f"<p>{body}</p>",
    )
    email_op.execute(context=context)

def on_sla_miss(dag, task_list, blocking_task_list, slas, blocking_slas):
    """
    Sends SLA breach alarms back to the notification channels.
    Replicates the literal text of the <NOTIFICATIONS> block from legacy XML.
    """
    subject = "[SLA] CRM_WEEKLY_WORKFLOW exceeding 5h window"
    body = f"SLA breach detected for tasks: {task_list}. Blocking tasks: {blocking_task_list}."
    
    email_op = EmailOperator(
        task_id="sla_alert_email",
        to=NOTIFY_EMAIL,
        subject=subject,
        html_content=f"<p>{body}</p>",
    )
    email_op.execute(context={})

# ── Default Task Arguments ─────────────────────────────────────────────
default_args = {
    "owner": "CRM_ETL_TEAM",
    "start_date": datetime(2025, 1, 1, tzinfo=pendulum.timezone("Europe/London")),
    "email_on_failure": True,
    "email": NOTIFY_EMAIL,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

# ── DAG Definition ─────────────────────────────────────────────────────
dag = DAG(
    dag_id="crm_weekly_workflow",
    schedule="0 4 * * 7",  # Every Sunday at 04:00 AM Europe/London
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    sla=timedelta(hours=5),
    sla_miss_callback=on_sla_miss,
    user_defined_filters={"generate_uuid": generate_deterministic_uuid},
)

# ── GCS Sensors ────────────────────────────────────────────────────────
crm_wait_finance_event = GCSObjectExistenceSensor(
    task_id="crm_wait_finance_event",
    bucket=GCS_BUCKET,
    object="finance/finance_daily.json",
    timeout=14400,  # 4 hours
    poke_interval=300,
    dag=dag,
)

crm_wait_retail_event = GCSObjectExistenceSensor(
    task_id="crm_wait_retail_event",
    bucket=GCS_BUCKET,
    object="sales/retail_daily.json",
    timeout=7200,  # 2 hours
    poke_interval=150,
    dag=dag,
)

# ── Segment Extract Jobs (VIP, RETAIL, WHOLESALE) ──────────────────────
def get_pyspark_job_config(segment_name: str) -> dict:
    return {
        "reference": {"project_id": GCP_PROJECT},
        "placement": {"cluster_name": DATAPROC_CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/process_customer_data.py",
            "args": [
                "--run-date", "{{ ds }}",
                "--segment", segment_name,
                "--env", ENV,
                "--batch-size", "5000"
            ]
        }
    }

crm_customer_extract_vip = DataprocSubmitJobOperator(
    task_id="crm_customer_extract_vip",
    project_id=GCP_PROJECT,
    region=GCP_REGION,
    job=get_pyspark_job_config("VIP"),
    job_id="crm-vip-ext-{{ ds_nodash }}-{{ run_id | generate_uuid('crm_customer_extract_vip') }}",
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

crm_customer_extract_retail = DataprocSubmitJobOperator(
    task_id="crm_customer_extract_retail",
    project_id=GCP_PROJECT,
    region=GCP_REGION,
    job=get_pyspark_job_config("RETAIL"),
    job_id="crm-retail-ext-{{ ds_nodash }}-{{ run_id | generate_uuid('crm_customer_extract_retail') }}",
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

crm_customer_extract_wholesale = DataprocSubmitJobOperator(
    task_id="crm_customer_extract_wholesale",
    project_id=GCP_PROJECT,
    region=GCP_REGION,
    job=get_pyspark_job_config("WHOLESALE"),
    job_id="crm-wholesale-ext-{{ ds_nodash }}-{{ run_id | generate_uuid('crm_customer_extract_wholesale') }}",
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# ── CRM Transformation (Ported Ab Initio logic) ───────────────────────
pyspark_job_abinitio_transform = {
    "reference": {"project_id": GCP_PROJECT},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/crm_customer_scoring.py",
        "args": [
            "--run-date", "{{ ds }}",
            "--customer-segment", "ALL",
            "--parallelism", "4"
        ]
    }
}

crm_abinitio_transform = DataprocSubmitJobOperator(
    task_id="crm_abinitio_transform",
    project_id=GCP_PROJECT,
    region=GCP_REGION,
    job=pyspark_job_abinitio_transform,
    job_id="crm-abinitio-{{ ds_nodash }}-{{ run_id | generate_uuid('crm_abinitio_transform') }}",
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# ── Spark Segmentation (Ported Scala Logic) ───────────────────────────
pyspark_job_segmentation = {
    "reference": {"project_id": GCP_PROJECT},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/customer_segmentation.py",
        "args": [
            "--run-date", "{{ ds }}",
            "--env", ENV
        ]
    }
}

crm_spark_segmentation = DataprocSubmitJobOperator(
    task_id="crm_spark_segmentation",
    project_id=GCP_PROJECT,
    region=GCP_REGION,
    job=pyspark_job_segmentation,
    job_id="crm-spark-segmentation-{{ ds_nodash }}-{{ run_id | generate_uuid('crm_spark_segmentation') }}",
    on_failure_callback=on_failure_alarm,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# ── Lineage Tracker ───────────────────────────────────────────────────
pyspark_job_lineage = {
    "reference": {"project_id": GCP_PROJECT},
    "placement": {"cluster_name": DATAPROC_CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/crm_lineage_tracker.py",
        "args": [
            "--run-date", "{{ ds }}",
            "--env", ENV
        ]
    }
}

crm_python_lineage = DataprocSubmitJobOperator(
    task_id="crm_python_lineage",
    project_id=GCP_PROJECT,
    region=GCP_REGION,
    job=pyspark_job_lineage,
    job_id="crm-lineage-{{ ds_nodash }}-{{ run_id | generate_uuid('crm_python_lineage') }}",
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# ── Completion Notification ───────────────────────────────────────────
crm_completion_notify = EmailOperator(
    task_id="crm_completion_notify",
    to=NOTIFY_EMAIL,
    subject="[CRM-OK] Weekly CRM Load {{ ds }}",
    html_content="<p>CRM_WEEKLY_WORKFLOW completed for RUN_DATE={{ ds }}</p>",
    trigger_rule=TriggerRule.ALL_DONE,  
    dag=dag,
)

# ── Dependency Lineage Tree ───────────────────────────────────────────
crm_wait_finance_event >> crm_wait_retail_event

crm_wait_retail_event >> [
    crm_customer_extract_vip,
    crm_customer_extract_retail,
    crm_customer_extract_wholesale
]

[
    crm_customer_extract_vip,
    crm_customer_extract_retail,
    crm_customer_extract_wholesale
] >> crm_abinitio_transform

crm_abinitio_transform >> [crm_spark_segmentation, crm_python_lineage]

[crm_spark_segmentation, crm_python_lineage] >> crm_completion_notify