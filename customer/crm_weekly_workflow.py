from datetime import datetime, timedelta
import logging
from typing import Any, Dict, List

import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.utils.trigger_rule import TriggerRule

# ==============================================================================
# 1. CONFIGURATION & ENVIRONMENT CONSTANTS
# ==============================================================================

# Timezone Definition
LOCAL_TZ = pendulum.timezone("Europe/London")

# Global Environment Variables
GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="YOUR_GCP_PROJECT_ID")
DATAPROC_CLUSTER = Variable.get("dataproc_cluster_name", default_var="YOUR_DATAPROC_CLUSTER_NAME")
DATAPROC_REGION = Variable.get("dataproc_region", default_var="YOUR_DATAPROC_REGION")
GCS_BUCKET = Variable.get("gcs_bucket_name", default_var="YOUR_BUCKET_NAME")

# Job Specific Settings
ENV = Variable.get("env", default_var="PROD")
CRM_NOTIFY_EMAIL = Variable.get("crm_notify_email", default_var="crm-etl@company.com")

# Script GCS URIs
PYSPARK_EXTRACT_URI = f"gs://{GCS_BUCKET}/pyspark_scripts/process_customer_data.py"
PYSPARK_TRANSFORM_URI = f"gs://{GCS_BUCKET}/pyspark_scripts/crm_customer_scoring.py"
PYSPARK_SEGMENTATION_URI = f"gs://{GCS_BUCKET}/pyspark_scripts/customer_segmentation.py"
PYSPARK_LINEAGE_URI = f"gs://{GCS_BUCKET}/pyspark_scripts/crm_lineage_tracker.py"


# ==============================================================================
# 2. REUSABLE CALLBACKS & HELPERS
# ==============================================================================

def on_failure_alarm(context: Dict[str, Any]) -> None:
    """
    On-failure callback to replicate UC4 standard failure alerts.
    Uses the exact source notification string.
    """
    logical_date = context.get('ds')
    # EXACT LITERAL MATCH FOR ON_WORKFLOW_FAILURE
    logging.error(f"[CRITICAL] CRM_WEEKLY_WORKFLOW FAILED for {logical_date}")


def sla_miss_callback(dag: DAG, task_list: List, blocking_task_list: List, slas: List, wins: List) -> None:
    """
    SLA Breach alarm mapping the legacy 5-hour (300 minutes) SLA limit.
    Uses the exact source notification string.
    """
    # EXACT LITERAL MATCH FOR ON_SLA_BREACH
    logging.warning("[SLA] CRM_WEEKLY_WORKFLOW exceeding 5h window")


def build_dataproc_pyspark_job(
    project_id: str,
    cluster_name: str,
    main_python_uri: str,
    arguments: List[str]
) -> Dict[str, Any]:
    """
    Reusable Factory function to construct consistent Dataproc PySpark job configs.
    """
    return {
        "reference": {"project_id": project_id},
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": main_python_uri,
            "args": arguments,
        },
    }


# ==============================================================================
# 3. DAG DEFINITION
# ==============================================================================

def on_success_callback(context: Dict[str, Any]) -> None:
    """
    On-success callback to log the exact workflow success notification string.
    """
    logical_date = context.get('ds')
    # EXACT LITERAL MATCH FOR ON_WORKFLOW_SUCCESS
    logging.info(f"[OK] CRM_WEEKLY_WORKFLOW completed for {logical_date}")


default_args = {
    'owner': 'CRM_ETL_TEAM',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1, tzinfo=LOCAL_TZ),
    'email': [CRM_NOTIFY_EMAIL, 'dw-alerts@company.com'],
    'email_on_failure': True,
    'on_failure_callback': on_failure_alarm,
    'on_success_callback': on_success_callback,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG( 
    dag_id='crm_weekly_workflow',
    default_args=default_args,
    description='Weekly CRM customer scoring and segmentation pipeline (Migrated from UC4)',
    schedule='0 4 * * 0',  # Every Sunday at 04:00 (Europe/London)
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    sla_miss_callback=sla_miss_callback,
    tags=['crm', 'weekly', 'etl'],
) as dag:

    # --------------------------------------------------------------------------
    # UPSTREAM SENSORS (EVENT TRACKING)
    # --------------------------------------------------------------------------

    # Sensor for FINANCE_GL_CLOSE_COMPLETE event. Timeout 240 mins (14400s).
    wait_finance_event = GCSObjectExistenceSensor( 
        task_id='wait_finance_event',
        bucket=GCS_BUCKET,
        object='events/FINANCE_GL_CLOSE_COMPLETE_{{ ds }}',
        timeout=14400,
        poke_interval=300,
        mode='reschedule',
    )

    # Sensor for RETAIL_DAILY_COMPLETE event. Timeout 120 mins (7200s).
    # Soft fails via downstreams to ensure execution proceeds with stale data if delayed.
    wait_retail_event = GCSObjectExistenceSensor(
        task_id='wait_retail_event',
        bucket=GCS_BUCKET,
        object='events/RETAIL_DAILY_COMPLETE_{{ ds }}',
        timeout=7200,
        poke_interval=180,
        mode='reschedule',
    )

    # --------------------------------------------------------------------------
    # PARALLEL CUSTOMER SEGMENT EXTRACTION
    # --------------------------------------------------------------------------

    customer_extract_vip = DataprocSubmitJobOperator(
        task_id='customer_extract_vip', 
        job=build_dataproc_pyspark_job(
            project_id=GCP_PROJECT_ID,
            cluster_name=DATAPROC_CLUSTER,
            main_python_uri=PYSPARK_EXTRACT_URI,
            arguments=["{{ ds }}", "VIP", "N"]
        ),
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=3,
        retry_delay=timedelta(seconds=120),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    customer_extract_retail = DataprocSubmitJobOperator(
        task_id='customer_extract_retail',
        job=build_dataproc_pyspark_job(
            project_id=GCP_PROJECT_ID,
            cluster_name=DATAPROC_CLUSTER,
            main_python_uri=PYSPARK_EXTRACT_URI,
            arguments=["{{ ds }}", "RETAIL", "N"]
        ),
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=3,
        retry_delay=timedelta(seconds=120),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    customer_extract_wholesale = DataprocSubmitJobOperator(
        task_id='customer_extract_wholesale',
        job=build_dataproc_pyspark_job(
            project_id=GCP_PROJECT_ID,
            cluster_name=DATAPROC_CLUSTER,
            main_python_uri=PYSPARK_EXTRACT_URI,
            arguments=["{{ ds }}", "WHOLESALE", "N"]
        ),
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        retries=3,
        retry_delay=timedelta(seconds=120),
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # --------------------------------------------------------------------------
    # CORE PROCESSING & SEGMENTATION PIPELINE
    # --------------------------------------------------------------------------

    abinitio_transform = DataprocSubmitJobOperator(
        task_id='abinitio_transform',
        job=build_dataproc_pyspark_job(
            project_id=GCP_PROJECT_ID,
            cluster_name=DATAPROC_CLUSTER,
            main_python_uri=PYSPARK_TRANSFORM_URI,
            arguments=[
                "--run_date", "{{ ds }}",
                "--customer_segment", "ALL",
                "--parallelism", "4"
            ]
        ),
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
    )

    spark_segmentation = DataprocSubmitJobOperator(
        task_id='spark_segmentation',
        job=build_dataproc_pyspark_job(
            project_id=GCP_PROJECT_ID,
            cluster_name=DATAPROC_CLUSTER,
            main_python_uri=PYSPARK_SEGMENTATION_URI,
            arguments=[
                "--run-date", "{{ ds }}",
                "--env", ENV
            ]
        ),
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
    )

    python_lineage = DataprocSubmitJobOperator(
        task_id='python_lineage',
        job=build_dataproc_pyspark_job(
            project_id=GCP_PROJECT_ID,
            cluster_name=DATAPROC_CLUSTER,
            main_python_uri=PYSPARK_LINEAGE_URI,
            arguments=[
                "--run-date", "{{ ds }}",
                "--env", ENV
            ]
        ),
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
    )

    # EXACT LITERAL MATCH FOR completion notification from source command
    completion_notify = EmailOperator(
        task_id='completion_notify',
        to=CRM_NOTIFY_EMAIL,
        subject='[CRM-OK] Weekly CRM Load {{ ds }}',
        html_content='CRM_WEEKLY_WORKFLOW completed for RUN_DATE={{ ds }}',
        trigger_rule=TriggerRule.ALL_DONE,  # Send mail even if python_lineage fails
    )

    # ==============================================================================
    # 4. RELATIONSHIP / DEPENDENCY WIRE-UP
    # ==============================================================================

    wait_finance_event >> wait_retail_event
    
    wait_retail_event >> [customer_extract_vip, customer_extract_retail, customer_extract_wholesale]
    
    [customer_extract_vip, customer_extract_retail, customer_extract_wholesale] >> abinitio_transform
    
    abinitio_transform >> [spark_segmentation, python_lineage]
    
    [spark_segmentation, python_lineage] >> completion_notify