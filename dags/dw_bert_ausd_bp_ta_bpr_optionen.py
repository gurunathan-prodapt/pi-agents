# ===================================================================
# Target File: dw_bert_ausd_bp_ta_bpr_optionen.py
# Path: dags/dw_bert_ausd_bp_ta_bpr_optionen.py
# Purpose: Orchestrates target table execution and audit checks
# Legacy Source: DW.BERT_AUSD_BP_TA_BPR_OPTIONEN.xml & Wrappers
# ===================================================================

from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator

# Retrieve Environment Configuration variables with fallback options
gcp_project_id = Variable.get("gcp_project_id", default_var="YOUR_GCP_PROJECT_ID")
bq_dataset = Variable.get("bq_dataset", default_var="isbert_schema")
bq_location = Variable.get("bq_location", default_var="EU")
gcp_conn_id = Variable.get("gcp_conn_id", default_var="google_cloud_default")

# Default Args aligned to the system constraints
default_args = {
    "owner": "DW.UNIX.ISBERT",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_bert_ausd_bp_ta_bpr_optionen",
    default_args=default_args,
    description="BERT Stammdaten: Prepare Instantiated Base Products (BPR_OPTIONEN)",
    schedule_interval=None,  # Handled on-demand or triggered by parent job orchestrator
    catchup=False,
    max_active_runs=1,
    tags=["bigquery", "bert", "stammdaten"],
) as dag:

    start_task = EmptyOperator(task_id="start")

    # Executes the external SQL file containing Truncate & Insert operations
    execute_sql_process = BigQueryExecuteQueryOperator(
        task_id="execute_bpr_optionen_update",
        sql="sql/d_ausd_bp_ta_bpr_optionen.sql",
        use_legacy_sql=False,
        gcp_conn_id=gcp_conn_id,
        location=bq_location,
        write_disposition="WRITE_APPEND", # Truncate and insert are handled internally inside the script
        create_disposition="CREATE_IF_NEEDED"
    )

    end_task = EmptyOperator(task_id="end")

    # Execution Sequence
    start_task >> execute_sql_process >> end_task