"""
DAG: sales_product_and_sales_extract
Description: Source availability check, product master SCD2 load, and daily sales extract into staging.
Original UC4 Job: SALES.PRODUCT_AND_SALES_EXTRACT
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

# ==============================================================================
# ── GCP Configuration / Environment-specific values ──────────────────────────
# ==============================================================================
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var=os.environ.get("GCP_PROJECT"))
DATAPROC_REGION = Variable.get("DATAPROC_REGION", default_var=os.environ.get("DATAPROC_REGION"))
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER", default_var=os.environ.get("DATAPROC_CLUSTER"))
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=os.environ.get("GCS_BUCKET"))
ENV_HOME = Variable.get("env_home", default_var=os.environ.get("ENV_HOME"))
CLIENT_QUEUE = Variable.get("CLIENT_QUEUE", default_var="default")

# ==============================================================================
# ── Default Args ─────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = { 
    "owner": "UNIX.ETL_SVC",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "queue": CLIENT_QUEUE,
}

# ==============================================================================
# ── DAG Definition ───────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id="sales_product_and_sales_extract",
    default_args=DEFAULT_ARGS,
    description="Source availability check, product master SCD2 load, and daily sales extract into staging",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["sales", "migration_uc4"],
) as dag:

    # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed
    sales_product_and_sales_extract_task = EmptyOperator(
        task_id="sales_product_and_sales_extract_task",
        doc_md="""
        ### UC4 Source Migration Note
        * **Original Name:** SALES.PRODUCT_AND_SALES_EXTRACT
        * **Login:** UNIX.ETL_SVC
        * **Host:** |ETLHOST3|HOST
        * **Original Script Body:**
          ```bash
          #!/bin/ksh
          # SALES.PRODUCT_AND_SALES_EXTRACT
          :SET &RUN_DATE='&$TODAY'
          . &HOME/sales/r_product_and_sales_extract.ksh
          ```
        """,
    )

    # Single-task workflow: no complex dependency chain required.
    sales_product_and_sales_extract_task