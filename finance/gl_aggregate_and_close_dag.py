"""
DAG: finance_gl_aggregate_and_close

Source UC4 Object: FINANCE.GL_AGGREGATE_AND_CLOSE (JOBS_UNIX)

Description:
This DAG performs a Spark aggregation for analytical financial outputs, writes
a close audit record, and dispatches notifications. Because no enclosing parent 
workflow (JOBP) was provided in the extraction, this is configured as a standalone 
externally-triggered pipeline.

Dynamic Parameters Managed:
- PERIOD_NAME: Calculated dynamically as the previous month (MMM_YYYY).
- FISCAL_YEAR: Calculated dynamically based on execution date.
"""

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# ==============================================================================
# GCP CONFIGURATION (GLOBAL ENVIRONMENT VARIABLES)
# ==============================================================================
GCP_PROJECT = Variable.get("GCP_PROJECT", default_var=os.environ.get("GCP_PROJECT"))
GCP_REGION = Variable.get("GCP_REGION", default_var=os.environ.get("GCP_REGION"))
DATAPROC_REGION = Variable.get("DATAPROC_REGION", default_var=os.environ.get("DATAPROC_REGION"))
DATAPROC_CLUSTER = Variable.get("DATAPROC_CLUSTER", default_var=os.environ.get("DATAPROC_CLUSTER"))
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=os.environ.get("GCS_BUCKET"))

# ==============================================================================
# JOB-SPECIFIC CONFIGURATION
# ==============================================================================
JOB_CONFIG = {
    "NOTIFY_EMAIL": "finance-etl@example.com"
}

# ==============================================================================
# DEFAULT ARGS & DAG DEFINITION
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "finance",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG( 
    dag_id="finance_gl_aggregate_and_close",
    default_args=DEFAULT_ARGS,
    description="Spark aggregation for analytical outputs and close audit recording",
    schedule=None,  # Externally triggered / Standalone execution
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["finance", "uc4_migration"],
    params={
        "PERIOD_NAME": "{{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%b_%Y') }}",
        "FISCAL_YEAR": "{{ execution_date.strftime('%Y') }}",
        "NOTIFY_EMAIL": JOB_CONFIG["NOTIFY_EMAIL"]
    }
) as dag:

    # REVIEW: This is a standalone UNIX job without an enclosing parent JOBP workflow. 
    # Confirm if this should be integrated as a sub-task into a larger existing pipeline.

    # ==========================================================================
    # TASK DEFINITIONS
    # ==========================================================================

    # Spark aggregation for analytical outputs, write close audit record, and send notification.
    # Originally resolved dynamic parameters:
    #   PERIOD_NAME: {{ (execution_date - macros.dateutil.relativedelta.relativedelta(months=1)).strftime('%b_%Y') }}
    #   FISCAL_YEAR: {{ execution_date.strftime('%Y') }}
    # Original script wrapper target: . &HOME/finance/r_gl_aggregate_and_close.ksh
    gl_aggregate_and_close = BashOperator(
        task_id="gl_aggregate_and_close",
        bash_command="python {{ var.value.get('FIN_HOME', '/opt/etl/finance') }}/finance/r_gl_aggregate_and_close.py",
        env={
            "PERIOD_NAME": "{{ params.PERIOD_NAME }}",
            "FISCAL_YEAR": "{{ params.FISCAL_YEAR }}",
            "NOTIFY_EMAIL": "{{ params.NOTIFY_EMAIL }}",
            "GCP_PROJECT": GCP_PROJECT,
            "GCS_BUCKET": GCS_BUCKET,
            "DATAPROC_CLUSTER": DATAPROC_CLUSTER,
            "DATAPROC_REGION": DATAPROC_REGION,
        },
    )

    # ==========================================================================
    # DEPENDENCY MAP
    # ==========================================================================
    # Standalone single-task DAG, no dependencies needed.
    gl_aggregate_and_close