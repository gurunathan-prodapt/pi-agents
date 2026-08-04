"""
DAG: customer_historization_load

Overview:
This workload performs a Slowly Changing Dimension Type 2 (SCD2) historization of 
the weekly customer segment and score into a target segment dimension table.
It executes a migrated Python wrapper script (customer/r_historization_load.py) 
which replaces the legacy Korn shell script (r_historization_load.ksh).

Since no parent workflow or schedule was supplied, this DAG is configured 
as an externally triggered process (schedule=None).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# ==============================================================================
# ── GCP AND ENVIRONMENT CONFIGURATION ─────────────────────────────────────────
# ==============================================================================
# Global environment parameters fetched dynamically from the Airflow Variable store.
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
SCRIPTS_DIR = Variable.get("SCRIPTS_DIR")

# ==============================================================================
# ── DEFAULT ARGS ──────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "UNIX.ETL_SVC",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# ── DAG DEFINITION ────────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id="customer_historization_load",
    default_args=DEFAULT_ARGS,
    description="SCD2 historization of the weekly customer segment/score into the segment dimension",
    schedule=None,  # Externally triggered (no schedule provided in UC4 source)
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["migration_uc4", "customer"],
) as dag:

    # ==========================================================================
    # ── TASK: CUSTOMER_HISTORIZATION_LOAD ─────────────────────────────────────
    # ==========================================================================
    # This task runs the migrated Python wrapper script which replaces r_historization_load.ksh.
    # It passes RUN_DATE (templated as Airflow logical date) and MAX_EXPECTED_CHANGE_PCT
    # as environment variables to configure the execution and validation thresholds.
    customer_historization_load = BashOperator(
        task_id="customer_historization_load",
        bash_command="python3 {{ params.script_path }}",
        env={
            "RUN_DATE": "{{ ds }}",
            "MAX_EXPECTED_CHANGE_PCT": "25",
            "GCP_PROJECT": GCP_PROJECT_ID,
            "GCP_REGION": DATAPROC_REGION,
        },
        params={
            "script_path": f"{SCRIPTS_DIR}/customer/r_historization_load.py"
        },
    )

    # ==========================================================================
    # ── DEPENDENCIES ──────────────────────────────────────────────────────────
    # ==========================================================================
    # Single-task DAG. No internal dependencies defined.
    # Downstream Note: Once CUSTOMER.WEEKLY_SCHEDULE is migrated, its orchestration
    # DAG should trigger or depend on this DAG.
    customer_historization_load