"""
Migration of UC4 DWH Dummy Plato Tarife Job.

This DAG represents the migration of the legacy UC4 UNIX job 'DW.DWH_DUMMY_ABSD_PLATO_TARIFE'.
In the source system, this was a dummy administrative job printing a diagnostic message
without functional business logic. It has been mapped to an EmptyOperator to preserve 
the structure of the legacy execution hierarchy and downstream dependency chains.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

# ── GCP CONFIGURATION ────────────────────────────────────────────────────────
# Note: This DAG currently does not run any Dataproc/PySpark tasks.
# These constants are provided for future-proofing and environment uniformity.
GCP_PROJECT_ID = Variable.get("GCP_PROJECT")
DATAPROC_REGION = Variable.get("DATAPROC_REGION")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER")
GCS_BUCKET = Variable.get("GCS_BUCKET")

# ── DEFAULT ARGUMENTS ────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=DEFAULT_ARGS,
    description='Migration of UC4 DWH Dummy Plato Tarife Job',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['migrated', 'uc4', 'jobs_unix'],
) as dag:

    # ── Task: dummy_execution ────────────────────────────────────────────────
    # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed
    # Original UC4 script printed "Doing nothinig" using an internal UC4 command [:print Doing nothinig]
    dummy_execution = BashOperator(
        task_id='dummy_execution',
        bash_command="echo 'Doing nothinig'",
        retries=1,
        retry_delay=timedelta(minutes=5),
    )

    # ── Dependencies ─────────────────────────────────────────────────────────
    # Single-task DAG. No dependencies to define.
    dummy_execution