"""
DAG: dw_dwh_dummy_absd_plato_tarife

Overview:
This migration package consists of a standalone, native UC4 UNIX job
(DW.DWH_DUMMY_ABSD_PLATO_TARIFE) with an active status. It functions as a
placeholder or dummy task that executes a basic print utility statement on a
target UNIX host. Because no parent Workflow (JOBP), Schedule (JSCH), or native
trigger Script (SCRI) was supplied within this extraction bundle, this job is
represented as an independent, single-task Airflow DAG configured for manual
or external triggering. The underlying command launcher is categorized as
unrecognized, requiring mapping to an EmptyOperator task acting as a functional stub.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# ==============================================================================
# ── Default Args ──────────────────────────────────────────────────────────────
# ==============================================================================
DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# ── DAG Definition ────────────────────────────────────────────────────────────
# ==============================================================================
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=DEFAULT_ARGS,
    description="Converted from UC4 standalone JOBS_UNIX: DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule=None,  # No calendar schedule present in extraction
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,  # Active=1 in extraction
    tags=["uc4_migration", "jobs_unix"],
) as dag:

    # ==========================================================================
    # ── Task: dw_dwh_dummy_absd_plato_tarife ──────────────────────────────────
    # ==========================================================================
    # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed
    # Original legacy command: ':print Doing nothinig'
    dw_dwh_dummy_absd_plato_tarife = EmptyOperator(
        task_id="dw_dwh_dummy_absd_plato_tarife",
    )

    # Single standalone task. No dependencies to establish.
    dw_dwh_dummy_absd_plato_tarife