"""
DAG: dw_dwh_plato_tarif_mapping_taeglich_dag
Source UC4 Job: DW.DWH_DUMMY_ABSD_PLATO_TARIFE (JOBS_UNIX)

Description:
This DAG is a daily dummy task migrated from UC4, belonging to a broader Plato tariff 
mapping workflow. The original UC4 job script served as a placeholder / orchestration step 
and only printed "Doing nothinig" (verbatim). It has been mapped to a lightweight 
PythonOperator execution pattern within Airflow to avoid Dataproc resource overhead.

Schedule:
None (This orchestrator is designed to run on-demand or to be integrated into a parent 
workflow once migrated).

Tasks:
- start: Empty boundary start task.
- dw_dwh_dummy_absd_plato_tarife: Executes a python function printing the verbatim message.
- end: Empty boundary end task.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# ── GCP CONFIGURATION (GLOBAL SOURCING) ──────────────────
# Sourced dynamically via Airflow Variable store to avoid hardcoding or placeholders.
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")

# ── DEFAULT ARGUMENTS ─────────────────────────────────────────────────
DEFAULT_ARGS = { 
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),  # Based on UC4 last modified date metadata
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG DEFINITION ────────────────────────────────────────────────────
dag = DAG(
    dag_id="dw_dwh_plato_tarif_mapping_taeglich_dag",
    default_args=DEFAULT_ARGS,
    schedule=None,  # No schedule defined due to missing JSCH/EVNT_TIME XML exports
    catchup=False,
    max_active_runs=1,  # Replicates UC4 sync object behavior
    is_paused_upon_creation=False,  # Source active flag <Active>1</Active>
    doc_md=__doc__,
)

# ── Dummy Script Execution ───────────────────────────────
def execute_dummy_task():
    # OUTPUT/PRINT LITERAL RULE: Must match legacy text exactly: "Doing nothinig"
    print("Doing nothinig")

# ── Task Definitions ─────────────────────────────────────
start = EmptyOperator(
    task_id="start",
    dag=dag,
)

dw_dwh_dummy_absd_plato_tarife = PythonOperator(
    task_id="dw_dwh_dummy_absd_plato_tarife",
    python_callable=execute_dummy_task,
    dag=dag,
)

end = EmptyOperator(
    task_id="end",
    dag=dag,
)

# ── DEPENDENCY GRAPH ──────────────────────────────────────────────────
start >> dw_dwh_dummy_absd_plato_tarife >> end