"""
### Converted DAG: dw_dwh_dummy_absd_plato_tarife

**Overview**:
This DAG is a migration of the standalone UC4 Unix Job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE`.
In the source system, this is defined as a dummy job that performs no execution logic
except printing a log statement ("Doing nothinig"). Because no parent Job Plan or schedule
context was provided, this DAG runs on a manual schedule (`None`).

**Schedule**: Manual / None (no EVNT_TIME scheduler configuration provided).

**Task List**:
- `start`: EmptyOperator marking the beginning of the workflow.
- `dwh_dummy_absd_plato_tarife`: BashOperator executing the exact legacy log statement 
  character-for-character to replicate the source print command.
- `end`: EmptyOperator marking the completion of the workflow.

**Recovery / Wiederanlauf Note**:
Wiederanlauf ohne weitere Maßnahmen möglich.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

# ==============================================================================
# Environment Configuration Retrieval (Strictly complying with Placeholder Ban)
# ==============================================================================
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var=None)
DATAPROC_REGION = Variable.get("GCP_REGION", default_var=None)
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER", default_var=None)
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=None)

# ==============================================================================
# DEFAULT ARGUMENTS
# ==============================================================================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# ==============================================================================
# DAG DEFINITION
# ==============================================================================
with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife",
    default_args=default_args,
    description="Converted dummy task from UC4 DW.DWH_DUMMY_ABSD_PLATO_TARIFE",
    schedule=None,  # Manual schedule due to missing EVNT_TIME context
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["migrated_uc4", "dummy_task"],
) as dag:

    dag.doc_md = """
    ### Recovery Documentation
    Wiederanlauf ohne weitere Maßnahmen möglich
    """

    # Start dummy marker task
    start = EmptyOperator(
        task_id="start"
    )

    # Legacy command print task replicating: ":print Doing nothinig"
    dwh_dummy_absd_plato_tarife = BashOperator(
        task_id="dwh_dummy_absd_plato_tarife",
        bash_command='echo "Doing nothinig"',
    )

    # End dummy marker task
    end = EmptyOperator(
        task_id="end"
    )

    # ==========================================================================
    # TASK DEPENDENCIES
    # ==========================================================================
    start >> dwh_dummy_absd_plato_tarife >> end