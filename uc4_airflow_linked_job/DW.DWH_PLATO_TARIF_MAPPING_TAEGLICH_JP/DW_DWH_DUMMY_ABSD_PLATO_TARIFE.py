"""
DAG: dw_dwh_dummy_absd_plato_tarife
Schedule: None (Triggered by parent workflow)

This DAG is a migration of the UC4 UNIX Job 'DW.DWH_DUMMY_ABSD_PLATO_TARIFE'.
It serves as a dummy orchestration milestone or synchronization task in the Plato Tariff 
daily mapping pipeline (DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP).
The legacy task ran purely as a structural pass-through milestone with no operational 
script body or Ab Initio graphs, simply printing/logging a status statement.

German Documentation: "Wiederanlauf ohne weitere Maßnahmen möglich"
(Restart possible without further measures / task is idempotent).
"""

from datetime import datetime
import logging
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

# ─── GCP CONFIGURATION ────────────────────────────────────────────────────────
# Environment values are resolved from the Airflow configuration store in production.
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var=None)
DATAPROC_REGION = Variable.get("GCP_REGION", default_var="europe-west3")
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER", default_var=None)

# ─── DEFAULT ARGS ─────────────────────────────────────────────────────────────
# Derived from UC4 Login 'DW.UNIX.ISTNS' -> mapped to 'air_istns'.
# Legacy metadata defined no retries or automatic restart postconditions.
default_args = {
    'owner': 'air_istns',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 30),
    'retries': 0,
    'email_on_failure': False,
    'email_on_retry': False,
}

# ─── PYTHON CALLABLE ──────────────────────────────────────────────────────────
def execute_dummy_script(**context):
    """
    Emulates the legacy UC4 script execution: :print Doing nothinig.
    Preserves the original typographical error ('nothinig') verbatim 
    to ensure compliance with downstream regex monitors.
    """
    logging.info("Executing script body from DW.DWH_DUMMY_ABSD_PLATO_TARIFE...")
    # Verbatim print literal rule preservation (German: Wiederanlauf ohne weitere Maßnahmen möglich):
    logging.info("Doing nothinig")
    logging.info("Execution finished successfully.")

# ─── DAG DEFINITION ───────────────────────────────────────────────────────────
with DAG(
    dag_id='dw_dwh_dummy_absd_plato_tarife',
    default_args=default_args,
    description='Migration of UC4 dummy task DW.DWH_DUMMY_ABSD_PLATO_TARIFE',
    schedule=None,  # No schedule provided in source files; triggered by parent workflow
    catchup=False,
    max_active_runs=1,  # Prevent concurrent executions (sync object emulation)
    is_paused_upon_creation=False,  # Active=1 in source XHEADER
    tags=['uc4_migration', 'dwh_plato_tarif'],
) as dag:

    # ─── TASKS ────────────────────────────────────────────────────────────────
    # Emulates the legacy dummy milestone execution using a PythonOperator.
    dwh_dummy_absd_plato_tarife_task = PythonOperator( 
        task_id='dwh_dummy_absd_plato_tarife',
        python_callable=execute_dummy_script,
        provide_context=True,
    )

    # ─── DEPENDENCIES ─────────────────────────────────────────────────────────
    # Single-node pipeline execution flow
    dwh_dummy_absd_plato_tarife_task