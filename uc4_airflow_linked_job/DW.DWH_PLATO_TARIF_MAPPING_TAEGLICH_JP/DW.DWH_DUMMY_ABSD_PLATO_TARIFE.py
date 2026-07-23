"""
### Workflow Migration: DW.DWH_DUMMY_ABSD_PLATO_TARIFE

This DAG is a migration of the UC4 UNIX job `DW.DWH_DUMMY_ABSD_PLATO_TARIFE` to Cloud Composer (Apache Airflow).
It represents a dummy/placeholder task in the daily mapping workflow (`DW.DWH_PLATO_TARIF_MAPPING_TAEGLICH_JP`).

Since this job does not contain any business data-processing scripts or Ab Initio/PySpark job executions,
it is migrated as a simple diagnostic logging task (PythonOperator) that outputs a diagnostic message,
preserving legacy spelling verbatim.
"""

from datetime import datetime, timedelta
import logging
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# ==============================================================================
# GCP CONFIGURATION SOURCED DYNAMICALLY
# ==============================================================================
# Note: GCP configurations are declared for environment parity but unused since 
# this job does not execute any Dataproc or GCS-based processes.
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var=os.environ.get("GCP_PROJECT"))
DATAPROC_REGION = Variable.get("DATAPROC_REGION", default_var=os.environ.get("DATAPROC_REGION"))
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER", default_var=os.environ.get("DATAPROC_CLUSTER"))
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=os.environ.get("GCS_BUCKET"))

# ==============================================================================
# DEFAULT ARGS & DAG DEFINITION
# ==============================================================================
# Owner mapped from Login: DW.UNIX.ISTNS
# Schedule is None as no EVNT_TIME or scheduling metadata was defined for this sub-component
default_args = {
    "owner": "DW.UNIX.ISTNS",
    "start_date": datetime(2026, 3, 30),
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dw_dwh_dummy_absd_plato_tarife_parent",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    doc_md=__doc__,
) as dag:

    # ==============================================================================
    # CALLBACKS AND CALLABLES
    # ==============================================================================
    def log_dummy_message_callable(**context):
        """
        Simulates the exact behavior of the UC4 job script: printing "Doing nothinig".
        Preserves the original legacy typo 'nothinig' for functional parity.
        """
        logging.info("Doing nothinig")

    # ==============================================================================
    # TASK DEFINITIONS
    # ==============================================================================
    # Mapped from JOBS_UNIX "DW.DWH_DUMMY_ABSD_PLATO_TARIFE"
    dw_dwh_dummy_absd_plato_tarife = PythonOperator(
        task_id="dw_dwh_dummy_absd_plato_tarife",
        python_callable=log_dummy_message_callable,
    )

    # ==============================================================================
    # PIPELINE LAYOUT
    # ==============================================================================
    # Single-node DAG mapping
    dw_dwh_dummy_absd_plato_tarife