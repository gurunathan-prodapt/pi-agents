"""
DAG: dw_dwh_dummy_vdgd_nvr_imvt_pre

This DAG is migrated from the UC4 native Unix job object `DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE`.
It is designed as a "DUMMY" execution step and contains a placeholder command
that performs no actual operational system tasks. It is configured to run on-demand
or via external triggers (schedule=None).
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable

# GCP Configuration (sourced from Airflow Variables to avoid hardcoded placeholders)
GCP_PROJECT_ID = Variable.get("GCP_PROJECT", default_var=None)
DATAPROC_REGION = Variable.get("DATAPROC_REGION", default_var=None)
DATAPROC_CLUSTER_NAME = Variable.get("DATAPROC_CLUSTER_NAME", default_var=None)
GCS_BUCKET = Variable.get("GCS_BUCKET", default_var=None)

# Default arguments applied to all tasks
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_dummy_vdgd_nvr_imvt_pre',
    default_args=DEFAULT_ARGS,
    description='Migration DAG for UC4 dummy job DW.DWH_DUMMY_VDGD_NVR_IMVT_PRE',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=['uc4_migration', 'dummy'],
) as dag:

    # REVIEW-STRUCT: launcher command not recognised during conversion — implement the correct operator once its behaviour is confirmed
    # Original UC4 Command: :print mach nix
    # This task is mapped to an EmptyOperator as it acts as a legacy no-op / dummy step.
    dwh_dummy_vdgd_nvr_imvt_pre = EmptyOperator(
        task_id='dwh_dummy_vdgd_nvr_imvt_pre',
    )