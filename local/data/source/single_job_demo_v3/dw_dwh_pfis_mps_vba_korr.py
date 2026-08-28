"""
DAG: dw_dwh_pfis_mps_vba_korr

Overview:
This DAG covers the migration of a single standalone UC4 UNIX job, DW.DWH_PFIS_MPS_VBA_KORR 
("Korrektur nicht ermittelbarer VBA-IDs"). This job is designed to perform data correction 
operations on non-determinable VBA IDs by executing a specialized Unix script 
(r_pfis_mps_vba_korrektur) on a target host. 

This process is completely idempotent and safe to restart or retry automatically without any 
manual preparation or cleanup.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

# Global Environment Variables (Sourced at runtime via Airflow Variables)
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION")
GCS_BUCKET = Variable.get("GCS_BUCKET")
LEGACY_HOST = Variable.get("legacy_host")

# Job-Specific Variables
DWH_JOB_KENNUNG = 'PFIS_MPS_VBA_KORR'

# Default Arguments
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="dw_dwh_pfis_mps_vba_korr",
    default_args=DEFAULT_ARGS,
    description="Korrektur nicht ermittelbarer VBA-IDs",
    schedule=None,  # Externally triggered, no schedule policy
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,  # Concurrency limit to prevent concurrent database writes
    tags=['migrated_uc4', 'jobs_unix', 'pfis'],
    is_paused_upon_creation=False,
) as dag:

    # Task: r_pfis_mps_vba_korrektur
    # This task maps to DW.DWH_PFIS_MPS_VBA_KORR.
    # It executes the Unix script '$HOME/aktuell/pruef/is/bin/r_pfis_mps_vba_korrektur' on the target host.
    # Environment variable DWH_JOB_KENNUNG is set to 'PFIS_MPS_VBA_KORR'.
    r_pfis_mps_vba_korrektur = BashOperator(
        task_id="r_pfis_mps_vba_korrektur",
        bash_command="$HOME/aktuell/pruef/is/bin/r_pfis_mps_vba_korrektur",
        env={
            'DWH_JOB_KENNUNG': DWH_JOB_KENNUNG,
            'GCP_PROJECT': GCP_PROJECT,
            'GCP_REGION': GCP_REGION,
            'GCS_BUCKET': GCS_BUCKET,
            'LEGACY_HOST': LEGACY_HOST,
        }
    )

    r_pfis_mps_vba_korrektur