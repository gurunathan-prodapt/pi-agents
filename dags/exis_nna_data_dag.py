# Legacy source: vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_EXIS_SD_APT_NNA_DATA.xml
# Job: EXIS

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import json
import logging

# Assuming exis_exporter.py is placed in the same directory as the DAGs or in a reachable path
# For production, consider packaging exis_exporter as a Python library
# and installing it in your Airflow environment.
try:
    from exis_exporter import ExisExporter
except ImportError:
    logging.warning("exis_exporter.py not found in PYTHONPATH. Please ensure it's accessible.")
    # Define a dummy class for local testing if exis_exporter is not available
    class ExisExporter:
        def __init__(self, config): self.config = config
        def run_export_job(self): logging.info(f"Dummy ExisExporter running for {self.config.get('job_name')}")

def _run_exis_exporter_task(ds_nodash, **context):
    """
    Executes the EXIS data export job for NNA Data.
    The configuration is passed as a dictionary.
    """
    # Placeholder for SFTP credentials - In production, use Airflow Connections or Secret Manager.
    # For this example, we'll try to get from environment variables or use dummy values.
    sftp_host = os.environ.get("EXIS_SFTP_HOST", "your-sftp-host.example.com")
    sftp_user = os.environ.get("EXIS_SFTP_USER", "exis_sftp_user")
    sftp_password = os.environ.get("EXIS_SFTP_PASSWORD", "your-sftp-password-nna-data")
    sftp_port = int(os.environ.get("EXIS_SFTP_PORT", 22))

    config = {
        "job_name": "EXIS_SD_APT_NNA_DATA",
        "sql_file_path": f"{os.getenv('AIRFLOW_HOME')}/dags/sql/bq_d_exis_apt_nna_daten.sql",
        "output_base_name": f"DWHM_APT_NNA_Daten_{ds_nodash}",
        "query_parameters": {
            "FROM_YYYYMM": int(ds_nodash[:6]) # MONATS_ID expects integer
        },
        "footer_config": {
            "header": "X",
            "destination_file": f"DWHM_APT_NNA_Daten_{ds_nodash}.csv.gz",
            "from_date": ds_nodash, # YYYYMMDD
            "record_count_placeholder": True, # Indicate where to put record count
            "fixed_string": "V_F_NNA_Daten",
            "sysdate": ds_nodash # YYYYMMDD
        },
        "gcs_bucket_name": "your-exis-exports-bucket", # TODO: Replace with actual bucket name
        "gcs_archive_path": "exis_exports/EXIS_SD_APT_NNA_DATA/archive/",
        "enable_sftp": True,
        "sftp_output_name": f"DWHM_APT_NNA_Daten_{ds_nodash}.csv.gz",
        "sftp_host": sftp_host,
        "sftp_port": sftp_port,
        "sftp_user": sftp_user,
        "sftp_password": sftp_password,
        "sftp_remote_path": "/exis/sftp/out/nna_data", # TODO: Replace with actual SFTP remote path
        "gcp_project_id": os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id"), # TODO: Replace with actual GCP Project ID
        "dwh_raw_layer_dataset": "dwh_raw_layer"
    }

    exporter = ExisExporter(config)
    exporter.run_export_job()

with DAG(
    dag_id='exis_nna_data_export_dag',
    start_date=days_ago(1),
    schedule_interval='@monthly', # Based on MONATS_ID, confirm with business for exact schedule
    catchup=False,
    tags=['exis', 'export', 'nna_data', 'bigquery', 'gcs', 'sftp'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:
    export_nna_data_task = PythonOperator(
        task_id='export_nna_data',
        python_callable=_run_exis_exporter_task,
        op_kwargs={
            'ds_nodash': '{{ ds_nodash }}', # Passes execution date as YYYYMMDD (e.g., 20231026)
        },
    )