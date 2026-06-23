# This Airflow DAG replaces the legacy UC4 job: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_DATA.xml
# Job: EXIS_SD_APT_NNA_DATA

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# GCP configuration placeholders — replace with real values before deployment.
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"  # TODO: replace with your GCP project ID
DATAPROC_REGION = "YOUR_DATAPROC_REGION"  # TODO: replace with your Dataproc region
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"  # TODO: replace with your cluster name
GCS_BUCKET = "YOUR_BUCKET_NAME"  # TODO: replace with your GCS bucket name
GCS_CONFIG_PATH = f"gs://{GCS_BUCKET}/config/h_exis_apt_nna_daten.var" # TODO: Update with actual config path
GCS_OUTPUT_PATH = f"gs://{GCS_BUCKET}/output/dwhm_apt_nna_daten/" # TODO: Update with actual output path

# Placeholder schedule derived from the provided skeleton; update if the UC4 design specifies otherwise.
# Manual investigation is required to determine the exact schedule and upstream dependencies.
SCHEDULE = None

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0,  # Based on design document, this might need adjustment based on business requirements.
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1), # TODO: Update with appropriate start date
}

with DAG(
    dag_id="dw_dwh_exis_sd_apt_nna_data",
    default_args=default_args,
    description="Exports telephone system master data and distributes the compressed CSV output.",
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["dataproc", "pyspark", "export"],
) as dag:
    # Start marker for the DAG.
    start = EmptyOperator(
        task_id="start",
    )

    # Submit the PySpark export job to Dataproc.
    dwh_exis_sd_apt_nna_data = DataprocSubmitJobOperator(
        task_id="dwh_exis_sd_apt_nna_data",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        job_id="{{ dag.dag_id }}_{{ run_id }}_dwh_exis_sd_apt_nna_data",
        pyspark_job={
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark/r_exis_v2.py",
            "args": [
                "--job-kennung", "EXIS_SD_APT_NNA_DATA",
                "--config-path", GCS_CONFIG_PATH,
                "--output-path", GCS_OUTPUT_PATH,
                "--execution-ts", "{{ ds }}", # Pass Airflow's execution date as YYYY-MM-DD
            ],
        },
    )

    start >> dwh_exis_sd_apt_nna_data