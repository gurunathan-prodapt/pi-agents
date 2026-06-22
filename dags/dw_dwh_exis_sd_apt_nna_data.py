# Airflow DAG for EXIS_SD_APT_NNA_DATA job, migrating UC4 UNIX export to GCP.
# Replaces: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_MONATLICH_JP/DW.DWH_EXIS_SD_APT_NNA_DATA.xml
# Job: EXIS_SD_APT_NNA_DATA

"""
Airflow DAG for EXIS_SD_APT_NNA_DATA job, migrating UC4 UNIX export to GCP.

This DAG runs a single PySpark export job on Dataproc to generate the NNA data
export and write it to GCS. It is configured with no schedule (manual trigger),
no catchup, and a single active run at a time to mirror the UC4 sync behavior.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# GCP configuration placeholders - replace with your actual values.
# TODO: Replace with your GCP project ID
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
# TODO: Replace with your Dataproc region, e.g., "us-central1"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
# TODO: Replace with your Dataproc cluster name. Consider using ephemeral clusters for cost optimization.
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
# TODO: Replace with your GCS bucket name where PySpark scripts and data exports will be stored.
GCS_BUCKET = "YOUR_BUCKET_NAME"

default_args = {
    "owner": "uc4_migration",
    "retries": 0,  # Based on design, review if this needs to be changed.
    "retry_delay": timedelta(seconds=0),
}

with DAG(
    dag_id="dw_dwh_exis_sd_apt_nna_data",
    description="Airflow DAG for EXIS_SD_APT_NNA_DATA job, migrating UC4 UNIX export to GCP.",
    start_date=datetime(2023, 1, 1),
    # TODO: Set schedule_interval based on business requirements. Current value is None (manual trigger).
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["uc4_migration", "dataproc", "gcs_export"],
) as dag:
    # Submit the PySpark export job to Dataproc.
    # This task will execute the exis_sd_apt_nna_data.py script on a Dataproc cluster.
    dwh_exis_sd_apt_nna_data = DataprocSubmitJobOperator(
        task_id="dwh_exis_sd_apt_nna_data",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        # job_id is dynamically generated to ensure uniqueness per run
        job_id="{{ dag.dag_id }}_{{ run_id | lower | replace('_', '-') }}_dwh-exis-sd-apt-nna-data",
        pyspark_job={
            # Path to the PySpark script in GCS
            "main_python_file_uri": f"gs://{GCS_BUCKET}/pyspark_scripts/exis_sd_apt_nna_data.py",
            "args": [
                # Arguments to be passed to the PySpark script
                "--output_gcs_path",
                f"gs://{GCS_BUCKET}/exports/nna_data/",
                "--config_gcs_path",
                f"gs://{GCS_BUCKET}/config/h_exis_apt_nna_daten.var",
            ],
        },
    )