"""
Legacy Source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_TAEGLICH_JP/DW.DWH_EXIS_SD_APT_BESTANDS.xml
Job: EXIS_SD_APT_BESTANDS

This Airflow DAG migrates the UC4 JOBS_UNIX workflow EXIS_SD_APT_BESTANDS
to Google Cloud Platform. It orchestrates a Dataproc PySpark job
to extract stock data, transform it, and export it as a gzipped CSV to GCS.
"""

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.models import Variable

# Retrieve GCP configuration from Airflow Variables
# Ensure these variables are set in your Airflow environment (Admin -> Variables)
# Example:
# gcp_project_id: your-gcp-project-id
# dataproc_region: us-central1
# dataproc_cluster_name: your-dataproc-cluster
# gcs_code_bucket: your-gcs-code-bucket (for pyspark_scripts)
# gcs_output_bucket: your-gcs-output-bucket (for CSV exports)
# gcs_config_bucket: your-gcs-config-bucket (for h_exis_apt_bestandsdaten.var)

GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="your-gcp-project-id")
DATAPROC_REGION = Variable.get("dataproc_region", default_var="your-dataproc-region")
DATAPROC_CLUSTER_NAME = Variable.get("dataproc_cluster_name", default_var="your-dataproc-cluster")
GCS_CODE_BUCKET = Variable.get("gcs_code_bucket", default_var="your-gcs-code-bucket")
GCS_OUTPUT_BUCKET = Variable.get("gcs_output_bucket", default_var="your-gcs-output-bucket")
GCS_CONFIG_BUCKET = Variable.get("gcs_config_bucket", default_var="your-gcs-config-bucket")

# TODO: Determine the actual schedule based on the legacy UC4 job schedule.
# For now, it's set to None for manual triggering or external scheduling.
# Example: "0 5 * * *" for daily at 5 AM UTC
SCHEDULE_INTERVAL = None

# TODO: Set the appropriate start_date for your environment.
# This should typically be the date from which you want to start processing data.
# It's recommended to set a fixed date in the past for initial deployments.
START_DATE = pendulum.datetime(2023, 1, 1, tz="UTC")

with DAG(
    dag_id="dw_dwh_exis_sd_apt_bestands",
    start_date=START_DATE,
    schedule=SCHEDULE_INTERVAL,
    catchup=False,
    tags=["exis", "sd", "apt", "bestands", "dataproc", "pyspark", "export", "migration"],
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
    },
    description="Migrated DAG for EXIS_SD_APT_BESTANDS stock export",
) as dag:
    start = EmptyOperator(
        task_id="start",
    )

    # Submit the PySpark job to Dataproc. This job replicates the logic of the
    # original r_exis_v2 binary to extract and export stock data.
    run_dwh_exis_sd_apt_bestands = DataprocSubmitJobOperator(
        task_id="run_dwh_exis_sd_apt_bestands",
        project_id=GCP_PROJECT_ID,
        region=DATAPROC_REGION,
        cluster_name=DATAPROC_CLUSTER_NAME,
        job_id="{{ dag.dag_id }}_{{ run_id }}_run_dwh_exis_sd_apt_bestands",
        pyspark_job={
            "main_python_file_uri": f"gs://{GCS_CODE_BUCKET}/pyspark_scripts/r_exis_v2.py",
            "args": [
                # Output path for the gzipped CSV file on GCS
                "--output-path",
                f"gs://{GCS_OUTPUT_BUCKET}/DWHM_APT_BESTANDSREPORT_{{{{ ds_nodash }}}}_{{{{ ts_nodash }}}}.csv.gz",
                # Path to the configuration file for the PySpark job
                "--config-file",
                f"gs://{GCS_CONFIG_BUCKET}/apt/cfg/h_exis_apt_bestandsdaten.var",
                # TODO: Add any other necessary arguments for the PySpark script,
                # e.g., BigQuery project/dataset, specific date ranges if not handled by Airflow macros.
                "--bq-project", GCP_PROJECT_ID,
                "--bq-dataset", "raw_oracle_data", # Placeholder for the BigQuery dataset where Oracle data is landed
            ],
            # Add other job properties if necessary, e.g., `properties`, `file_uris`, `py_files`
        },
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> run_dwh_exis_sd_apt_bestands >> end