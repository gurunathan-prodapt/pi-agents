# Migrated from UC4/Automic object: vobs/.../DW.DWH_EXIS_SD_APT_NNA_VOIC.xml
# Job: EXIS_SD_APT_NNA_VOIC
# Description: Airflow DAG for orchestrating the EXIS SD APT NNA VOIC ETL process.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator # Placeholder for SFTP transfer

with DAG(
    dag_id="dw_dwh_exis_sd_apt_nna_voic",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set to desired cron schedule or interval, e.g., "@daily", "0 5 * * *"
    catchup=False,
    tags=["exis", "dwh", "nna", "voice", "etl"],
    description="Orchestrates the export of telephone system master data to gzipped CSV via BigQuery and Dataproc.",
) as dag:
    
    # Calculate MONAT_ID (YYYYMM) for the previous month
    # Corresponds to UC4's SYS_DATE('YYYYMMDD') and SUBSTR(...,1,6) logic,
    # often meaning the processing month is the previous month.
    # For this example, let's assume it processes data for the current month - 1.
    # Adjust as per exact UC4 behavior.
    calculate_monat_id = BashOperator(
        task_id="calculate_monat_id",
        bash_command="""
            # For this example, let's derive YYYYMM for the previous month.
            # Adjust `date` command based on exact business logic if needed (e.g., current month).
            MONAT_ID=$(date -d "last month" +"%Y%m")
            echo "MONAT_ID=${MONAT_ID}"
            echo "${MONAT_ID}" > /tmp/monat_id.txt
        """,
    )

    # Read the calculated MONAT_ID
    get_monat_id = BashOperator(
        task_id="get_monat_id_var",
        bash_command="""
            export MONAT_ID=$(cat /tmp/monat_id.txt)
            echo "Fetched MONAT_ID: $MONAT_ID"
        """,
        do_xcom_push=True,
    )

    # Submit PySpark job to Dataproc for data export, post-processing, and compression
    # This job will run the r_exis_v2.py script
    submit_dataproc_job = DataprocSubmitJobOperator(
        task_id="submit_dataproc_job",
        project_id="{{ var.value.gcp_project_id }}", # Replace with your GCP project ID
        region="{{ var.value.gcp_region }}", # Replace with your GCP region
        job={
            "placement": {
                "cluster_name": "{{ var.value.dataproc_cluster_name }}" # Replace with your Dataproc cluster name
            },
            "pyspark_job": {
                "main_python_file_uri": "gs://{{ var.value.dataproc_code_bucket }}/r_exis_v2.py", # Path to r_exis_v2.py in GCS
                "args": [
                    "--yyyymm",
                    "{{ ti.xcom_pull(task_ids='get_monat_id_var') }}",
                    "--project_id",
                    "{{ var.value.gcp_project_id }}",
                    "--bq_query_file",
                    "gs://{{ var.value.dags_code_bucket }}/d_exis_apt_nna_voice.bqsql", # Path to BigQuery SQL query file in GCS
                    "--gcs_temp_bucket",
                    "{{ var.value.gcs_temp_bucket_name }}", # e.g., 'your-bucket-temp/bq_exports'
                    "--gcs_output_bucket",
                    "{{ var.value.gcs_output_bucket_name }}", # e.g., 'your-bucket-exports'
                    "--gcs_output_prefix",
                    "{{ var.value.gcs_output_prefix }}", # e.g., 'exis_data/nna_voice'
                    "--bq_dataset",
                    "{{ var.value.bq_dataset_name }}" # e.g., 'dwh_transformed'
                ],
            },
        },
    )

    # Placeholder for SFTP distribution using a Cloud Run service
    # The Cloud Run service `sftp-transfer-service` needs to be deployed separately
    # and configured to handle SFTP transfers from GCS.
    # It will receive the final GCS path from XCom or infer it.
    distribute_via_sftp = CloudRunExecuteJobOperator(
        task_id="distribute_via_sftp",
        project_id="{{ var.value.gcp_project_id }}",
        region="{{ var.value.gcp_region }}",
        job_name="sftp-transfer-service", # Name of your Cloud Run job for SFTP transfer
        body={
            "overrides": {
                "containerOverrides": [
                    {
                        "args": [
                            "--gcs_file_path",
                            "{{ ti.xcom_pull(task_ids='submit_dataproc_job', key='return_value') }}" # Assuming the PySpark job returns the GCS path
                        ]
                    }
                ]
            }
        },
        # Ensure the Cloud Run job has necessary permissions (e.g., storage.objects.get)
    )

    # Define task dependencies
    calculate_monat_id >> get_monat_id >> submit_dataproc_job >> distribute_via_sftp