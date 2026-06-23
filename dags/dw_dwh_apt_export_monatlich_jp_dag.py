"""
Airflow DAG for DW.DWH_APT_EXPORT_MONATLICH_JP monthly export process.

This DAG runs monthly (@monthly) and orchestrates the export of telephone system
master data into compressed CSV files in Cloud Storage. It first checks that the
prerequisite UC4 jobs have completed successfully, then runs two parallel BigQuery
SQL transformations to prepare temporary tables for the export month, and finally
exports those tables to GCS as GZIP-compressed CSV files.

Tasks:
- Check prerequisite job status placeholders
- Prepare NNA data in BigQuery from SQL
- Prepare VOIC data in BigQuery from SQL
- Export both prepared tables to Cloud Storage as .csv.gz files
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

# GCP configuration placeholders - replace with your actual values.
# It's recommended to configure these via Airflow Variables or environment variables in a real deployment.
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")  # TODO: replace with your GCP project ID
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "your_bigquery_dataset")  # TODO: replace with your BigQuery dataset
GCS_BUCKET = os.environ.get("GCS_BUCKET", "your-gcs-export-bucket")  # TODO: replace with your GCS bucket name
GCP_LOCATION = os.environ.get("GCP_LOCATION", "US")  # TODO: replace with your BigQuery/Dataproc region if different

DAG_ID = "dw_dwh_apt_export_monatlich_jp_dag"
SQL_DATA_FILE = "sql/dwh_exis_sd_apt_nna_data.sql"
SQL_VOIC_FILE = "sql/dwh_exis_sd_apt_nna_voic.sql"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "gcp_conn_id": "google_cloud_default", # Assuming default GCP connection
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Monthly export of telephone system master data to GCS",
    schedule_interval="@monthly",
    start_date=datetime(2023, 1, 1), # Set a concrete historical start date for the DAG
    catchup=False,
    max_active_runs=1,
    tags=["dw", "bigquery", "export", "monthly", "jp"],
    template_searchpath=f"{os.environ.get('AIRFLOW_HOME', '/opt/airflow')}/dags", # Ensure SQL files are found
) as dag:
    # Monthly identifier derived from execution date
    export_month_yyyymm = "{{ execution_date.strftime('%Y%m') }}"
    export_timestamp_yyyymmddhhmmss = "{{ execution_date.strftime('%Y%m%d%H%M%S') }}"

    start_export_process = EmptyOperator(task_id="start_export_process")

    # Placeholder tasks for checking prerequisite job status (DW.BERT_STAMMDATEN_JP and DW.ACCESSP_SIGMA_GPRS_MONATLICH_JP)
    # These would ideally be replaced by ExternalTaskSensor or specific operators to check external system status.
    check_prereq_dw_bert_stammdaten_jp = EmptyOperator(task_id="check_prereq_dw_bert_stammdaten_jp")
    check_prereq_dw_accessp_sigma_gprs_monatlich_jp = EmptyOperator(task_id="check_prereq_dw_accessp_sigma_gprs_monatlich_jp")

    # Prepare the NNA data in a temporary BigQuery table using SQL from file.
    prepare_nna_data = BigQueryInsertJobOperator(
        task_id="prepare_nna_data",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        configuration={
            "query": {
                "query": "{% include '" + SQL_DATA_FILE + "' %}", # Load SQL from file
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"temp_dwh_exis_sd_apt_nna_data_{export_month_yyyymm}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "queryParameters": [ # Pass parameter to SQL
                    {
                        "name": "export_month_yyyymm",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": export_month_yyyymm}
                    }
                ]
            }
        },
        job_id=f"{DAG_ID}_{{ run_id }}_prepare_nna_data",
    )

    # Prepare the VOIC data in a temporary BigQuery table using SQL from file.
    prepare_voic_data = BigQueryInsertJobOperator(
        task_id="prepare_voic_data",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        configuration={
            "query": {
                "query": "{% include '" + SQL_VOIC_FILE + "' %}", # Load SQL from file
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"temp_dwh_exis_sd_apt_nna_voic_{export_month_yyyymm}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
                "queryParameters": [ # Pass parameter to SQL
                    {
                        "name": "export_month_yyyymm",
                        "parameterType": {"type": "STRING"},
                        "parameterValue": {"value": export_month_yyyymm}
                    }
                ]
            }
        },
        job_id=f"{DAG_ID}_{{ run_id }}_prepare_voic_data",
    )

    # Export the prepared NNA data table to Cloud Storage as a compressed CSV file.
    export_nna_data_to_gcs = BigQueryToGCSOperator(
        task_id="export_nna_data_to_gcs",
        project_id=GCP_PROJECT_ID,
        source_project_dataset_table=(
            f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.temp_dwh_exis_sd_apt_nna_data_{export_month_yyyymm}"
        ),
        destination_cloud_storage_uris=[
            f"gs://{GCS_BUCKET}/exports/DWHM_APT_NNA_Daten_{export_timestamp_yyyymmddhhmmss}.csv.gz"
        ],
        export_format="CSV",
        compression="GZIP",
        print_header=True, # Ensure header is included in the exported CSV
        location=GCP_LOCATION,
        job_id=f"{DAG_ID}_{{ run_id }}_export_nna_data_to_gcs",
    )

    # Export the prepared VOIC data table to Cloud Storage as a compressed CSV file.
    export_voic_data_to_gcs = BigQueryToGCSOperator(
        task_id="export_voic_data_to_gcs",
        project_id=GCP_PROJECT_ID,
        source_project_dataset_table=(
            f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET}.temp_dwh_exis_sd_apt_nna_voic_{export_month_yyyymm}"
        ),
        destination_cloud_storage_uris=[
            f"gs://{GCS_BUCKET}/exports/DWHM_APT_NNA_Voic_{export_timestamp_yyyymmddhhmmss}.csv.gz"
        ],
        export_format="CSV",
        compression="GZIP",
        print_header=True, # Ensure header is included in the exported CSV
        location=GCP_LOCATION,
        job_id=f"{DAG_ID}_{{ run_id }}_export_voic_data_to_gcs",
    )

    end_export_process = EmptyOperator(task_id="end_export_process")

    # Define task dependencies:
    # Start -> Parallel prerequisite checks
    start_export_process >> [check_prereq_dw_bert_stammdaten_jp, check_prereq_dw_accessp_sigma_gprs_monatlich_jp]
    # Prerequisite checks complete -> Parallel data preparation tasks
    [check_prereq_dw_bert_stammdaten_jp, check_prereq_dw_accessp_sigma_gprs_monatlich_jp] >> [prepare_nna_data, prepare_voic_data]
    # Data preparation complete -> Corresponding GCS export
    prepare_nna_data >> export_nna_data_to_gcs
    prepare_voic_data >> export_voic_data_to_gcs
    # Both GCS exports complete -> End process
    [export_nna_data_to_gcs, export_voic_data_to_gcs] >> end_export_process