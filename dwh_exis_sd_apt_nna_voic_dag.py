# Airflow DAG for EXIS_SD_APT_NNA_VOIC
# Replaces: vobs/dw_source/isdwh/uc4_prod_exports/.../DW.DWH_EXIS_SD_APT_NNA_VOIC.xml

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Assuming sftp_exporter.py is available in the DAGs folder or a configured Python path
from sftp_exporter import sftp_transfer_gcs_file

# Define GCS bucket and BigQuery project/dataset
GCS_BUCKET = "your-gcs-export-bucket"  # TODO: Replace with your actual GCS bucket
BIGQUERY_PROJECT_ID = "your-gcp-project-id"  # TODO: Replace with your actual GCP Project ID
BIGQUERY_DATASET = "raw_dwh"

# SFTP Connection details - should be stored securely, e.g., in Airflow Connections or Secret Manager
# Using placeholders here. In a real scenario, retrieve from Airflow Connection.
SFTP_HOST = "your-sftp-host"
SFTP_PORT = 22  # Standard SFTP port
SFTP_USERNAME = "your-sftp-username"
SFTP_PASSWORD = "your-sftp-password"  # Best practice: use SSH keys or Airflow Connections with password
SFTP_REMOTE_PATH = "/path/to/remote/sftp/directory" # TODO: Replace with actual remote SFTP directory

with DAG(
    dag_id="dwh_exis_sd_apt_nna_voic_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 0 1 * *",  # Run on the 1st of every month at midnight UTC
    catchup=False,
    tags=["dwh", "export", "sftp"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
    # Task 1: Execute BigQuery SQL query to extract data
    # The MONATS_ID parameter is derived from the execution date (ds) in YYYYMM format.
    # Airflow macro `ds_format` is used to convert 'YYYY-MM-DD' to 'YYYYMM'.
    bigquery_extract_task = BigQueryExecuteQueryOperator(
        task_id="extract_voice_data_from_bigquery",
        sql="d_exis_apt_nna_voice.bq.sql",
        use_legacy_sql=False,
        destination_dataset_table=f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.temp_nna_voice_export_table_{{{{ ds_nodash }}}}", # Temporary table for export
        write_disposition="WRITE_TRUNCATE",
        query_params=[
            {
                "name": "FROM_YYYYMM",
                "parameterType": {"type": "INT64"},
                "parameterValue": {"value": "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m') }}"},
            },
        ],
        gcp_conn_id="google_cloud_default",
    )

    # Task 2: Export BigQuery query results to GCS as a gzipped CSV
    gcs_export_uri = f"gs://{GCS_BUCKET}/dwhm_apt_nna_voice_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m') }}}}.csv.gz"
    bigquery_export_to_gcs_task = BigQueryToGCSOperator(
        task_id="export_bigquery_to_gcs",
        source_project_dataset_table=f"{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.temp_nna_voice_export_table_{{{{ ds_nodash }}}}",
        destination_cloud_storage_uris=[gcs_export_uri],
        compression="GZIP",
        export_format="CSV",
        field_delimiter=",",
        print_header=True,
        gcp_conn_id="google_cloud_default",
    )

    # Task 3: Transfer the gzipped CSV from GCS to the external SFTP server
    # This task calls a Python function defined in sftp_exporter.py
    sftp_transfer_task = PythonOperator(
        task_id="sftp_transfer_to_external_system",
        python_callable=sftp_transfer_gcs_file,
        op_kwargs={
            "gcs_bucket": GCS_BUCKET,
            "gcs_object_name": f"dwhm_apt_nna_voice_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m') }}}}.csv.gz",
            "sftp_host": SFTP_HOST,
            "sftp_port": SFTP_PORT,
            "sftp_username": SFTP_USERNAME,
            "sftp_password": SFTP_PASSWORD,
            "sftp_remote_path": SFTP_REMOTE_PATH,
            "sftp_filename": f"dwhm_apt_nna_voice_{{{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m') }}}}.csv.gz",
        },
    )

    # Task 4 (Optional): Cleanup the temporary BigQuery table
    cleanup_temp_bq_table_task = BigQueryExecuteQueryOperator(
        task_id="cleanup_temp_bigquery_table",
        sql=f"DROP TABLE IF EXISTS `{BIGQUERY_PROJECT_ID}.{BIGQUERY_DATASET}.temp_nna_voice_export_table_{{{{ ds_nodash }}}}`",
        use_legacy_sql=False,
        trigger_rule=TriggerRule.ALL_DONE,  # Run even if upstream tasks fail
        gcp_conn_id="google_cloud_default",
    )


    # Define task dependencies
    bigquery_extract_task >> bigquery_export_to_gcs_task >> sftp_transfer_task
    sftp_transfer_task >> cleanup_temp_bq_table_task