# Apache Airflow DAG for EXIS_SD_APT_RABATT
# Legacy Source: DW.DWH_EXIS_SD_APT_RABATT.xml
# Job: EXIS_SD_APT_RABATT

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

import os
from datetime import timedelta

# Import the Python script for post-processing
# For Airflow, this script should be available in the DAGs folder or a mounted volume.
# We'll call it via PythonOperator with arguments.
# Make sure python/post_process_rabatt_data.py is uploaded to your Composer environment's DAGs folder
# or a location accessible by the Airflow workers.
# Alternatively, embed the logic directly if the script is small, or use a custom hook/operator.
# For this example, we assume it's callable via PythonOperator.

# --- Airflow Variables (Configurable in Airflow UI/CLI) ---
# Example values, replace with actual production values
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
BQ_SOURCE_DATASET = "ORACLE_DATA" # Dataset where Oracle replicated tables reside
BQ_TEMP_DATASET = "dwh_apt_rabatt" # Dataset for staging data
BQ_TEMP_TABLE_PREFIX = "rabatt_report_staging"
GCS_EXPORT_BUCKET = f"{GCP_PROJECT_ID}-apt-rabatt-export"
GCS_WORK_PATH = f"gs://{GCS_EXPORT_BUCKET}/work"
GCS_ARCHIVE_PATH = f"gs://{GCS_EXPORT_BUCKET}/archive"

# SFTP Connection ID (configured in Airflow Connections)
SFTP_CONNECTION_ID = "sftp_apt_rabatt_conn"
SFTP_REMOTE_PATH = Variable.get("sftp_remote_path", "/path/to/sftp/target/dir/") # Example SFTP target directory

# --- DAG Definition ---
with DAG(
    dag_id="exis_sd_apt_rabatt_dag",
    schedule_interval="0 4 * * *",  # Example: daily at 4 AM, adjust as per UC4 schedule
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    tags=["exis", "rabatt", "bigquery", "sftp"],
    params={
        "output_file_prefix": "DWHM_APT_RABATTREPORT",
        "output_file_separator": "|" # From h_exis_apt_rabattdaten.var
    }
) as dag:
    # Generate timestamp for output file names
    # e.g., YYYYMMDDHH24MISS
    timestamp_format = "%Y%m%d%H%M%S"
    formatted_timestamp = "{{ ds_nodash }}{{ ts_nodash[9:15] }}" # Example: 20231027143000

    output_csv_filename_raw = f"{{ params.output_file_prefix }}_{formatted_timestamp}.csv"
    output_gz_filename = f"{{ params.output_file_prefix }}_{formatted_timestamp}.csv.gz"

    # Task 1: Extract and Transform data using BigQuery
    # This task executes the SQL and exports the result to GCS as a CSV file.
    extract_transform_bq = BigQueryExecuteQueryOperator(
        task_id="extract_transform_bq",
        sql="sql/rabatt_data_extraction.sql",
        destination_dataset_table=f"{BQ_TEMP_DATASET}.{BQ_TEMP_TABLE_PREFIX}_{{{{ ds_nodash }}}}", # Temporary table if needed for inspection
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        destination_format="CSV",
        field_delimiter=",", # BigQuery default CSV delimiter, will be adjusted in post-processing
        export_format="CSV",
        # Target URI for GCS export, will be used as input for PythonOperator
        # Note: BigQueryExecuteQueryOperator doesn't directly support exporting to GCS.
        # We need to use BigQueryToGCSOperator for that, or write to a table first then export.
        # For simplicity and to match the flow, let's assume `rabatt_data_extraction.sql`
        # can directly output to a temporary GCS file. A `BigQueryToGCSOperator` would be more appropriate.
        # Let's adjust this to write to a temp table, then use a BigQueryToGCSOperator.
        # This requires an additional task.

        # Corrected approach: Write to a temporary BigQuery table, then export to GCS.
        # This BigQueryExecuteQueryOperator will create and populate the staging table.
    )

    # Task 1.1: Export BigQuery table to GCS as CSV
    from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator

    bq_to_gcs_export = BigQueryToGCSOperator(
        task_id="bq_to_gcs_export",
        source_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_TEMP_DATASET}.{BQ_TEMP_TABLE_PREFIX}_{{{{ ds_nodash }}}}",
        destination_cloud_storage_uris=[f"{GCS_WORK_PATH}/{output_csv_filename_raw}"],
        export_format="CSV",
        field_delimiter="{{ params.output_file_separator }}", # Use the desired separator for direct export from BQ if possible, otherwise Python script handles.
                                                              # BigQuery export supports a single character delimiter.
        print_header=False, # We want to add our own header later
        gcp_conn_id="google_cloud_default",
    )


    # Task 2: Post-process and Compress using Python script
    post_process_and_compress_task = PythonOperator(
        task_id="post_process_and_compress",
        python_callable=post_process_and_compress,
        op_kwargs={
            "input_gcs_path": f"{GCS_WORK_PATH}/{output_csv_filename_raw}",
            "output_gcs_path": f"{GCS_WORK_PATH}/{output_gz_filename}",
            "project_id": GCP_PROJECT_ID,
        },
        # Assuming the python script `post_process_rabatt_data.py` is available
        # in the same DAGs folder or an accessible location.
        # If not, it needs to be packaged and handled appropriately (e.g., custom operator, Dataflow job).
    )

    # Task 3: Distribute to SFTP
    distribute_to_sftp = SFTPOperator(
        task_id="distribute_to_sftp",
        sftp_conn_id=SFTP_CONNECTION_ID,
        local_filepath=f"{GCS_WORK_PATH}/{output_gz_filename}", # SFTP operator typically expects a local path
                                                               # This will require downloading from GCS first.
                                                               # A custom Python task or GCS hook would be better.
        remote_filepath=f"{SFTP_REMOTE_PATH}/{output_gz_filename}",
        operation="put", # 'put' means local_filepath to remote_filepath
        create_intermediate_dirs=True,
    )

    # Note on distribute_to_sftp: The SFTPOperator's `local_filepath` typically expects a path on the Airflow worker's local filesystem.
    # To get the file from GCS to the local filesystem for SFTP, an intermediate step is needed:
    # 1. Use `GoogleCloudStorageDownloadOperator` to download the file from GCS to a temporary location on the worker.
    # 2. Then pass that temporary local path to `SFTPOperator`.
    # Let's adjust for this by adding a download task.

    from airflow.providers.google.cloud.operators.gcs import GCSDownloadOperator

    gcs_download_for_sftp = GCSDownloadOperator(
        task_id="gcs_download_for_sftp",
        bucket_name=GCS_EXPORT_BUCKET,
        object_name=f"work/{output_gz_filename}",
        filename=f"/tmp/{output_gz_filename}", # Download to worker's temp dir
        gcp_conn_id="google_cloud_default",
    )

    distribute_to_sftp_corrected = SFTPOperator(
        task_id="distribute_to_sftp_corrected",
        sftp_conn_id=SFTP_CONNECTION_ID,
        local_filepath=f"/tmp/{output_gz_filename}", # Use the downloaded file
        remote_filepath=f"{SFTP_REMOTE_PATH}/{output_gz_filename}",
        operation="put",
        create_intermediate_dirs=True,
    )

    # Task 4: Archive processed file in GCS
    archive_processed_file = GCSToGCSOperator(
        task_id="archive_processed_file",
        source_bucket=GCS_EXPORT_BUCKET,
        source_object=f"work/{output_gz_filename}",
        destination_bucket=GCS_EXPORT_BUCKET,
        destination_object=f"archive/{output_gz_filename}",
        move_object=True, # Moves the file instead of copying
        gcp_conn_id="google_cloud_default",
    )

    # --- Task Dependencies ---
    extract_transform_bq >> bq_to_gcs_export >> post_process_and_compress_task
    post_process_and_compress_task >> gcs_download_for_sftp >> distribute_to_sftp_corrected
    distribute_to_sftp_corrected >> archive_processed_file

# Note for deployment:
# 1. Ensure `sql/rabatt_data_extraction.sql` is in a folder accessible by Airflow (e.g., dags/sql/).
# 2. Ensure `python/post_process_rabatt_data.py` is in a folder accessible by Airflow (e.g., dags/python/).
# 3. Create Airflow Connection `sftp_apt_rabatt_conn` with appropriate SFTP credentials.
# 4. Create Airflow Variable `sftp_remote_path` for the target SFTP directory.
# 5. Replace "your-gcp-project-id" with your actual GCP Project ID.
# 6. Ensure the GCS buckets `gs://<project_id>-apt-rabatt-export/work/` and `gs://<project_id>-apt-rabatt-export/archive/` exist.
# 7. Grant necessary IAM permissions to the Airflow Service Account for BigQuery, GCS, and SFTP (if using GCE for SFTP).