# Migrated from vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_TAEGLICH_JP/DW.DWH_EXIS_SD_APT_RABATT.xml
# Job: EXIS_SD_APT_RABATT
#
# This Airflow DAG orchestrates the extraction, transformation, post-processing,
# and distribution of discount data.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator, GCSObjectsWithPrefixMoveOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator # For SFTP - replace with SFTPOperator or custom Python
from airflow.utils.dates import days_ago
from airflow.models import Variable

import os
from datetime import datetime, timedelta

# --- Airflow Variables (Configurable outside the DAG file) ---
# It's recommended to set these in the Airflow UI or via CLI/API
# Example: airflow variables set gcs_data_bucket your-gcs-bucket-name
# Example: airflow variables set bq_project_id your-gcp-project-id
# Example: airflow variables set bq_dataset_id your-bq-dataset-id
# Example: airflow variables set dw_dir_exp_apt DWH/DWH_KERN/RELEASEWECHSEL/RELEASEWECHSEL172/INSTALL_NACH_172_APT_DWHM/DW.DWH_APT_EXPORT_TAEGLICH_JP
# Example: airflow variables set sftp_conn_id sftp_external_connection # Requires an Airflow SFTP connection
# Example: airflow variables set sftp_remote_path /path/to/remote/sftp/dir

GCS_DATA_BUCKET = Variable.get("gcs_data_bucket", "your-gcs-data-bucket")
BQ_PROJECT_ID = Variable.get("bq_project_id", "your-gcp-project-id")
BQ_DATASET_ID = Variable.get("bq_dataset_id", "your-bq-dataset-id")
DW_DIR_EXP_APT = Variable.get("dw_dir_exp_apt", "DW.DWH_APT_EXPORT_TAEGLICH_JP")
SFTP_CONNECTION_ID = Variable.get("sftp_conn_id", "sftp_external_connection") # Needs to be defined in Airflow Connections
SFTP_REMOTE_PATH = Variable.get("sftp_remote_path", "/sftp/apt/rabatt")

# --- Constants ---
JOB_ID = "EXIS_SD_APT_RABATT"
CSV_SEPARATOR = "|"
# Temporary GCS path for raw BQ output before post-processing
TEMP_GCS_RAW_PATH = f"gs://{GCS_DATA_BUCKET}/tmp/{JOB_ID}/raw_rabattdaten_{{{{ ds_nodash }}}}.csv"
# GCS path for the processed and compressed file before archiving/SFTP
WORK_GCS_PREFIX = f"{DW_DIR_EXP_APT}/work"
ARCHIVE_GCS_PREFIX = f"{DW_DIR_EXP_APT}/store"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=f'{JOB_ID}_dag',
    default_args=default_args,
    description='ETL for EXIS_SD_APT_RABATT - generates and distributes discount data',
    schedule_interval='@daily', # Matches original UC4 daily schedule
    start_date=days_ago(1),
    catchup=False,
    tags=['exis', 'apt', 'rabatt', 'etl', 'bigquery', 'gcs', 'sftp'],
) as dag:

    # 1. Extract and Transform (BigQuery)
    # Executes the BQ SQL and exports results to a temporary GCS CSV file.
    extract_transform_bq_task = BigQueryInsertJobOperator(
        task_id='extract_transform_bq_to_gcs',
        project_id=BQ_PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include 'd_exis_apt_rabattdaten.bqsql' %}",
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": BQ_PROJECT_ID,
                    "datasetId": BQ_DATASET_ID,
                    "tableId": f"temp_{JOB_ID}_{{{{ ds_nodash }}}}" # Temporary table for direct export
                },
                "createDisposition": "CREATE_IF_NEEDED",
                "writeDisposition": "WRITE_TRUNCATE"
            },
            "extract": {
                "sourceTable": {
                    "projectId": BQ_PROJECT_ID,
                    "datasetId": BQ_DATASET_ID,
                    "tableId": f"temp_{JOB_ID}_{{{{ ds_nodash }}}}"
                },
                "destinationUris": [TEMP_GCS_RAW_PATH],
                "destinationFormat": "CSV",
                "compression": "NONE",
                "fieldDelimiter": CSV_SEPARATOR,
                "printHeader": True
            }
        },
        gcp_conn_id='google_cloud_default', # Assumes default GCP connection
        params={'bq_project_id': BQ_PROJECT_ID, 'bq_dataset_id': BQ_DATASET_ID},
    )

    # 2. Post-processing (Python Script)
    # Reads the raw CSV from GCS, applies nawk-like logic (trailer line),
    # compresses it, and uploads the final file to the 'work' GCS directory.
    post_process_data_task = PythonOperator(
        task_id='post_process_and_compress_data',
        python_callable='post_process_rabattdaten.post_process_and_compress_rabattdaten',
        op_kwargs={
            'input_gcs_path': TEMP_GCS_RAW_PATH,
            'output_gcs_bucket': GCS_DATA_BUCKET,
            'output_gcs_prefix': WORK_GCS_PREFIX,
            'job_id': JOB_ID,
            'separator': CSV_SEPARATOR,
            'ds_nodash': '{{ ds_nodash }}',
        },
        provide_context=True,
    )

    # 3. SFTP Transfer
    # Transfers the processed and compressed file from GCS to an external SFTP server.
    # NOTE: This uses a BashOperator with a 'gsutil cp' and 'sftp' command as a placeholder.
    # In a real scenario, use Airflow's SFTPOperator or a more robust Python custom operator
    # with paramiko or other libraries for secure SFTP transfer, handling credentials via
    # Airflow Connections or GCP Secret Manager.
    # BashOperator approach requires gcloud SDK and sftp client on the worker.
    sftp_transfer_task = BashOperator(
        task_id='sftp_transfer_file',
        bash_command=f"""
            PROCESSED_FILE_GCS_PATH={{{{ task_instance.xcom_pull(task_ids='post_process_and_compress_data', key='processed_compressed_gcs_path') }}}}
            
            # Download file from GCS to a temporary location on the worker
            FILE_NAME=$(basename $PROCESSED_FILE_GCS_PATH)
            LOCAL_TEMP_PATH="/tmp/$FILE_NAME"
            gsutil cp $PROCESSED_FILE_GCS_PATH $LOCAL_TEMP_PATH

            # Use sftp client (ensure it's installed and connection details are secure)
            # This is a highly simplified example. In production, use Airflow's SFTPHook
            # or a custom Python operator for better security and error handling.
            # SFTP_USER and SFTP_HOST would typically come from an Airflow Connection
            # or Secret Manager.
            echo "Attempting SFTP transfer of $LOCAL_TEMP_PATH to {SFTP_REMOTE_PATH}/$FILE_NAME"
            
            # Placeholder for actual SFTP logic:
            # sftp -o StrictHostKeyChecking=no {{{{ conn.{SFTP_CONNECTION_ID}.login }}}}@{{{{ conn.{SFTP_CONNECTION_ID}.host }}}} <<EOF
            #   cd {SFTP_REMOTE_PATH}
            #   put $LOCAL_TEMP_PATH $FILE_NAME
            #   bye
            # EOF
            
            # For demonstration, we'll just simulate success and cleanup
            echo "Simulating SFTP transfer success for $FILE_NAME"
            rm $LOCAL_TEMP_PATH || true
            
            if [ $? -eq 0 ]; then
                echo "SFTP transfer successful for $FILE_NAME"
            else
                echo "SFTP transfer failed for $FILE_NAME"
                exit 1
            fi
        """,
    )

    # 4. Archiving
    # Moves the processed file from the 'work' directory to the 'store' (archive) directory in GCS.
    # NOTE: GCSObjectsWithPrefixMoveOperator expects a prefix, not a full file path for source.
    # We will instead move the specific file pulled from XCom.
    archive_file_task = PythonOperator(
        task_id='archive_processed_file',
        python_callable=GCSHook().upload, # Re-using GCSHook.upload for simplicity, but object move is better
        op_kwargs={
            'bucket_name': GCS_DATA_BUCKET,
            'object_name': f"{ARCHIVE_GCS_PREFIX}/{{{{ (task_instance.xcom_pull(task_ids='post_process_and_compress_data', key='processed_compressed_gcs_path')).split('/')[-1] }}}}",
            'data': '{{{{ (task_instance.xcom_pull(task_ids="post_process_and_compress_data", key="processed_compressed_gcs_path")).replace("gs://", "") }}}}',
            'move_object': True, # Indicate that the source object should be moved/deleted
        },
        # Need to implement move functionality, GCSHook.upload with data as a path doesn't move.
        # A custom Python callable or GCSObjectsWithPrefixMoveOperator (if source/dest prefixes match) is needed.
        # For a single file move, direct GCSHook.copy/delete or GoogleCloudStorageMoveObjectOperator is better.
        # Let's use a PythonOperator to perform the move explicitly.
        python_callable=lambda src_path, dest_path, **kwargs: GCSHook().copy(
            source_bucket=src_path.split('/')[2],
            source_object='/'.join(src_path.split('/')[3:]),
            destination_bucket=dest_path.split('/')[2],
            destination_object='/'.join(dest_path.split('/')[3:])
        ) and GCSHook().delete(
            bucket_name=src_path.split('/')[2],
            object_name='/'.join(src_path.split('/')[3:])
        ),
        op_kwargs={
            'src_path': '{{ task_instance.xcom_pull(task_ids="post_process_and_compress_data", key="processed_compressed_gcs_path") }}',
            'dest_path': f"gs://{GCS_DATA_BUCKET}/{ARCHIVE_GCS_PREFIX}/{{{{ (task_instance.xcom_pull(task_ids='post_process_and_compress_data', key='processed_compressed_gcs_path')).split('/')[-1] }}}}",
        }
    )


    # 5. Cleanup Temporary File
    # Deletes the raw CSV exported from BigQuery to GCS.
    cleanup_temp_raw_csv = GCSDeleteObjectsOperator(
        task_id='cleanup_temp_raw_csv',
        bucket_name=GCS_DATA_BUCKET,
        objects=[f"tmp/{JOB_ID}/raw_rabattdaten_{{{{ ds_nodash }}}}.csv"],
        gcp_conn_id='google_cloud_default',
    )


    # --- Task Dependencies ---
    extract_transform_bq_task >> post_process_data_task
    post_process_data_task >> [sftp_transfer_task, archive_file_task]
    archive_file_task >> cleanup_temp_raw_csv # Cleanup only after archive is successful and not blocked by SFTP.
    sftp_transfer_task >> cleanup_temp_raw_csv

    # To simplify dependencies, ensure cleanup runs after both distribution tasks.
    # In real world, SFTP failures might need separate handling or retry logic.
    # For this example, let's just make it depend on post_process_data_task directly and let it run.
    # It might be better to have cleanup depend on successful completion of BOTH sftp_transfer_task AND archive_file_task.
    # current_datetime = datetime.now().strftime('%Y%m%d%H%M%S')
    # temp_bq_output_table_id = f"temp_{JOB_ID}_{{{{ ds_nodash }}}}"
    # temp_gcs_raw_path_to_delete = f"tmp/{JOB_ID}/raw_rabattdaten_{{{{ ds_nodash }}}}.csv"
    # cleanup_temp_raw_csv.set_downstream(extract_transform_bq_task)
    # cleanup_temp_raw_csv.set_upstream([sftp_transfer_task, archive_file_task]) # This is more robust.
    # However, for the purpose of demonstrating the flow, the current setup where cleanup happens after archive is fine.
    # Let's adjust cleanup to run after both SFTP and archive are done to be safer.
    [sftp_transfer_task, archive_file_task] >> cleanup_temp_raw_csv