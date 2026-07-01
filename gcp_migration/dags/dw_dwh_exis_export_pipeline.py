import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator

# Dynamically configure python load path for the auxiliary utilities module
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
EXPORTER_BIN_DIR = os.path.join(os.path.dirname(DAG_DIR), 'exporter', 'apt', 'bin')
if EXPORTER_BIN_DIR not in sys.path:
    sys.path.insert(0, EXPORTER_BIN_DIR)

from add_trailer_and_compress import append_trailer_and_gzip_gcs

# Load environmental variables securely with fallback to default targets
GCP_PROJECT_ID = Variable.get("gcp_project_id", "prod-dwh-gcp-project")
BQ_DATASET_RAW = Variable.get("bq_dataset_raw", "prod_dwh_raw_dataset")
GCS_TEMP_BUCKET = Variable.get("gcs_temp_bucket", "prod-dwh-exporter-temp")
GCS_STORE_BUCKET = Variable.get("gcs_store_bucket", "prod-dwh-exporter-store")
SFTP_CONN_ID = Variable.get("sftp_connection_id", "ssh_sftp_apt_receiver")
SFTP_REMOTE_DIR = Variable.get("sftp_remote_dir", "/incoming/apt_exports")
AIRFLOW_OWNER = Variable.get("airflow_owner", "data_engineering_exports")

def get_sql_query(filename):
    """Reads the content of SQL files from the migrated folder structure."""
    path = os.path.join(os.path.dirname(DAG_DIR), 'exporter', 'apt', 'sql', filename)
    with open(path, 'r') as f:
        return f.read()

def load_query_safe(filename):
    """Protects the DAG validation process in case files are loaded asynchronously."""
    try:
        return get_sql_query(filename)
    except Exception as e:
        return f"-- Error loading {filename}: {str(e)}"

default_args = {
    'owner': AIRFLOW_OWNER,
    'depends_on_past': False, 
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_dwh_exis_export_pipeline',
    default_args=default_args,
    description='Modernized GCP-native EXIS DWH Exporter Pipeline',
    schedule_interval=None,  # Configured by scheduler metadata accordingly
    catchup=False,
    max_active_runs=1,
) as dag:

    # ------------------------------------------------------------------------
    # PIPELINE 1: Stock Data Export (Bestandsdaten - Daily)
    # ------------------------------------------------------------------------

    extract_bestandsdaten = BigQueryInsertJobOperator(
        task_id='extract_bestandsdaten_query',
        configuration={
            "query": {
                "query": load_query_safe('d_exis_apt_bestandsdaten.sql'),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET_RAW,
                    "tableId": "temp_bestandsdaten_export"
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    spool_bestandsdaten = BigQueryToGCSOperator(
        task_id='spool_bestandsdaten_to_gcs',
        source_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.temp_bestandsdaten_export",
        destination_cloud_storage_uris=[f"gs://{GCS_TEMP_BUCKET}/bestandsdaten_raw.csv"],
        export_format="CSV",
        field_delimiter="|",
        print_header=False
    )

    post_process_bestandsdaten = PythonOperator(
        task_id='post_process_bestandsdaten',
        python_callable=append_trailer_and_gzip_gcs,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "source_bucket_name": GCS_TEMP_BUCKET,
            "dest_bucket_name": GCS_STORE_BUCKET,
            "source_blob_name": "bestandsdaten_raw.csv",
            "dest_blob_name": "work/DWHM_APT_BESTANDSREPORT_{{ ts_nodash }}.csv.gz",
            "report_type": "V_S_Bestandsreport",
            "from_date": "{{ ds_nodash }}",
            "separator": "|"
        }
    )

    download_bestandsdaten = GCSToLocalFilesystemOperator(
        task_id='download_bestandsdaten_to_local',
        bucket_name=GCS_STORE_BUCKET,
        object_name="work/DWHM_APT_BESTANDSREPORT_{{ ts_nodash }}.csv.gz",
        filename="/tmp/DWHM_APT_BESTANDSREPORT_{{ ts_nodash }}.csv.gz"
    )

    sftp_bestandsdaten = SFTPOperator(
        task_id='sftp_transfer_bestandsdaten',
        ssh_conn_id=SFTP_CONN_ID,
        local_filepath="/tmp/DWHM_APT_BESTANDSREPORT_{{ ts_nodash }}.csv.gz",
        remote_filepath=f"{SFTP_REMOTE_DIR}/DWHM_APT_BESTANDSREPORT_{{{{ ts_nodash }}}}.csv.gz",
        operation="put"
    )

    cleanup_bestandsdaten = BashOperator(
        task_id='cleanup_bestandsdaten',
        bash_command="rm -f /tmp/DWHM_APT_BESTANDSREPORT_{{ ts_nodash }}.csv.gz"
    )

    extract_bestandsdaten >> spool_bestandsdaten >> post_process_bestandsdaten >> download_bestandsdaten >> sftp_bestandsdaten >> cleanup_bestandsdaten

    # ------------------------------------------------------------------------
    # PIPELINE 2: GPRS Data Export (NNA Daten - Monthly)
    # ------------------------------------------------------------------------

    extract_nna_daten = BigQueryInsertJobOperator(
        task_id='extract_nna_daten_query',
        configuration={
            "query": {
                "query": load_query_safe('d_exis_apt_nna_daten.sql'),
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "MONAT_ID",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {"value": "{{ ds_nodash[:6] }}"}
                    }
                ],
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET_RAW,
                    "tableId": "temp_nna_daten_export"
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    spool_nna_daten = BigQueryToGCSOperator(
        task_id='spool_nna_daten_to_gcs',
        source_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.temp_nna_daten_export",
        destination_cloud_storage_uris=[f"gs://{GCS_TEMP_BUCKET}/nna_daten_raw.csv"],
        export_format="CSV",
        field_delimiter="|",
        print_header=False
    )

    post_process_nna_daten = PythonOperator(
        task_id='post_process_nna_daten',
        python_callable=append_trailer_and_gzip_gcs,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "source_bucket_name": GCS_TEMP_BUCKET,
            "dest_bucket_name": GCS_STORE_BUCKET,
            "source_blob_name": "nna_daten_raw.csv",
            "dest_blob_name": "work/DWHM_APT_NNA_Daten_{{ ts_nodash }}.csv.gz",
            "report_type": "V_F_NNA_Daten",
            "from_date": "{{ ds_nodash[:6] }}01",
            "separator": "|"
        }
    )

    download_nna_daten = GCSToLocalFilesystemOperator(
        task_id='download_nna_daten_to_local',
        bucket_name=GCS_STORE_BUCKET,
        object_name="work/DWHM_APT_NNA_Daten_{{ ts_nodash }}.csv.gz",
        filename="/tmp/DWHM_APT_NNA_Daten_{{ ts_nodash }}.csv.gz"
    )

    sftp_nna_daten = SFTPOperator(
        task_id='sftp_transfer_nna_daten',
        ssh_conn_id=SFTP_CONN_ID,
        local_filepath="/tmp/DWHM_APT_NNA_Daten_{{ ts_nodash }}.csv.gz",
        remote_filepath=f"{SFTP_REMOTE_DIR}/DWHM_APT_NNA_Daten_{{{{ ts_nodash }}}}.csv.gz",
        operation="put"
    )

    cleanup_nna_daten = BashOperator(
        task_id='cleanup_nna_daten',
        bash_command="rm -f /tmp/DWHM_APT_NNA_Daten_{{ ts_nodash }}.csv.gz"
    )

    extract_nna_daten >> spool_nna_daten >> post_process_nna_daten >> download_nna_daten >> sftp_nna_daten >> cleanup_nna_daten

    # ------------------------------------------------------------------------
    # PIPELINE 3: Voice Data Export (NNA Voice - Monthly)
    # ------------------------------------------------------------------------

    extract_nna_voice = BigQueryInsertJobOperator(
        task_id='extract_nna_voice_query',
        configuration={
            "query": {
                "query": load_query_safe('d_exis_apt_nna_voice.sql'),
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "MONAT_ID",
                        "parameterType": {"type": "INT64"},
                        "parameterValue": {"value": "{{ ds_nodash[:6] }}"}
                    }
                ],
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET_RAW,
                    "tableId": "temp_nna_voice_export"
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    spool_nna_voice = BigQueryToGCSOperator(
        task_id='spool_nna_voice_to_gcs',
        source_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.temp_nna_voice_export",
        destination_cloud_storage_uris=[f"gs://{GCS_TEMP_BUCKET}/nna_voice_raw.csv"],
        export_format="CSV",
        field_delimiter="|",
        print_header=False
    )

    post_process_nna_voice = PythonOperator(
        task_id='post_process_nna_voice',
        python_callable=append_trailer_and_gzip_gcs,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "source_bucket_name": GCS_TEMP_BUCKET,
            "dest_bucket_name": GCS_STORE_BUCKET,
            "source_blob_name": "nna_voice_raw.csv",
            "dest_blob_name": "work/DWHM_APT_NNA_Voice_{{ ts_nodash }}.csv.gz",
            "report_type": "V_F_NNA_Voice",
            "from_date": "{{ ds_nodash[:6] }}01",
            "separator": "|"
        }
    )

    download_nna_voice = GCSToLocalFilesystemOperator(
        task_id='download_nna_voice_to_local',
        bucket_name=GCS_STORE_BUCKET,
        object_name="work/DWHM_APT_NNA_Voice_{{ ts_nodash }}.csv.gz",
        filename="/tmp/DWHM_APT_NNA_Voice_{{ ts_nodash }}.csv.gz"
    )

    sftp_nna_voice = SFTPOperator(
        task_id='sftp_transfer_nna_voice',
        ssh_conn_id=SFTP_CONN_ID,
        local_filepath="/tmp/DWHM_APT_NNA_Voice_{{ ts_nodash }}.csv.gz",
        remote_filepath=f"{SFTP_REMOTE_DIR}/DWHM_APT_NNA_Voice_{{{{ ts_nodash }}}}.csv.gz",
        operation="put"
    )

    cleanup_nna_voice = BashOperator(
        task_id='cleanup_nna_voice',
        bash_command="rm -f /tmp/DWHM_APT_NNA_Voice_{{ ts_nodash }}.csv.gz"
    )

    extract_nna_voice >> spool_nna_voice >> post_process_nna_voice >> download_nna_voice >> sftp_nna_voice >> cleanup_nna_voice

    # ------------------------------------------------------------------------
    # PIPELINE 4: Discount Data Export (Rabattdaten - Daily)
    # ------------------------------------------------------------------------

    extract_rabattdaten = BigQueryInsertJobOperator(
        task_id='extract_rabattdaten_query',
        configuration={
            "query": {
                "query": load_query_safe('d_exis_apt_rabattdaten.sql'),
                "useLegacySql": False,
                "destinationTable": {
                    "projectId": GCP_PROJECT_ID,
                    "datasetId": BQ_DATASET_RAW,
                    "tableId": "temp_rabattdaten_export"
                },
                "writeDisposition": "WRITE_TRUNCATE"
            }
        }
    )

    spool_rabattdaten = BigQueryToGCSOperator(
        task_id='spool_rabattdaten_to_gcs',
        source_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.temp_rabattdaten_export",
        destination_cloud_storage_uris=[f"gs://{GCS_TEMP_BUCKET}/rabattdaten_raw.csv"],
        export_format="CSV",
        field_delimiter="|",
        print_header=False
    )

    post_process_rabattdaten = PythonOperator(
        task_id='post_process_rabattdaten',
        python_callable=append_trailer_and_gzip_gcs,
        op_kwargs={
            "project_id": GCP_PROJECT_ID,
            "source_bucket_name": GCS_TEMP_BUCKET,
            "dest_bucket_name": GCS_STORE_BUCKET,
            "source_blob_name": "rabattdaten_raw.csv",
            "dest_blob_name": "work/DWHM_APT_RABATTREPORT_{{ ts_nodash }}.csv.gz",
            "report_type": "V_S_Rabattreport",
            "from_date": "{{ ds_nodash }}",
            "separator": "|"
        }
    )

    download_rabattdaten = GCSToLocalFilesystemOperator(
        task_id='download_rabattdaten_to_local',
        bucket_name=GCS_STORE_BUCKET,
        object_name="work/DWHM_APT_RABATTREPORT_{{ ts_nodash }}.csv.gz",
        filename="/tmp/DWHM_APT_RABATTREPORT_{{ ts_nodash }}.csv.gz"
    )

    sftp_rabattdaten = SFTPOperator(
        task_id='sftp_transfer_rabattdaten',
        ssh_conn_id=SFTP_CONN_ID,
        local_filepath="/tmp/DWHM_APT_RABATTREPORT_{{ ts_nodash }}.csv.gz",
        remote_filepath=f"{SFTP_REMOTE_DIR}/DWHM_APT_RABATTREPORT_{{{{ ts_nodash }}}}.csv.gz",
        operation="put"
    )

    cleanup_rabattdaten = BashOperator(
        task_id='cleanup_rabattdaten',
        bash_command="rm -f /tmp/DWHM_APT_RABATTREPORT_{{ ts_nodash }}.csv.gz"
    )

    extract_rabattdaten >> spool_rabattdaten >> post_process_rabattdaten >> download_rabattdaten >> sftp_rabattdaten >> cleanup_rabattdaten