from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.operators.python import ShortCircuitOperator, PythonOperator

# Retrieve environment-wide global variable configurations
GCP_PROJECT = Variable.get("GCP_PROJECT")
GCP_REGION = Variable.get("GCP_REGION", default_var="europe-west3")
GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_DATASET = Variable.get("BQ_DATASET")

# Job-specific variables derived verbatim from map_rpos_carmen_import.cfg
JOB_CONFIG = {
    "BHB_Projektverzeichnis": "/Projects/TMD/processing/BHB/BD_PROC",
    "BHB_Version": "RLS_BHB_nach_64_rabatt_sap",
    "BHB_Graph": "map_rpos_carmen_import",
    "BHB_Prozesstyp": "D",
    "BHB_Quellverzeichnis": "crs/work/",
    "BHB_Zielverzeichnis": "crs/store/",
    "BHB_Dateimaske": "CARMEN_B_*_pos.fix",
    "BHB_Kopfdatensatzkennung": "H",
    "BHB_Nutzdatensatzkennung": "P",
    "BHB_Endedatensatzkennung": "X",
}

def archive_processed_files(bucket_name, src_dir, dst_dir, file_mask):
    from google.cloud import storage
    import fnmatch
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=src_dir)
    for blob in blobs:
        filename = blob.name.split('/')[-1]
        if fnmatch.fnmatch(filename, file_mask):
            new_name = f"{dst_dir}{filename}"
            # Copy and delete to simulate move
            bucket.copy_blob(blob, bucket, new_name)
            blob.delete()

default_args = {
    'owner': 'dwh_admin',
    'start_date': datetime(2005, 4, 1),
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dw_rpos_carm_import',
    default_args=default_args,
    schedule_interval=None,  # Event-triggered or triggered by upstream orchestrator
    catchup=False,
    max_active_runs=1,
) as dag:

    # Task 1: List incoming files in GCS (replaces file mask search)
    list_incoming_files = GCSListObjectsOperator(
        task_id='list_incoming_files',
        bucket=GCS_BUCKET,
        prefix=JOB_CONFIG["BHB_Quellverzeichnis"],
    )

    # Task 2: Validate file existence matching the mask
    def check_for_input_files(ti):
        files = ti.xcom_pull(task_ids='list_incoming_files')
        # Filter files matching 'CARMEN_B_*_pos.fix'
        matched_files = [f for f in files if "CARMEN_B_" in f and f.endswith("_pos.fix")]
        return len(matched_files) > 0

    validate_inputs = ShortCircuitOperator(
        task_id='validate_inputs',
        python_callable=check_for_input_files,
    )

    # Task 3: Dataproc Serverless PySpark Batch submission
    run_pyspark_graph = DataprocCreateBatchOperator(
        task_id='run_map_rpos_carmen_import',
        project_id=GCP_PROJECT,
        region=GCP_REGION,
        batch_id='map-rpos-carm-import-{{ ds_nodash }}-{{ task_instance.try_number }}',
        batch={
            'pyspark_batch': {
                'main_python_file_uri': f'gs://{GCS_BUCKET}/code/abinitio_rpos_carmen_linked_job/TMD_processing/BHB/BD_PROC/run/map_rpos_carmen_import.py',
                'args': [
                    '--bucket', GCS_BUCKET,
                    '--dataset', BQ_DATASET,
                    '--project', GCP_PROJECT,
                    '--entry-nr', '{{ dag_run.conf.get("entry_nr", "0") }}',
                    '--file-mask', JOB_CONFIG["BHB_Dateimaske"],
                    '--src-dir', JOB_CONFIG["BHB_Quellverzeichnis"],
                    '--dst-dir', JOB_CONFIG["BHB_Zielverzeichnis"],
                ],
            },
            'environment_config': {
                'execution_config': {
                    'subnetwork_uri': 'default',
                }
            }
        }
    )

    # Task 4: Archive processed files to the storage directory
    archive_files = PythonOperator(
        task_id='archive_files',
        python_callable=archive_processed_files,
        op_kwargs={
            'bucket_name': GCS_BUCKET,
            'src_dir': JOB_CONFIG["BHB_Quellverzeichnis"],
            'dst_dir': JOB_CONFIG["BHB_Zielverzeichnis"],
            'file_mask': JOB_CONFIG["BHB_Dateimaske"],
        }
    )

    list_incoming_files >> validate_inputs >> run_pyspark_graph >> archive_files