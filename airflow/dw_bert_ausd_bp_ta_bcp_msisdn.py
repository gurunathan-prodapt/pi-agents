# Apache Airflow DAG for DW.BERT_AUSD_BP_TA_BCP_MSISDN
# Replaces: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_BCP_MSISDN.xml
# Job: DW.BERT_AUSD_BP_TA_BCP_MSISDN

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago

# Define your GCP project and region
# TODO: Replace with your actual GCP project ID and region
GCP_PROJECT_ID = "your-gcp-project-id"
GCP_REGION = "your-gcp-region" # e.g., "us-central1"

# TODO: Replace with your actual Dataproc cluster name
DATAPROC_CLUSTER_NAME = "your-dataproc-cluster-name"

# TODO: Replace with the GCS bucket where your Python script is uploaded
# e.g., "gs://your-bucket-name/pyspark_scripts/r_ausd_bp_ta_bcp_msisdn.py"
PYTHON_SCRIPT_GCS_PATH = "gs://your-gcs-bucket/pyspark_scripts/r_ausd_bp_ta_bcp_msisdn.py"

# TODO: Replace with your BigQuery dataset for data tables
BIGQUERY_DATASET_NAME = "your_bigquery_dataset"
# TODO: Replace with your BigQuery dataset for metadata tables (if different from default)
METADATA_DATASET_NAME = "isbert_schema"

default_args = {
    'owner': 'DW.UNIX.ISBERT',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    # 'retry_delay': timedelta(minutes=5), # Uncomment and set if retries are desired
}

with DAG(
    dag_id='dw_bert_ausd_bp_ta_bcp_msisdn',
    default_args=default_args,
    description='Orchestrates MSISDN basic product data processing using Dataproc and BigQuery',
    start_date=days_ago(1), # This will set the first run to yesterday
    schedule_interval=None, # As no schedule was specified in UC4 XML, setting to None for manual/external trigger
    catchup=False,
    tags=['bert', 'msisdn', 'dataproc', 'bigquery'],
) as dag:
    run_dataproc_job = DataprocSubmitJobOperator(
        task_id='run_dw_bert_ausd_bp_ta_bcp_msisdn',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "placement": {
                "cluster_name": DATAPROC_CLUSTER_NAME
            },
            "pyspark_job": {
                "main_python_file_uri": PYTHON_SCRIPT_GCS_PATH,
                "args": [
                    f"--gcp_project={GCP_PROJECT_ID}",
                    f"--bigquery_dataset={BIGQUERY_DATASET_NAME}",
                    f"--metadata_dataset={METADATA_DATASET_NAME}",
                    # You can pass 'stichtag' dynamically here if needed, e.g.,
                    # f"--stichtag={{{{ ds_nodash }}}}", # passes current DAG run date as YYYYMMDD
                    # For now, it will be determined by the Python script if not provided.
                ],
                "properties": {
                    # Example: Add properties if your script needs them, e.g.
                    # "spark.yarn.queue": "the-queue",
                    # "spark.executor.memory": "2g",
                },
                # You might need additional python file URIs or JARs
                # "python_file_uris": ["gs://your-bucket/libs/some_utility.py"],
                # "jar_file_uris": ["gs://your-bucket/jars/some_connector.jar"],
            },
        },
    )