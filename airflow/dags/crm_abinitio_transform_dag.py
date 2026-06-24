# CRM_ABINITIO_TRANSFORM - Airflow DAG for Ab Initio to GCP Migration
# Legacy Source: Orchestration of customer/customer_transform.xfr and finance/gl_transform.xfr

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocServerlessBatchOperator
from datetime import datetime, timedelta

# --- Configuration Variables ---
# Replace with your actual GCP Project ID and Region
PROJECT_ID = "your-gcp-project-id"
REGION = "us-central1"
GCS_BUCKET_FOR_PYSPARK_FILES = "gs://your-dataproc-code-bucket" # GCS bucket where PySpark files are uploaded

PYSPARK_CUSTOMER_MAIN_FILE = f"{GCS_BUCKET_FOR_PYSPARK_FILES}/pyspark/customer_transform_pyspark.py"
PYSPARK_GL_MAIN_FILE = f"{GCS_BUCKET_FOR_PYSPARK_FILES}/pyspark/gl_transform_pyspark.py"

# Default Entity Code for GL - in a real scenario, this might come from Airflow Variables or a more complex parameter system
DEFAULT_GL_ENTITY_CODE = "GL_ENTITY_A"

# --- DAG Definition ---
with DAG(
    dag_id="crm_abinitio_transform_dag",
    start_date=days_ago(1),
    schedule_interval=timedelta(days=1), # Daily run
    catchup=False,
    tags=["crm", "finance", "dataproc", "pyspark", "bigquery"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    }
) as dag:
    start_task = DummyOperator(
        task_id="start_pipeline",
    )

    # --- Task Group 1: Data Ingestion from Oracle to BigQuery Staging ---
    # These tasks are placeholders. In a real scenario, they would trigger Data Fusion pipelines
    # or check Datastream replication status.
    ingest_crm_staging_data = DummyOperator(
        task_id="ingest_crm_staging_data",
        doc_md="Placeholder for ingesting CRM data (customer profile, sales, campaigns) to BigQuery staging.",
    )

    ingest_finance_staging_data = DummyOperator(
        task_id="ingest_finance_staging_data",
        doc_md="Placeholder for ingesting Finance GL data (transactions, dim_account, rates) to BigQuery staging.",
    )

    # --- Task Group 2: Run customer_transform_pyspark.py on Dataproc Serverless ---
    customer_transform_pyspark_task = DataprocServerlessBatchOperator(
        task_id="customer_transform_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"customer-transform-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        body={
            "sparkBatch": {
                "mainPythonFileUri": PYSPARK_CUSTOMER_MAIN_FILE,
                "args": [
                    f"--run_date={{{{ ds }}}}", # Airflow's 'ds' macro for YYYY-MM-DD
                ],
                "jarFileUris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.28.0.jar"
                ]
            },
            "environmentConfig": {
                "peripheralsConfig": {
                    "sparkHistoryServerConfig": {
                        "dataprocCluster": f"projects/{PROJECT_ID}/regions/{REGION}/clusters/your-spark-history-server-cluster" # Optional
                    }
                }
            }
        },
        gcp_conn_id="google_cloud_default",
        delegate_to=None,
        asynchronous=True,
    )

    # --- Task Group 3: Run gl_transform_pyspark.py on Dataproc Serverless ---
    gl_transform_pyspark_task = DataprocServerlessBatchOperator(
        task_id="gl_transform_pyspark_job",
        project_id=PROJECT_ID,
        region=REGION,
        batch_id=f"gl-transform-{datetime.now().strftime('%Y%m%d%H%M%S')}",
        body={
            "sparkBatch": {
                "mainPythonFileUri": PYSPARK_GL_MAIN_FILE,
                "args": [
                    f"--period_name={{{{ ds_nodash[:6] }}}}", # Extracts YYYYMM from 'ds_nodash'
                    f"--entity_code={DEFAULT_GL_ENTITY_CODE}", # Using a default entity code
                ],
                "jarFileUris": [
                    "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.28.0.jar"
                ]
            },
            "environmentConfig": {
                "peripheralsConfig": {
                    "sparkHistoryServerConfig": {
                        "dataprocCluster": f"projects/{PROJECT_ID}/regions/{REGION}/clusters/your-spark-history-server-cluster" # Optional
                    }
                }
            }
        },
        gcp_conn_id="google_cloud_default",
        asynchronous=True,
    )

    end_task = DummyOperator(
        task_id="end_pipeline",
    )

    # --- Task Dependencies ---
    start_task >> [ingest_crm_staging_data, ingest_finance_staging_data]
    ingest_crm_staging_data >> customer_transform_pyspark_task
    ingest_finance_staging_data >> gl_transform_pyspark_task
    [customer_transform_pyspark_task, gl_transform_pyspark_task] >> end_task