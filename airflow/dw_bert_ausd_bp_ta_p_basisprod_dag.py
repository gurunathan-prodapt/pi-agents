# Migrated from vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_P_BASISPROD.xml
# Job: DW.BERT_AUSD_BP_TA_P_BASISPROD

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# --- User-defined variables ---
# Update these values to match your GCP environment and GCS paths
GCP_PROJECT_ID = "your-gcp-project-id"  # e.g., "my-gcp-project"
DATAPROC_CLUSTER_NAME = "your-dataproc-cluster-name" # e.g., "bert-dataproc-cluster"
GCP_REGION = "your-gcp-region"  # e.g., "us-central1"
GCS_BUCKET_NAME = "your-gcs-bucket-name" # e.g., "gs://my-data-lake-bucket"

# GCS paths for PySpark script and BigQuery SQL file
PYSPARK_SCRIPT_GCS_PATH = f"{GCS_BUCKET_NAME}/pyspark_scripts/k_ausd_bp_ta_p_basisprod.py"
BIGQUERY_SQL_FILE_GCS_PATH = f"{GCS_BUCKET_NAME}/sql/d_ausd_bp_ta_p_basisprod_bq.sql"
# --- End User-defined variables ---

with DAG(
    dag_id="dw_bert_ausd_bp_ta_p_basisprod",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"), # Adjust as per business requirements
    schedule=None, # Define schedule based on business requirements, e.g., "@daily"
    catchup=False,
    tags=["bert", "basisprod", "dwh"],
    description="Airflow DAG for DW.BERT_AUSD_BP_TA_P_BASISPROD, migrating UC4 and KornShell logic to PySpark and BigQuery.",
) as dag:
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="run_bert_ausd_bp_ta_p_basisprod",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            "placement": {"cluster_name": DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_SCRIPT_GCS_PATH,
                "args": [
                    "--stichtag",
                    "{{ ds_nodash }}",  # Pass execution date as YYYYMMDD
                    "--wiederanlaufwert",
                    "0", # Default value, can be parameterized via Airflow variables if needed
                    "--sql_file_gcs_path",
                    BIGQUERY_SQL_FILE_GCS_PATH,
                    "--project_id",
                    GCP_PROJECT_ID,
                ],
                "properties": {
                    "spark.logConf": "true",
                    "spark.dynamicAllocation.enabled": "false",
                    "spark.executor.instances": "2",
                    "spark.executor.cores": "2",
                    "spark.executor.memory": "4g",
                    "spark.driver.cores": "1",
                    "spark.driver.memory": "2g",
                },
                "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
            },
        },
    )