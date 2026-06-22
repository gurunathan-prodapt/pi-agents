# Migrated from UC4 job: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_VERTRAG_JP/DW.BERT_AUSD_V_TA_CNTRCT_CRS2.xml
# Job: DW.BERT_AUSD_V_TA_CNTRCT_CRS2

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# --- GCP Configuration Placeholders ---
# IMPORTANT: Replace these with your actual GCP project details and Dataproc cluster info.
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
GCP_DATAPROC_REGION = "YOUR_DATAPROC_REGION"  # e.g., "us-central1"
GCP_DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"  # e.g., "composer-dataproc-cluster"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME"  # GCS bucket where PySpark script and SQL script are stored

# PySpark script path on GCS
PYSPARK_GCS_PATH = f"gs://{GCS_BUCKET_NAME}/dataproc_jobs/r_ausd_v_ta_cntrct_crs2.py"
# BigQuery SQL script path on GCS (will be passed to PySpark script)
BIGQUERY_SQL_GCS_PATH = f"gs://{GCS_BUCKET_NAME}/dataproc_jobs/d_ausd_v_ta_cntrct_crs2_bq.sql"

# --- DAG Definition ---
with DAG(
    dag_id="dw_bert_ausd_v_ta_cntrct_crs2",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # No explicit schedule found in UC4. Set based on business requirements.
    catchup=False,
    tags=["bert", "contract", "dataproc", "bigquery"],
    description="ETL workflow for updating contract data (ta_cntrct_crs2) from Oracle UC4.",
) as dag:
    start_task = DummyOperator(task_id="start")

    # Upload the BigQuery SQL file to GCS
    # This is a preparatory step for the PySpark script to access the SQL.
    # In a production environment, this file should already be present in GCS.
    # We are demonstrating how to get the file to Dataproc worker nodes.
    # For a real scenario, consider using `gcs_to_gcs_operator` or ensuring the file is deployed.
    # Here, we assume the BigQuery SQL file is available to the PySpark script.

    # Dataproc job submission to run the PySpark wrapper script
    run_bert_ausd_v_ta_cntrct_crs2 = DataprocSubmitJobOperator(
        task_id="run_bert_ausd_v_ta_cntrct_crs2",
        project_id=GCP_PROJECT_ID,
        region=GCP_DATAPROC_REGION,
        job={
            "placement": {"cluster_name": GCP_DATAPROC_CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": PYSPARK_GCS_PATH,
                "file_uris": [BIGQUERY_SQL_GCS_PATH],  # Make the SQL script available to the PySpark job
                "properties": {
                    "spark.yarn.appMasterEnv.GCP_PROJECT_ID": GCP_PROJECT_ID,
                    "spark.executorEnv.GCP_PROJECT_ID": GCP_PROJECT_ID,
                    "spark.yarn.appMasterEnv.BIGQUERY_SQL_SCRIPT_PATH": os.path.basename(BIGQUERY_SQL_GCS_PATH),
                    "spark.executorEnv.BIGQUERY_SQL_SCRIPT_PATH": os.path.basename(BIGQUERY_SQL_GCS_PATH),
                },
                "args": [
                    f"--project_id={GCP_PROJECT_ID}",
                    f"--sql_script_path={os.path.basename(BIGQUERY_SQL_GCS_PATH)}", # PySpark script expects local path
                ],
            },
        },
    )

    end_task = DummyOperator(task_id="end")

    start_task >> run_bert_ausd_v_ta_cntrct_crs2 >> end_task