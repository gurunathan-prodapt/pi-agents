#
# Airflow DAG for DW.BERT_AUSD_BP_TA_RN_VERTRAG
# Legacy source: vobs/dw_source/isdwh/uc4_prod_exports/UC4_PROD - 0001/DWH/BERT/PRODUKTION/DW.BERT_STAMMDATEN_JP/DW.BERT_P_BASISPRODUKT_JP/DW.BERT_AUSD_BP_TA_RN_VERTRAG.xml
# Target platform: Apache Airflow on Cloud Composer
#

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitPySparkJobOperator

# Define GCP project and environment details (PLACEHOLDERS - REPLACE WITH ACTUAL VALUES)
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DATAPROC_REGION = "YOUR_DATAPROC_REGION"
DATAPROC_CLUSTER_NAME = "YOUR_DATAPROC_CLUSTER_NAME"
GCS_BUCKET_NAME = "YOUR_BUCKET_NAME" # GCS bucket where Python script and SQL file are uploaded

# Paths to the job script and SQL file in GCS
DATAPROC_JOB_PATH = f"gs://{GCS_BUCKET_NAME}/dataproc/dataproc_job.py"
BIGQUERY_SQL_PATH = f"gs://{GCS_BUCKET_NAME}/sql/d_ausd_bp_ta_rn_vertrag_bq.sql"

with DAG(
    dag_id="dw_bert_ausd_bp_ta_rn_vertrag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # As per design document, set schedule based on business requirements
    catchup=False,
    tags=["bert", "dataproc", "bigquery", "migration"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
) as dag:
    start_task = DummyOperator(
        task_id="start_dw_bert_ausd_bp_ta_rn_vertrag",
    )

    # Task to submit the PySpark/Python job to Dataproc
    # The 'stichtag' parameter can be dynamically set using Airflow macros if needed,
    # e.g., '{{ ds_nodash }}' for execution date in YYYYMMDD.
    # For now, it's left to default inside the Python script or can be passed explicitly.
    run_dataproc_job = DataprocSubmitPySparkJobOperator(
        task_id="run_bert_aggregation_dataproc",
        main=DATAPROC_JOB_PATH,
        cluster_name=DATAPROC_CLUSTER_NAME,
        region=DATAPROC_REGION,
        project_id=GCP_PROJECT_ID,
        arguments=[
            f"--project_id={GCP_PROJECT_ID}",
            f"--dataset_id=isbert_schema",
            f"--sql_file={BIGQUERY_SQL_PATH}",
            # Pass stichtag as DDMMYYYY, e.g., using Airflow macro for yesterday:
            f"--stichtag={{ (data_interval_end - macros.timedelta(days=1)).strftime('%d%m%Y') }}"
            # Optionally pass wiederanlaufwert:
            # "--wiederanlaufwert=0"
        ],
    )

    # Placeholder for the legacy DW.BERT_LESE_LOG functionality
    # This task can be expanded to integrate with Cloud Logging or other monitoring tools.
    log_completion = DummyOperator(
        task_id="log_job_completion",
        # Consider adding a PythonOperator here to push metrics to Cloud Monitoring
        # or record job metadata in a BigQuery table.
    )

    end_task = DummyOperator(
        task_id="end_dw_bert_ausd_bp_ta_rn_vertrag",
    )

    start_task >> run_dataproc_job >> log_completion >> end_task