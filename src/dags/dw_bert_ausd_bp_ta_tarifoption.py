# Legacy Source: DW.BERT_AUSD_BP_TA_TARIFOPTION (UC4 XML Job)
# Job: DW.BERT_AUSD_BP_TA_TARIFOPTION

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.trigger_rule import TriggerRule

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='dw_bert_ausd_bp_ta_tarifoption',
    default_args=default_args,
    description='Migrated DAG for DW.BERT_AUSD_BP_TA_TARIFOPTION to prepare tariff options.',
    schedule_interval='0 0 * * *',  # Daily at midnight, adjust as needed
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['bert', 'dwh', 'etl', 'bigquery', 'dataproc'],
) as dag:
    # Dataproc cluster details - replace with your actual cluster config
    # It's recommended to use a managed Dataproc workflow template or ephemeral clusters
    # for production environments. For simplicity, a fixed cluster name is used here.
    GCP_PROJECT_ID = 'your-gcp-project-id' # TODO: Replace with your GCP Project ID
    GCP_REGION = 'your-gcp-region'         # TODO: Replace with your GCP Region (e.g., us-central1)
    DATAPROC_CLUSTER_NAME = 'your-dataproc-cluster' # TODO: Replace with your Dataproc Cluster Name
    GCS_BUCKET = 'gs://your-gcs-bucket'    # TODO: Replace with your GCS bucket for PySpark scripts

    # Path to the PySpark script in GCS
    PYSPARK_JOB_FILE = f'{GCS_BUCKET}/pyspark_jobs/r_ausd_bp_ta_tarifoption_main.py'
    # Path to the BigQuery SQL script in GCS (or accessible by PySpark)
    BQ_SQL_TEMPLATE_FILE = f'{GCS_BUCKET}/bigquery_sql/d_ausd_bp_ta_tarifoption.sql' # Assuming BQ SQL is also on GCS or accessible

    # Task to execute the PySpark job on Dataproc
    run_dw_bert_ausd_bp_ta_tarifoption = DataprocSubmitJobOperator(
        task_id='run_dw_bert_ausd_bp_ta_tarifoption',
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        job={
            'placement': {
                'cluster_name': DATAPROC_CLUSTER_NAME,
            },
            'pyspark_job': {
                'main_python_file_uri': PYSPARK_JOB_FILE,
                'args': [
                    '--stichtag', '{{ ds }}', # Airflow's ds (date string) macro
                    '--wiederanlaufwert', '{{ dag_run.run_id }}', # Example: use run_id for restart value
                    '--sql_template_path', BQ_SQL_TEMPLATE_FILE,
                ],
                'properties': {
                    'spark.logConf': 'true',
                },
            },
        },
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # Define task dependencies
    run_dw_bert_ausd_bp_ta_tarifoption