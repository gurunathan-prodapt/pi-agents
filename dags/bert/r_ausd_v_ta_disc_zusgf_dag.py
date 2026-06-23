# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_disc_zusgf.ksh
#
# Apache Airflow DAG for orchestrating the migration of the r_ausd_v_ta_disc_zusgf.ksh job.
# This DAG will:
# 1. Truncate the target BigQuery table.
# 2. Execute a PySpark job on Dataproc Serverless to process and concatenate discount data.
# 3. Insert the transformed data into the final BigQuery table.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitSparkJobOperator
# For Dataproc Serverless, consider using `DataprocCreateBatchOperator` or `DataprocServerlessSparkBatchOperator`
# if available in your Airflow environment for more explicit serverless integration.
# This example uses DataprocSubmitSparkJobOperator which can be configured for serverless batches.

# Define environment-specific variables
# These should ideally be set as Airflow Variables or passed via environment configurations
# For demonstration, they are hardcoded.
GCP_PROJECT_ID = "your-gcp-project-id"  # Replace with your GCP Project ID
GCP_REGION = "your-gcp-region"          # e.g., "us-central1"
BIGQUERY_DATASET_SOURCE = "your_source_dataset" # e.g., "isbert_schema_migrated"
BIGQUERY_DATASET_TARGET = "your_target_dataset" # e.g., "isbert_schema_migrated"
BIGQUERY_DATASET_STAGING = "your_staging_dataset" # e.g., "isbert_staging"
GCS_TEMP_BUCKET = "gs://your-gcs-staging-bucket" # GCS bucket for Dataproc temp files and PySpark script upload

# GCS path for the PySpark script. Assumes the script is uploaded to GCS.
# In a CI/CD pipeline, this script (src/pyspark/concat_discounts_spark.py) would be uploaded here.
PYSPARK_GCS_PATH = f"{GCS_TEMP_BUCKET}/dags_resources/pyspark/concat_discounts_spark.py"

with DAG(
    dag_id="bert_ausd_v_ta_disc_zusgf_etl",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Replace with your desired schedule (e.g., "@daily", "0 0 * * *")
    catchup=False,
    tags=["bert", "etl", "bigquery", "dataproc"],
    params={
        "project_id": GCP_PROJECT_ID,
        "region": GCP_REGION,
        "bigquery_dataset_source": BIGQUERY_DATASET_SOURCE,
        "bigquery_dataset_target": BIGQUERY_DATASET_TARGET,
        "bigquery_dataset_staging": BIGQUERY_DATASET_STAGING,
        "gcs_temp_bucket": GCS_TEMP_BUCKET,
    },
) as dag:
    start_task = DummyOperator(
        task_id="start",
    )

    # Task to truncate the target table before insertion
    truncate_target_table = BigQueryOperator(
        task_id="truncate_sof_ta_disc_zusgf",
        sql=f"TRUNCATE TABLE `{GCP_PROJECT_ID}.{BIGQUERY_DATASET_TARGET}.SOF_TA_DISC_ZUSGF`;",
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default", # Ensure this connection ID is configured in Airflow
    )

    # Task to run the PySpark job on Dataproc Serverless
    # The 'PYSPARK_GCS_PATH' should point to where concat_discounts_spark.py is uploaded in GCS.
    run_pyspark_job = DataprocSubmitSparkJobOperator(
        task_id="run_pyspark_concat_discounts",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        main_python_file=PYSPARK_GCS_PATH,
        # Arguments to be passed to the PySpark script
        arguments=[
            GCP_PROJECT_ID,
            BIGQUERY_DATASET_SOURCE,
            "SOF_TA_DISCOUNT", # Source BigQuery table name
            BIGQUERY_DATASET_STAGING,
            "ta_disc_zusgf_spark_staging", # Staging BigQuery table name for PySpark output
        ],
        # Example configuration for a Dataproc Serverless batch job
        # Actual configuration might vary based on your Dataproc Serverless setup
        # Refer to Dataproc documentation for specific batch configurations.
        # This setup assumes a default serverless batch environment can be targeted.
        # For explicit serverless setup, 'DataprocCreateBatchOperator' is recommended.
        # The following 'job_config' attempts to mimic a serverless profile.
        job_name="{{ task_instance.task_id }}-{{ ds_nodash }}",
        gcp_conn_id="google_cloud_default",
        # For Dataproc Serverless batches, instead of cluster_name/master_machine_type,
        # you might specify a 'batch_config' or use DataprocCreateBatchOperator.
        # The 'DataprocSubmitSparkJobOperator' uses 'job_config'.
        # For serverless, consider this structure:
        # spark_batch={'main_python_file_uri': PYSPARK_GCS_PATH, 'jar_file_uris': [...], ...}
        # A direct DataprocCreateBatchOperator might be more appropriate.
        # For this example, we'll use a simplified config, assuming the operator adapts.
        # If using DataprocCreateBatchOperator:
        # batch_id="spark-concat-{{ ds_nodash }}",
        # batch_config={
        #     "pyspark_batch": {
        #         "main_python_file_uri": PYSPARK_GCS_PATH,
        #         "args": [
        #             GCP_PROJECT_ID,
        #             BIGQUERY_DATASET_SOURCE, "SOF_TA_DISCOUNT",
        #             BIGQUERY_DATASET_STAGING, "ta_disc_zusgf_spark_staging",
        #         ],
        #         "jar_file_uris": [
        #             "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.28.0.jar"
        #         ]
        #     },
        #     "environment_config": {
        #         "execution_config": {"service_account": "your-dataproc-service-account@your-gcp-project-id.iam.gserviceaccount.com"},
        #     },
        # },
    )


    # Task to insert the final data into SOF_TA_DISC_ZUSGF from staging and source tables
    insert_into_target_table = BigQueryOperator(
        task_id="insert_into_sof_ta_disc_zusgf",
        sql="src/sql/ta_disc_zusgf_insert.sql", # Airflow can read SQL from a file path
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
        params={
            "project_id": GCP_PROJECT_ID,
            "bigquery_dataset_source": BIGQUERY_DATASET_SOURCE,
            "bigquery_dataset_target": BIGQUERY_DATASET_TARGET,
            "bigquery_dataset_staging": BIGQUERY_DATASET_STAGING,
        },
    )

    end_task = DummyOperator(
        task_id="end",
    )

    # Define task dependencies
    start_task >> truncate_target_table >> run_pyspark_job >> insert_into_target_table >> end_task