#
# Airflow DAG for r_ausd_v_ta_p_discount_rr.ksh migration
#
# This DAG orchestrates the execution of the BigQuery stored procedure
# that replaces the original KornShell wrapper script.
#
from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

# Define BigQuery project and dataset from the design
GCP_PROJECT_ID = "my_gcp_project"
BQ_DATASET_ID = "my_bq_dataset"
BQ_PROCEDURE_NAME = "vertragsdatenabgleich" # Name of the wrapper stored procedure

with DAG(
    dag_id="r_ausd_v_ta_p_discount_rr_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None, # Define your schedule here, e.g., "@daily"
    tags=["isbert", "reconciliation", "bigquery"],
    description="Airflow DAG for the r_ausd_v_ta_p_discount_rr job, orchestrating BigQuery stored procedures.",
) as dag:
    # Example of how to pass parameters.
    # For dynamic dates, you would use Airflow macros, e.g., '{{ ds_nodash }}'
    # or '{{ ds }}' depending on the required format.
    # The original script used `date +%d%m%Y`. This example uses YYYYMMDD.
    # Adjust `p_stichtag` format as needed for your BigQuery SP.
    # p_stichtag = "{{ ds_nodash }}" # Example: 20230101
    p_stichtag_val = "20231026" # Hardcoded for example, replace with Airflow macro for dynamic dates
    p_log_level_val = "INFO"

    # Task to execute the BigQuery stored procedure
    execute_reconciliation_sp = BigQueryExecuteStoredProcedureOperator(
        task_id="execute_vertragsdatenabgleich_sp",
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_ID,
        procedure_id=BQ_PROCEDURE_NAME,
        gcp_conn_id="google_cloud_default", # Ensure this connection ID is configured in Airflow
        parameters=[
            {"name": "p_stichtag", "parameterType": {"type": "STRING"}, "value": p_stichtag_val},
            {"name": "p_log_level", "parameterType": {"type": "STRING"}, "value": p_log_level_val},
            {"name": "p_status", "parameterType": {"type": "STRING"}, "value": None, "mode": "OUT"}
        ],
        # The procedure returns an OUT parameter 'p_status'.
        # We can retrieve it if needed by subsequent tasks using XComs.
        # However, BigQueryExecuteStoredProcedureOperator itself succeeds/fails
        # based on the BigQuery job status.
    )

    # You can add more tasks here, e.g.,:
    # - A task to check the output status from XComs (if needed for custom logic)
    # - Data quality checks after the reconciliation
    # - Notifications (e.g., Slack, Email) on success/failure

    # Define task dependencies
    execute_reconciliation_sp