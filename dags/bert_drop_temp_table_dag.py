from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

# Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh
# Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_drop_temp_table.ksh
# Description: Airflow DAG to orchestrate the BigQuery k_drop_temp_table_wrapper stored procedure.
# This DAG replaces the legacy UC4 job for `r_drop_temp_table.ksh`.

# IMPORTANT: Replace these with your actual GCP Project ID and BigQuery Dataset ID
GCP_PROJECT_ID = "your-gcp-project-id"
BIGQUERY_DATASET = "your_bigquery_dataset_id"

with DAG(
    dag_id="bert_drop_temp_table_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set a schedule (e.g., "@daily", "0 3 * * *") or leave None for manual/external triggers
    catchup=False,
    tags=["bert", "bigquery", "cleanup"],
    doc_md="""
    ### BERT Drop Temporary Table DAG
    This DAG orchestrates the BigQuery stored procedure responsible for dropping temporary tables.
    It replaces the legacy KornShell script `r_drop_temp_table.ksh` and its UC4 scheduler.

    **Parameters:**
    - `p_stichtag_in`: Optional. Reference date in 'DDMMYYYY' format. If not provided, defaults to current date.
    - `p_wiederanlauf_wert_in`: Optional. Restart threshold as an integer. If not provided, defaults to 0.
    """,
) as dag:
    call_drop_temp_table_procedure = BigQueryExecuteStoredProcedureOperator(
        task_id="call_k_drop_temp_table_wrapper",
        project_id=GCP_PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        procedure_id="k_drop_temp_table_wrapper",
        gcp_conn_id="google_cloud_default",  # Ensure this Airflow connection is configured
        # Pass parameters to the BigQuery stored procedure.
        # These can be dynamic using Airflow's Jinja templating.
        # If arguments are not provided, the stored procedure's internal defaulting logic will apply.
        arguments={
            # Example: To pass the Airflow execution date (YYYY-MM-DD) formatted as DDMMYYYY
            # and a specific restart value:
            # "p_stichtag_in": "{{ ds_nodash[6:] + ds_nodash[4:6] + ds_nodash[0:4] }}", # Convert YYYYMMDD to DDMMYYYY
            # "p_wiederanlauf_wert_in": 12345

            # Example: To let the BigQuery stored procedure use its default for stichtag (current date)
            # and set a restart value:
            # "p_stichtag_in": None,
            # "p_wiederanlauf_wert_in": 12345

            # Example: To let the BigQuery stored procedure use all its default values (current date, restart_value = 0)
            # Just omit the arguments or pass an empty dictionary.
        }
    )

    # Further tasks can be added here, e.g.,
    # - Data quality checks post-cleanup
    # - Notifications (e.g., Slack, Email) for success or failure
    # - Downstream trigger of other DAGs