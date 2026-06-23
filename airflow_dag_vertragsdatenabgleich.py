-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_period.ksh

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "my_gcp_project"  # Replace with your GCP project ID
DATASET_ID = "my_dataset"      # Replace with your BigQuery dataset ID
BIGQUERY_CONNECTION_ID = "google_cloud_default" # Or your specific BigQuery connection ID

with DAG(
    dag_id="vertragsdatenabgleich_wrapper_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Set your desired schedule, e.g., "@daily"
    catchup=False,
    tags=["bigquery", "data_sync"],
    description="Airflow DAG to orchestrate the BigQuery vertragsdatenabgleich_wrapper stored procedure.",
) as dag:
    call_wrapper_sp = BigQueryInsertJobOperator(
        task_id="call_vertragsdatenabgleich_wrapper_sp",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CALL `{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`(
                        p_stichtag => FORMAT_DATE('%d%m%Y', CURRENT_DATE()), -- Pass today's date in DDMMYYYY format
                        p_log_level => 'INFO',
                        p_show_help => FALSE
                    );
                """,
                "useLegacySql": False,
                "queryParameters": [], # Add parameters if needed by the SP
            }
        },
        gcp_conn_id=BIGQUERY_CONNECTION_ID,
    )

    # You can add more tasks here, e.g., checking status, sending notifications, etc.
    # For example, if you wanted to pass a specific date from a DAG parameter:
    # call_wrapper_sp_with_param = BigQueryInsertJobOperator(
    #     task_id="call_vertragsdatenabgleich_wrapper_sp_with_param",
    #     project_id=PROJECT_ID,
    #     configuration={
    #         "query": {
    #             "query": f"""
    #                 CALL `{PROJECT_ID}.{DATASET_ID}.vertragsdatenabgleich_wrapper`(
    #                     p_stichtag => '{{ ds_nodash }}', -- Airflow macro for YYYYMMDD, adjust format as needed for your SP
    #                     p_log_level => 'INFO',
    #                     p_show_help => FALSE
    #                 );
    #             """,
    #             "useLegacySql": False,
    #         }
    #     },
    #     gcp_conn_id=BIGQUERY_CONNECTION_ID,
    # )