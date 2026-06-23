# Target: Python DAG for Cloud Composer
# Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh
# Description: Cloud Composer DAG to orchestrate the BigQuery Stored Procedure.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "your-gcp-project-id"  # Replace with your GCP project ID
DATASET_ID = "your_bigquery_dataset_id"  # Replace with your BigQuery dataset ID
BIGQUERY_LOCATION = "your-bigquery-location" # e.g., 'US', 'EU'

with DAG(
    dag_id="k_ausd_bp_ta_bpr_basis_his_orchestration",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,  # Define your schedule here, e.g., "@daily"
    catchup=False,
    tags=["bigquery", "etl", "isbert"],
    description="Orchestrates the k_ausd_bp_ta_bpr_basis_his BigQuery Stored Procedure.",
) as dag:
    # Example of how to pass dynamic parameters to the BigQuery Stored Procedure.
    # In a real scenario, 'Stichtag' might be derived from ds_nodash or another templated variable.
    # For demonstration, we use a fixed value or a simple date format for now.
    # The Stichtag needs to be in DDMMYYYY format.

    # Example: Dynamic Stichtag from Airflow's execution date, formatted as DDMMYYYY
    # ds_nodash provides YYYYMMDD, so we need to reformat.
    # For a simple daily run:
    # Stichtag = "{{ ds_nodash[6:8] }}{{ ds_nodash[4:6] }}{{ ds_nodash[0:4] }}"
    # Or for yesterday:
    # Stichtag = "{{ yesterday_ds_nodash[6:8] }}{{ yesterday_ds_nodash[4:6] }}{{ yesterday_ds_nodash[0:4] }}"

    # For this example, let's use a hardcoded value or a simple template if no specific dynamic is provided in design.
    # Assuming 'Stichtag' maps to 'execution_date' in DDMMYYYY format for daily run.
    # Let's use today's date minus 1 day to represent "yesterday" which is common for daily processing.
    # For `yesterday_ds_nodash` is YYYYMMDD.
    stichtag_formatted = "{{ execution_date.strftime('%d%m%Y') }}"


    call_main_bigquery_sp = BigQueryInsertJobOperator(
        task_id="call_k_ausd_bp_ta_bpr_basis_his_sp",
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": f"""
                    CALL `{PROJECT_ID}.{DATASET_ID}.proc_k_ausd_bp_ta_bpr_basis_his`(
                        p_JobKennung => 'BPR_BASIS_HIS',
                        p_EintragsNr => '12345', -- Example fixed value, or could be templated
                        p_Stichtag => '{stichtag_formatted}',
                        p_wiederanlaufWert => NULL -- Pass NULL if optional and not always provided
                    );
                """,
                "useLegacySql": False,
                "queryParameters": [
                    # If you want to use true query parameters rather than string formatting
                    # {
                    #     "name": "p_Stichtag_param",
                    #     "parameterType": {"type": "STRING"},
                    #     "parameterValue": {"value": stichtag_formatted},
                    # },
                ],
            }
        },
        location=BIGQUERY_LOCATION,
    )

    # If the optional post-processing procedure is activated, you would add another task here.
    # For example:
    # call_postprocessing_sp = BigQueryInsertJobOperator(
    #     task_id="call_k_ausd_bp_ta_bpr_basis_his_postprocess_sp",
    #     project_id=PROJECT_ID,
    #     configuration={
    #         "query": {
    #             "query": f"""
    #                 CALL `{PROJECT_ID}.{DATASET_ID}.proc_k_ausd_bp_ta_bpr_basis_his_postprocess`(
    #                     p_processing_date => PARSE_DATE('%d%m%Y', '{stichtag_formatted}'),
    #                     p_output_gcs_path => 'gs://your-gcs-bucket/path/to/exports/'
    #                 );
    #             """,
    #             "useLegacySql": False,
    #         }
    #     },
    #     location=BIGQUERY_LOCATION,
    # )

    # call_main_bigquery_sp >> call_postprocessing_sp # Define dependencies if post-processing is active
---