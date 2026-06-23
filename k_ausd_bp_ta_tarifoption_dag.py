-- Legacy Source: vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- Job: vobs/dw_source/isrpt/isbert/install_save/k_ausd_bp_ta_tarifoption.ksh
-- Description: Airflow DAG to orchestrate the execution of the BigQuery Stored Procedure.

from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator

with DAG(
    dag_id="k_ausd_bp_ta_tarifoption_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    schedule=None, # Define your schedule here, e.g., "@daily" or specific cron
    tags=["bigquery", "etl"],
    doc_md="""
    ### k_ausd_bp_ta_tarifoption_dag
    This DAG migrates the functionality of the `k_ausd_bp_ta_tarifoption.ksh` KornShell script.
    It orchestrates the execution of a BigQuery Stored Procedure that handles parameter validation,
    date derivation, core data processing for tariff options, and audit logging.

    **Note:** Replace `your_project` and `your_dataset` with your actual GCP Project ID and BigQuery Dataset ID.
    """,
) as dag:
    execute_bp_tarifoption_sp = BigQueryExecuteStoredProcedureOperator(
        task_id="execute_bp_tarifoption_sp",
        project_id="your_project", # Replace with your GCP Project ID
        dataset_id="your_dataset", # Replace with your BigQuery Dataset ID
        procedure_id="k_ausd_bp_ta_tarifoption",
        parameters=[
            # Example values. For production, consider pulling these from Airflow variables,
            # XComs, or dynamically based on the DAG run context.
            {"name": "job_kennung", "parameter_type": {"type": "STRING"}, "value": "BERT_TA_TARIFOPTION"},
            {"name": "entry_nr", "parameter_type": {"type": "STRING"}, "value": "1"}, # Example entry number
            {"name": "as_of_date_str", "parameter_type": {"type": "STRING"}, "value": "{{ ds_nodash }}"}, # Airflow macro for 'YYYYMMDD' of current logical date
            {"name": "restart_val", "parameter_type": {"type": "INT64"}, "value": 0},
        ],
        gcp_conn_id="google_cloud_default", # Ensure this Airflow connection is configured for BigQuery access
    )