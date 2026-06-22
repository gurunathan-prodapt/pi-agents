# Airflow DAG for k_ausd_v_ta_inv_assign.ksh
# Replaces: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_assign.ksh
# Orchestrates the execution of the BigQuery stored procedure.

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.utils.dates import days_ago
from datetime import datetime

with DAG(
    dag_id='k_ausd_v_ta_inv_assign_dag',
    start_date=days_ago(1),
    schedule_interval=None,  # This DAG is typically triggered manually or by an upstream process
    catchup=False,
    tags=['isbert', 'bigquery', 'extraction', 'automated'],
    doc_md="""
    ### BigQuery Data Extraction for ta_inv_assign
    This DAG replaces the legacy KornShell script `k_ausd_v_ta_inv_assign.ksh`.
    It orchestrates the execution of a BigQuery Stored Procedure,
    `your-gcp-project.isbert_target_data.sp_d_ausd_v_ta_inv_assign`,
    which extracts and transforms data related to `ta_inv_assign`.

    **Input Parameters (via `dag_run.conf`):**
    - `job_kennung`: (Optional) Corresponds to the original `-j` parameter. Default: "DEFAULT_JOB"
    - `eintrags_nr`: (Optional) Corresponds to the original `-f` parameter. Default: "DEFAULT_ENTRY"
    """,
) as dag:
    # Task to execute the BigQuery Stored Procedure
    # The Stored Procedure encapsulates the logic from d_ausd_v_ta_inv_assign.sql
    execute_ta_inv_assign_sp = BigQueryExecuteStoredProcedureOperator(
        task_id='execute_ta_inv_assign_sp',
        project_id='your-gcp-project',
        dataset_id='isbert_target_data',
        procedure_id='sp_d_ausd_v_ta_inv_assign',
        gcp_conn_id='google_cloud_default',  # Assumes 'google_cloud_default' connection is configured
        # Pass parameters to the stored procedure.
        # These are sourced from Airflow's DAG run configuration.
        parameters=[
            {
                'name': 'p_job_kennung',
                'parameterType': {'type': 'STRING'},
                'defaultValue': '{{ dag_run.conf.get("job_kennung", "DEFAULT_JOB") }}'
            },
            {
                'name': 'p_eintrags_nr',
                'parameterType': {'type': 'STRING'},
                'defaultValue': '{{ dag_run.conf.get("eintrags_nr", "DEFAULT_ENTRY") }}'
            },
        ],
    )