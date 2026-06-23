# This Airflow DAG replaces the legacy KornShell script:
# vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh
#
# It orchestrates the execution of a BigQuery Stored Procedure for data processing.

from __future__ import annotations

import pendulum

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteStoredProcedureOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta

@dag(
    dag_id="k_ausd_bp_ta_bpr_basis_his",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["isbert", "bigquery", "etl"],
    params={
        "JobKennung": {
            "type": "string",
            "title": "Job Identifier",
            "description": "Identifier for the job.",
            "default": "DEFAULT_JOB",
        },
        "EintragsNr": {
            "type": "string",
            "title": "Entry Number",
            "description": "Entry number for the process.",
            "default": "0",
        },
        "Stichtag": {
            "type": "string",
            "title": "Reference Date (DDMMYYYY)",
            "description": "The reference date for data processing in DDMMYYYY format.",
            "default": datetime.now().strftime("%d%m%Y"),
        },
        "wiederanlaufWert": {
            "type": "string",
            "title": "Restart Value",
            "description": "Restart value, if applicable.",
            "default": "0",
        },
    },
)
def k_ausd_bp_ta_bpr_basis_his_dag():
    @task
    def validate_and_prepare_params(**kwargs) -> dict:
        """
        Validates input parameters and prepares them for BigQuery.
        Replaces legacy date utility functions.
        """
        job_kennung = kwargs["params"].get("JobKennung")
        eintrags_nr = kwargs["params"].get("EintragsNr")
        stichtag_str = kwargs["params"].get("Stichtag")
        wiederanlauf_wert = kwargs["params"].get("wiederanlaufWert")

        if not job_kennung:
            raise AirflowException("Parameter JobKennung is missing.")
        if not eintrags_nr:
            raise AirflowException("Parameter EintragsNr is missing.")
        if not stichtag_str:
            raise AirflowException("Parameter Stichtag is missing.")

        try:
            # Validate Stichtag format (DDMMYYYY)
            stichtag_date = datetime.strptime(stichtag_str, "%d%m%Y").date()
        except ValueError:
            raise AirflowException(f"Invalid Stichtag format: {stichtag_str}. Expected DDMMYYYY.")

        # Equivalent to 'gestern.ksh' and other date utilities
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        # Assuming the stored procedure expects DATE type for p_process_date
        return {
            "p_job_kennung": job_kennung,
            "p_eintrags_nr": eintrags_nr,
            "p_process_date": stichtag_date.isoformat(), # YYYY-MM-DD for BigQuery DATE type
            "p_wiederanlauf_wert": wiederanlauf_wert,
            "p_datum_heute": today.isoformat(),
            "p_datum_gestern": yesterday.isoformat(),
        }

    @task
    def log_job_start(**kwargs):
        """
        Placeholder for logging job start in a BigQuery control table.
        """
        task_instance = kwargs['ti']
        job_kennung = task_instance.xcom_pull(task_ids='validate_and_prepare_params')['p_job_kennung']
        process_date = task_instance.xcom_pull(task_ids='validate_and_prepare_params')['p_process_date']
        print(f"Logging job start for JobKennung: {job_kennung}, Process Date: {process_date}")
        # Example BQ insert (needs actual table schema)
        # BigQueryExecuteQueryOperator(
        #     task_id="insert_job_start_log",
        #     sql=f"""
        #         INSERT INTO `your-gcp-project.isbert_dataset.job_control` (job_name, start_time, status, process_date)
        #         VALUES ('k_ausd_bp_ta_bpr_basis_his', CURRENT_TIMESTAMP(), 'RUNNING', PARSE_DATE('%Y-%m-%d', '{process_date}'));
        #     """,
        #     use_legacy_sql=False,
        #     gcp_conn_id="google_cloud_default",
        # ).execute(context=kwargs)

    @task
    def log_job_end(status: str, rows_processed: int = 0, error_message: str = None, **kwargs):
        """
        Placeholder for logging job end in a BigQuery control table.
        """
        task_instance = kwargs['ti']
        job_kennung = task_instance.xcom_pull(task_ids='validate_and_prepare_params')['p_job_kennung']
        process_date = task_instance.xcom_pull(task_ids='validate_and_prepare_params')['p_process_date']
        print(f"Logging job end for JobKennung: {job_kennung}, Status: {status}, Rows: {rows_processed}, Error: {error_message}")
        # Example BQ insert (needs actual table schema)
        # BigQueryExecuteQueryOperator(
        #     task_id="insert_job_end_log",
        #     sql=f"""
        #         INSERT INTO `your-gcp-project.isbert_dataset.job_control` (job_name, end_time, status, process_date, rows_processed, error_message)
        #         VALUES (
        #             'k_ausd_bp_ta_bpr_basis_his',
        #             CURRENT_TIMESTAMP(),
        #             '{status}',
        #             PARSE_DATE('%Y-%m-%d', '{process_date}'),
        #             {rows_processed},
        #             '{error_message if error_message else "NULL"}'
        #         );
        #     """,
        #     use_legacy_sql=False,
        #     gcp_conn_id="google_cloud_default",
        # ).execute(context=kwargs)


    params = validate_and_prepare_params()
    log_start = log_job_start()

    # The core data processing stored procedure
    # The output of this task would be the result from the stored procedure
    # which includes 'status' and 'rows_inserted'
    process_data = BigQueryExecuteStoredProcedureOperator(
        task_id="process_basisprodukt_data",
        project_id="your-gcp-project", # Replace with your GCP Project ID
        dataset_id="sof", # As inferred from the generated SQL
        procedure_id="d_ausd_bp_ta_bpr_basis_his", # Name of the generated stored procedure
        gcp_conn_id="google_cloud_default",
        sql_data_args=[
            {"name": "p_process_date", "value": "{{ task_instance.xcom_pull('validate_and_prepare_params')['p_process_date'] }}"},
        ],
    )

    # Placeholder for post-processing task, if activated
    # This would typically run a separate BigQuery script or a Dataflow job
    # to handle the `sed`, `sort`, `join` operations on external tables.
    post_process_files = BigQueryExecuteStoredProcedureOperator(
        task_id="post_process_cibasis_files",
        project_id="your-gcp-project", # Replace with your GCP Project ID
        dataset_id="isbert_dataset", # A hypothetical dataset for staging/external tables
        procedure_id="post_process_cibasis_data", # A hypothetical procedure for post-processing
        gcp_conn_id="google_cloud_default",
        # Assuming the post-processing procedure might also take the process date
        sql_data_args=[
            {"name": "p_process_date", "value": "{{ task_instance.xcom_pull('validate_and_prepare_params')['p_process_date'] }}"},
        ],
        trigger_rule="all_success", # Only run if main processing is successful
        # This task should only run if the commented-out logic is needed.
        # This could be controlled by an Airflow variable or DAG param if needed.
        # For simplicity, we assume it's always run if this DAG is activated.
    )


    # Task to capture the result of the stored procedure and log
    @task
    def capture_and_log_results(**kwargs):
        ti = kwargs['ti']
        # The BigQueryExecuteStoredProcedureOperator does not directly return procedure output via XCom.
        # A common pattern is for the procedure to write results to a temporary table,
        # which can then be queried by a subsequent task.
        # For simplicity here, we assume the procedure directly returns a result set (as the generated BQ proc does).
        # We would need to execute a BigQuery query to fetch this result.
        # This is a conceptual example for fetching results from a BQ procedure call.
        # In a real scenario, you might have the BQ procedure insert status into a control table,
        # or have a separate BQ query task to retrieve the result.

        # For the generated BQ procedure, it SELECTS a status row.
        # To get this, we'd typically query a temporary table it writes to, or
        # use the bq command line tool in a BashOperator if `BigQueryExecuteStoredProcedureOperator`
        # doesn't expose the procedure's SELECT output.
        # Assuming the procedure's output (status, rows_inserted) can be retrieved.
        # For demonstration, we'll hardcode success. In reality, you'd query BigQuery for status.
        # The generated BQ stored procedure explicitly returns a status SELECT.
        # To capture this, you might need a BigQueryGetDataOperator or similar.
        # For now, let's assume success for the main process_data task to proceed.
        main_proc_status = "SUCCESS" # Placeholder, actual logic needed to read procedure output
        rows_processed = 0 # Placeholder

        # The BigQueryExecuteStoredProcedureOperator does not push XComs for returned results directly.
        # You'd typically make the stored procedure write to a log table and then query that table.
        # Or, if the procedure is simple and returns a single row, you can call it with BigQueryExecuteQueryOperator
        # and parse the result.

        # For this design, let's assume a simplified success/failure path
        # If `process_data` throws an exception, the DAG fails.
        # If it succeeds, we call log_job_end with SUCCESS.
        log_job_end_success = log_job_end(status="SUCCESS", rows_processed=rows_processed)
        log_job_end_success.set_upstream(process_data) # This task depends on successful completion

    # Define the task flow
    chain = validate_and_prepare_params() >> log_start >> process_data >> post_process_files >> capture_and_log_results()

k_ausd_bp_ta_bpr_basis_his_dag()