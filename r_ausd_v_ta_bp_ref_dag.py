# Migrated from legacy KornShell job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_bp_ref.ksh
# This Airflow DAG orchestrates the contract data reconciliation process for the 'ta_bp_ref' table.

import pendulum
import logging

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryOperator

# Configure logging for the DAG
log = logging.getLogger(__name__)

# --- Utility Functions (Replacing legacy KornShell environment setup and logging) ---
# These Python functions encapsulate the logic found in the original .ksh script
# for environment initialization, parameter generation, and final status logging.
# In a larger migration, more complex utility scripts might be refactored into
# separate Python modules, but for this wrapper script's functionality,
# embedding them directly or relying on Airflow's native features is sufficient.

def _start_job_and_generate_params(**context):
    """
    Mimics the initial setup, parameter parsing, and dynamic parameter generation
    from the original r_ausd_v_ta_bp_ref.ksh script.
    It generates JobKennung and DW_EintragsNr, pushing them to XCom for downstream tasks.
    """
    # Corresponds to JobKennung="${BASENAME%.ksh}"
    job_kennung = "r_ausd_v_ta_bp_ref"
    
    # Corresponds to DW_EintragsNr="${JobKennung}_${DATUM_JJJJMMDD}_${UHRZEIT_HHMMSS}"
    # Airflow's data_interval_start provides the logical execution date/time.
    execution_datetime = context["data_interval_start"]
    dw_eintrags_nr = f"{job_kennung}_{execution_datetime.strftime('%Y%m%d_%H%M%S')}"

    log.info(f"Job initialization for: {job_kennung}")
    log.info(f"Generated DW_EintragsNr: {dw_eintrags_nr}")

    # Push these generated parameters to XCom for use by subsequent tasks
    context["ti"].xcom_push(key="job_kennung", value=job_kennung)
    context["ti"].xcom_push(key="dw_eintrags_nr", value=dw_eintrags_nr)

def _log_success_message(**context):
    """
    Mimics the final success message and status update from the original .ksh script
    (e.g., DWMSG_SetzeStatusOK).
    """
    job_kennung = context["ti"].xcom_pull(task_ids="start_job_and_generate_params", key="job_kennung")
    log.info(f"Job '{job_kennung}' completed successfully.")

# --- Airflow DAG Definition ---
with DAG(
    dag_id="r_ausd_v_ta_bp_ref_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    # The original script was an on-demand wrapper. Set to None for manual trigger,
    # or specify a schedule (e.g., "@daily", "0 0 * * *") if it was implicitly scheduled.
    schedule_interval=None,
    catchup=False, # Do not run for past missed schedules
    tags=["isbert", "contract_reconciliation", "bigquery"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False, # Configure for actual alerts in production
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    description="Migrated Airflow DAG for r_ausd_v_ta_bp_ref.ksh. Orchestrates contract "
                "data reconciliation, invoking the BigQuery-migrated logic of k_ausd_v_ta_bp_ref.ksh.",
) as dag:
    # Task 1: Initialize job and generate dynamic parameters
    start_job_task = PythonOperator(
        task_id="start_job_and_generate_params",
        python_callable=_start_job_and_generate_params,
    )

    # Task 2: Execute the core data reconciliation logic in BigQuery.
    # This task replaces the invocation of 'k_ausd_v_ta_bp_ref.ksh'.
    # The SQL provided here is a placeholder. The actual BigQuery SQL or
    # stored procedure call will be derived from the separate migration of
    # 'k_ausd_v_ta_bp_ref.ksh', which operates on the 'ta_bp_ref' table.
    execute_core_logic_task = BigQueryOperator(
        task_id="execute_bigquery_reconciliation_logic",
        sql="""
            -- Placeholder for the migrated logic from k_ausd_v_ta_bp_ref.ksh.
            -- This SQL would perform the actual contract data reconciliation for 'ta_bp_ref'.
            -- It would typically involve SELECT, INSERT, UPDATE, or MERGE statements
            -- against BigQuery tables, or call a BigQuery Stored Procedure.

            -- Parameters passed from the wrapper (extracted via XComs):
            -- job_kennung: {{ ti.xcom_pull(task_ids='start_job_and_generate_params', key='job_kennung') }}
            -- dw_eintrags_nr: {{ ti.xcom_pull(task_ids='start_job_and_generate_params', key='dw_eintrags_nr') }}

            -- Example: Log reconciliation event or perform a dummy operation
            SELECT
                'Contract data reconciliation process initiated for ta_bp_ref.' AS status_message,
                '{{ ti.xcom_pull(task_ids="start_job_and_generate_params", key="job_kennung") }}' AS job_identifier,
                '{{ ti.xcom_pull(task_ids="start_job_and_generate_params", key="dw_eintrags_nr") }}' AS reconciliation_entry_number,
                CURRENT_TIMESTAMP() AS reconciliation_timestamp;

            -- Uncomment and replace with the actual BigQuery SQL for 'ta_bp_ref' reconciliation.
            -- For example, if k_ausd_v_ta_bp_ref.ksh logic translates to a stored procedure:
            -- CALL `your-gcp-project.your_dataset.sp_reconcile_ta_bp_ref`(
            --     '{{ ti.xcom_pull(task_ids="start_job_and_generate_params", key="job_kennung") }}',
            --     '{{ ti.xcom_pull(task_ids="start_job_and_generate_params", key="dw_eintrags_nr") }}'
            -- );

            -- Or if it's direct SQL to update 'ta_bp_ref' or a related table:
            -- MERGE `your-gcp-project.your_dataset.ta_bp_ref` AS T
            -- USING (
            --     -- Your complex reconciliation logic here, referencing source tables
            --     SELECT ... FROM `source_project.source_dataset.source_table` WHERE ...
            -- ) AS S
            -- ON T.contract_id = S.contract_id
            -- WHEN MATCHED THEN
            --     UPDATE SET T.status = 'RECONCILED', T.last_update_ts = CURRENT_TIMESTAMP()
            -- WHEN NOT MATCHED THEN
            --     INSERT (contract_id, status, last_update_ts) VALUES (S.contract_id, 'RECONCILED', CURRENT_TIMESTAMP());
        """,
        use_legacy_sql=False, # Use Standard SQL
        gcp_conn_id="google_cloud_default", # Ensure this connection is configured in Airflow
        # Optional: Specify project and dataset if not inferred from connection or environment
        # project_id="your-gcp-project-id",
        # location="your-bigquery-location", # e.g., "US", "EU"
        # destination_dataset_table="your_dataset.reconciliation_log_table", # If the SELECT statement outputs data to a table
        # write_disposition="WRITE_APPEND", # To append results if destination_dataset_table is set
    )

    # Task 3: Log the final success message
    end_job_task = PythonOperator(
        task_id="log_job_success",
        python_callable=_log_success_message,
    )

    # Define task dependencies
    start_job_task >> execute_core_logic_task >> end_job_task