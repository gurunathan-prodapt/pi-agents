-- Header: DDL for BigQuery audit log table
-- Legacy Source: N/A (this is a new DDL for auditing)
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_apn_ve.ksh

-- Placeholder for BigQuery project and dataset
-- Replace `your_project_id.your_dataset_id` with your actual project and dataset.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_log` (
    job_run_id STRING NOT NULL OPTIONS(description="Unique identifier for each job run"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job/procedure being executed"),
    start_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job run started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job run ended"),
    status STRING OPTIONS(description="Current status of the job run (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    error_message STRING OPTIONS(description="Detailed error message if the job failed"),
    error_stack_trace STRING OPTIONS(description="Stack trace of the error if available"),
    process_date DATE OPTIONS(description="The primary processing date for the job"),
    version STRING OPTIONS(description="Version of the job/script"),
    log_correlation_id STRING OPTIONS(description="Identifier for correlating logs in Cloud Logging (e.g., job_name_job_run_id)"),
    additional_info JSON OPTIONS(description="Additional JSON data for logging specific job parameters or outcomes")
)
OPTIONS(
    description="Table to store audit logs and execution details for migrated BigQuery jobs."
);