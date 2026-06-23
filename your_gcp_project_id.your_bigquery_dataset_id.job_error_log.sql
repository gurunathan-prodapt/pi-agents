-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_p_discount_rr.ksh
-- Description: DDL for job_error_log table for BigQuery migration.
-- This table logs errors encountered during job execution.

CREATE TABLE IF NOT EXISTS `your_gcp_project_id.your_bigquery_dataset_id.job_error_log` (
    log_id STRING NOT NULL OPTIONS(description="Unique identifier for each error log entry."),
    job_run_id STRING OPTIONS(description="Reference to job_control.job_run_id for the job instance that failed."),
    job_name STRING OPTIONS(description="Name of the job or stored procedure that encountered the error."),
    job_kennung STRING OPTIONS(description="Business identifier for the job, analogous to p_JobKennung."),
    eintrags_nr INT64 OPTIONS(description="Entry number or identifier, analogous to p_EintragsNr."),
    error_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred."),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message."),
    error_stacktrace STRING OPTIONS(description="Optional stack trace or additional error context.")
)
PARTITION BY DATE(error_time)
CLUSTER BY job_name, job_kennung, eintrags_nr
OPTIONS(
    description="Table to log errors and exceptions during job execution, replacing legacy error messaging utilities."
);