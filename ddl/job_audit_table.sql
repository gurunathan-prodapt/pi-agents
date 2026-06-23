-- BigQuery DDL for job_audit_table
-- Used for logging execution details of migrated jobs.
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_table`
(
    job_name STRING NOT NULL OPTIONS(description="Name of the job executed"),
    run_id STRING OPTIONS(description="Unique identifier for the job run"),
    start_timestamp TIMESTAMP OPTIONS(description="Start time of the job execution"),
    end_timestamp TIMESTAMP OPTIONS(description="End time of the job execution"),
    status STRING OPTIONS(description="Status of the job (SUCCESS, FAILED)"),
    error_message STRING OPTIONS(description="Error message if the job failed"),
    input_params JSON OPTIONS(description="Input parameters passed to the job"),
    output_records INT64 OPTIONS(description="Number of records processed/inserted"),
    duration_ms INT64 OPTIONS(description="Execution duration in milliseconds")
)
OPTIONS(
    description="Audit log for BigQuery job executions."
);