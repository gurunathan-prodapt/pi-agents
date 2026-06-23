-- Target: BigQuery DDL
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_basis_his.ksh
-- Description: DDL for the BigQuery error log table, replacing shell script's DWMSG_MeldeFehler mechanism.

CREATE TABLE IF NOT EXISTS `project.dataset.error_log` (
    job_id STRING NOT NULL OPTIONS(description="Identifier for the job that reported the error."),
    run_id STRING OPTIONS(description="Unique identifier for the specific execution run of the job."),
    timestamp TIMESTAMP NOT NULL OPTIONS(description="When the error occurred."),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message."),
    error_code STRING OPTIONS(description="Optional error code or type."),
    component STRING OPTIONS(description="Component or stage where the error occurred (e.g., 'Parameter Validation', 'SQL Execution')."),
    stack_trace STRING OPTIONS(description="Optional stack trace or additional debugging information.")
)
PARTITION BY DATE(timestamp)
CLUSTER BY job_id
OPTIONS(
    description="Table to log errors encountered during BigQuery job executions."
);