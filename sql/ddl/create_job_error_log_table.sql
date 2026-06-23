-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Description: DDL for job error log table, storing detailed error information.

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    error_id INT64 OPTIONS(description="Unique identifier for each error entry."),
    job_id STRING NOT NULL OPTIONS(description="Foreign key to `job_control.job_id`."),
    error_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred."),
    error_code STRING OPTIONS(description="Categorical error code."),
    error_message STRING OPTIONS(description="Detailed error message."),
    stack_trace STRING OPTIONS(description="Full stack trace or execution context at the time of error."),
    component STRING OPTIONS(description="Component where the error occurred (e.g., 'orchestration', 'kernel')."),
    additional_info JSON OPTIONS(description="Additional JSON formatted information about the error.")
)
OPTIONS(
    description="Table for storing detailed error information encountered during job execution."
);