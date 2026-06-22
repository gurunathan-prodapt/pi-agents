-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_msisdn.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    error_id STRING NOT NULL OPTIONS(description="Unique identifier for each error entry"),
    job_run_id STRING NOT NULL OPTIONS(description="Foreign key to job_control.job_run_id"),
    error_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred"),
    error_type STRING NOT NULL OPTIONS(description="Classification of the error (e.g., PARAMETER_VALIDATION, RUNTIME_ERROR)"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error description"),
    stack_trace STRING OPTIONS(description="Stack trace for runtime errors"),
    source_file STRING OPTIONS(description="Source file or procedure where the error originated")
);