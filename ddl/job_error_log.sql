-- Target BigQuery DDL for job_error_log table
-- Legacy Source: Error logging from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_error_log` (
    run_id STRING OPTIONS(description="Unique identifier for the job execution that encountered an error"),
    error_timestamp TIMESTAMP OPTIONS(description="Timestamp when the error occurred"),
    procedure_name STRING OPTIONS(description="Name of the stored procedure where the error occurred"),
    error_code STRING OPTIONS(description="BigQuery error code (e.g., 'BQ-3000')"),
    error_message STRING OPTIONS(description="Detailed error message"),
    error_stack_trace STRING OPTIONS(description="Stack trace or additional error details from BigQuery"),
    stichtag DATE OPTIONS(description="Cutoff date parameter at the time of error"),
    wiederanlauf_wert INT64 OPTIONS(description="Restart value parameter at the time of error")
);