-- BigQuery DDL for job_error_log table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_c_bfc.ksh
-- This table replaces shell-based error logging mechanisms.

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_error_log` (
    job_kennung STRING NOT NULL,
    err_nr INT64,
    err_arg STRING,
    created_ts TIMESTAMP NOT NULL,
    message STRING,
    error_sqlstate STRING,
    error_message STRING,
    error_stack STRING,
    error_current_statement STRING,
    error_constraint STRING
)
OPTIONS(
    description="Error log for job executions, replacing shell script error logging."
);