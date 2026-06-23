-- BigQuery DDL for job_error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.job_error_log` (
    log_timestamp TIMESTAMP NOT NULL, -- Timestamp of the error
    job_id STRING NOT NULL,           -- Job ID from job_control
    error_code INT64,                 -- Error number (ErrNr from script)
    error_argument STRING,            -- Argument related to the error (ErrArg from script)
    error_message STRING,             -- Descriptive error message
    script_name STRING                -- Name of the script/procedure where the error occurred
);