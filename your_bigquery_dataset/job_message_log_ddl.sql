-- BigQuery DDL for job_message_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.job_message_log` (
    log_timestamp TIMESTAMP NOT NULL, -- Timestamp of the message
    job_id STRING NOT NULL,           -- Job ID from job_control
    message_type STRING,              -- Type of message (e.g., 'INFO', 'WARNING', 'START', 'END')
    message STRING,                   -- The actual message content
    script_name STRING                -- Name of the script/procedure generating the message
);