-- DDL for job_error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    job_name STRING NOT NULL,
    job_entry_nr INT64,
    error_number INT64,
    error_argument STRING,
    error_message STRING,
    created_at TIMESTAMP
);