-- BigQuery DDL for project.job_control.error_log
-- Purpose: For logging errors.
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh

CREATE TABLE IF NOT EXISTS `project.job_control.error_log`
(
    job_kennung      STRING NOT NULL,
    eintrags_nr      STRING NOT NULL,
    error_time       TIMESTAMP NOT NULL,
    error_message    STRING NOT NULL
)
OPTIONS(
    description="Table for logging errors."
);