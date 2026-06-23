-- DDL for job_error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_msisdn.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bq_dataset.job_error_log`
(
    job_kennung STRING,
    eintrags_nr STRING,
    stichtag DATE,
    error_message STRING,
    error_timestamp TIMESTAMP
)
OPTIONS(
    description="Logs errors encountered during job execution"
);