-- BigQuery DDL for the error logging table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.error_log` (
    `job_id` STRING,
    `entry_number` STRING,
    `reference_date` DATE,
    `error_timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    `error_message` STRING,
    `component` STRING,
    `severity` STRING
);