-- BigQuery DDL for the job logging table
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_cntrct_dist.ksh

CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_log` (
    `job_id` STRING,
    `entry_number` STRING,
    `reference_date` DATE,
    `start_time` TIMESTAMP,
    `end_time` TIMESTAMP,
    `status` STRING, -- e.g., 'SUCCESS', 'FAILED', 'RUNNING'
    `record_count` INT64,
    `restart_value` STRING,
    `comment` STRING,
    `processing_timestamp` TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);