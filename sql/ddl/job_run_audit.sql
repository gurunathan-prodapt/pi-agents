-- DDL for project.dataset.job_run_audit
-- Audit table for legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_run_audit` (
    `job_id` STRING NOT NULL,
    `entry_number` STRING,
    `key_date` DATE,
    `run_timestamp` TIMESTAMP NOT NULL,
    `status` STRING NOT NULL,
    `processed_records` INT64,
    `start_time` TIMESTAMP,
    `end_time` TIMESTAMP
);