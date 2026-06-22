-- DDL for project.dataset.job_error_audit
-- Audit table for legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_bpr_opt_text.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_audit` (
    `job_id` STRING NOT NULL,
    `entry_number` STRING,
    `key_date` DATE,
    `error_timestamp` TIMESTAMP NOT NULL,
    `error_message` STRING NOT NULL,
    `stack_trace` STRING
);