-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn_his.ksh
-- This DDL creates the job_error_log table for detailed error tracking of the migrated job.

CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    job_id STRING NOT NULL,
    job_name STRING,
    error_timestamp TIMESTAMP NOT NULL,
    error_message STRING NOT NULL,
    error_details JSON
);