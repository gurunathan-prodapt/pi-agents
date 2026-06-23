-- BigQuery DDL for control_tables.job_error_log
-- Purpose: To log errors and messages for the migrated job.
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_error_log` (
    run_id STRING,
    job_kennung STRING,
    error_time TIMESTAMP,
    error_message STRING,
    severity STRING,
    step STRING
);