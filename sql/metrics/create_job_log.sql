-- DDL for BigQuery job logging table metrics.job_log
-- Created by k_ausd_adressen.ksh (d_ausd_adressen.sql) migration
CREATE TABLE IF NOT EXISTS `PROJECT_ID.metrics.job_log`
(
    job_id                      STRING,
    entry_number                INT64,
    key_date                    DATE,
    restart_value               STRING,
    start_timestamp             TIMESTAMP,
    end_timestamp               TIMESTAMP,
    status                      STRING, -- e.g., 'SUCCESS', 'FAILED'
    error_message               STRING,
    record_count                INT64,
    target_table                STRING
);