-- BigQuery DDL for control_tables.job_table
-- Purpose: To track job execution, status, and record counts for the migrated job.
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_discount_rr.ksh

CREATE TABLE IF NOT EXISTS `your_project.your_dataset.job_table` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING,
    run_id STRING NOT NULL,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    status STRING, -- e.g., 'RUNNING', 'COMPLETED', 'FAILED'
    record_count INT64,
    message STRING
);