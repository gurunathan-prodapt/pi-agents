-- DDL for BigQuery job control and logging table
-- This replaces implied job tracking and temporary file handling from the legacy ksh script.
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier.ksh
CREATE TABLE IF NOT EXISTS `my-project.data_warehouse.job_control_log` (
    job_name STRING NOT NULL,
    job_kennung STRING,
    entry_nr STRING,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING NOT NULL, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED', 'SKIPPED'
    message STRING,
    records_processed INT64
);