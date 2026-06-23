-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
--
-- DDL for job_table to track job execution status.
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_table` (
    job_id STRING NOT NULL,
    entry_number STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING NOT NULL, -- e.g., 'RUNNING', 'COMPLETED', 'FAILED', 'IGNORED'
    record_count INT64,
    error_message STRING,
    is_active BOOL NOT NULL DEFAULT TRUE,
    last_updated TIMESTAMP NOT NULL
);