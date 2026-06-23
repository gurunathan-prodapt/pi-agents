-- BigQuery schema definition for job_table
-- Replaces implicit job management in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_inv_acc.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_table` (
    job_kennung STRING NOT NULL,
    eintrags_nr STRING NOT NULL,
    tab_name STRING NOT NULL,
    active_flag BOOL,
    created_ts TIMESTAMP,
    completed_ts TIMESTAMP,
    record_count INT64,
    error_code INT64,
    error_message STRING
);