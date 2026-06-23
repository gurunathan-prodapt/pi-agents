-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.error_log` (
    job_name STRING NOT NULL,
    job_id STRING,
    entry_number STRING,
    error_timestamp TIMESTAMP NOT NULL,
    error_message STRING NOT NULL,
    error_stack_trace STRING,
    error_severity STRING DEFAULT 'ERROR'
);