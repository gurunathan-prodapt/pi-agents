-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_valid.ksh
--
-- DDL for job_log to store detailed log messages and errors.
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_log` (
    log_time TIMESTAMP NOT NULL,
    job_id STRING NOT NULL,
    entry_number STRING NOT NULL,
    message STRING NOT NULL,
    severity STRING NOT NULL -- e.g., 'INFO', 'WARNING', 'ERROR'
);