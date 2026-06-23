-- DDL for BigQuery table job_error_log
-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_bp_ref.ksh

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_error_log` (
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    job_kennung STRING,
    entry_nr INT64,
    error_level STRING, -- e.g., 'ERROR', 'WARNING', 'INFO'
    error_message STRING
);