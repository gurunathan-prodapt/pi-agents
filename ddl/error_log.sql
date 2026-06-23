-- DDL for error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.error_log` (
    job_id STRING NOT NULL,
    run_id STRING NOT NULL,
    log_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    message STRING,
    error_code STRING,
    severity STRING
);