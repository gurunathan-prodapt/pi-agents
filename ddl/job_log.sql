-- DDL for job_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_p_basisprod.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    job_id STRING NOT NULL,
    run_id STRING NOT NULL,
    log_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    message STRING,
    record_count INT64,
    status STRING
);