-- DDL for job_log table for job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    tab_name STRING,
    job_status STRING,
    job_type STRING,
    stichtag DATE,
    run_date DATE,
    record_count INT64,
    message STRING,
    created_at TIMESTAMP
);