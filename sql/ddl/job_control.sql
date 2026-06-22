-- DDL for job_control table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    job_entry_nr INT64 NOT NULL,
    job_name STRING NOT NULL,
    script_name STRING,
    log_reference STRING,
    stichtag STRING,
    status STRING,
    created_at TIMESTAMP,
    finished_at TIMESTAMP
);