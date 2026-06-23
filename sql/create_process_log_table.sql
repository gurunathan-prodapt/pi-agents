-- DDL for process_log table for job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.process_log` (
    job_kennung STRING,
    eintrags_nr STRING,
    stichtag DATE,
    records INT64,
    created_at TIMESTAMP
);