-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_beschr.ksh
-- Target BigQuery DDL for job execution audit table job_audit.

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
    job_kennung STRING,
    stichtag STRING,
    restart_value INT64,
    status STRING,
    message STRING,
    created_at TIMESTAMP,
    finished_at TIMESTAMP
);