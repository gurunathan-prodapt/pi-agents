-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh
-- Description: DDL for the job_audit_log table in BigQuery.
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
    job_kennung STRING NOT NULL,
    job_nr INT64 NOT NULL,
    log_ts TIMESTAMP NOT NULL,
    status STRING NOT NULL,
    stichtag DATE,
    sysdate DATE,
    log_file STRING,
    err_nr INT64,
    err_arg STRING,
    message STRING
);