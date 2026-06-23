-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_msisdn.ksh
-- Description: DDL for the job_run_info table in BigQuery.
CREATE TABLE IF NOT EXISTS `project.dataset.job_run_info` (
    job_kennung STRING NOT NULL,
    job_nr INT64 NOT NULL,
    stichtag DATE NOT NULL,
    sysdate DATE NOT NULL,
    created_ts TIMESTAMP NOT NULL
);