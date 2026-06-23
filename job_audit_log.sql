-- DDL for job_audit_log, part of migration for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
    event_ts TIMESTAMP,
    procedure_name STRING,
    job_kennung STRING,
    eintrags_nr STRING,
    tab_name STRING,
    record_count INT64,
    message STRING
);