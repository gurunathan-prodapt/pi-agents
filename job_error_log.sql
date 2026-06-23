-- DDL for job_error_log, part of migration for vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_cntrct_crs.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    event_ts TIMESTAMP,
    procedure_name STRING,
    err_nr INT64,
    err_arg STRING,
    message STRING
);