-- DDL for error_log table for job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_iccid_einzeln.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.error_log` (
    timestamp TIMESTAMP,
    tab_name STRING,
    error_type STRING,
    error_code INT64,
    message STRING
);