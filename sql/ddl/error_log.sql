-- DDL for error_log, replacing error reporting in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_action_assoc.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.error_log` (
    error_ts TIMESTAMP NOT NULL,
    error_code INT64 NOT NULL,
    error_arg STRING,
    procedure_name STRING NOT NULL,
    message STRING
);