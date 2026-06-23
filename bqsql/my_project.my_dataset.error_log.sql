-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.error_log` (
    job_kennung STRING,
    entry_nr INT64,
    error_timestamp TIMESTAMP,
    error_message STRING,
    error_code STRING,
    script_name STRING,
    stack_trace STRING
);