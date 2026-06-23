-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_log` (
    job_kennung STRING,
    entry_nr INT64,
    log_timestamp TIMESTAMP,
    log_message STRING,
    log_level STRING,
    process_id STRING,
    script_name STRING
);