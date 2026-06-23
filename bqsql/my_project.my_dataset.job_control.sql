-- Migrated from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_notice.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_control` (
    job_kennung STRING,
    entry_nr INT64,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    status STRING, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED'
    last_modified TIMESTAMP,
    pid STRING,
    hostname STRING,
    message STRING,
    record_count INT64
);