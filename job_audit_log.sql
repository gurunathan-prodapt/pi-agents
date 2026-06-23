-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_vvl_upgrade.ksh

CREATE TABLE IF NOT EXISTS `PROJECT_ID.DATASET_ID.job_audit_log` (
    job_name STRING NOT NULL,
    job_version STRING,
    job_entry_no INT64 NOT NULL,
    log_file_name STRING,
    event_type STRING,
    error_no INT64,
    error_arg STRING,
    event_message STRING,
    stichtag STRING,
    stichtag_format STRING,
    event_ts TIMESTAMP NOT NULL
)
OPTIONS(
    description="Logs of job executions, including start, end, and error events."
);