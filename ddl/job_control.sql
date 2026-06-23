-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Target: BigQuery DDL for job_control table
CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    job_entry_nr INT64,
    job_name STRING,
    source_script STRING,
    log_name STRING,
    stichtag STRING,
    sysdate_ddmmyyyy STRING,
    restart_value INT64,
    status STRING,
    created_at TIMESTAMP,
    finished_at TIMESTAMP,
    success_message STRING,
    error_message STRING
);