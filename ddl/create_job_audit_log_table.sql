-- Legacy source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_evn.ksh

CREATE TABLE IF NOT EXISTS `your_gcp_project.your_bigquery_dataset.job_audit_log` (
    job_name STRING,
    job_entry_nr INT64,
    error_nr INT64,
    error_arg STRING,
    log_ts TIMESTAMP,
    message STRING,
    stichtag DATE,
    sysdate_ddmmyyyy STRING,
    restart_value INT64,
    status STRING
);