--
-- Target: BigQuery DDL for job_log_audit table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
--

CREATE TABLE IF NOT EXISTS `project.dataset.job_log_audit` (
    entry_nr INT64,
    job_name STRING,
    script_name STRING,
    log_name STRING,
    status STRING,
    stichtag STRING,
    restart_value INT64,
    message STRING,
    created_at TIMESTAMP
);