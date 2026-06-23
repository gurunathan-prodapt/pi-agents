-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Target: BigQuery DDL for job_error_log table
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    job_name STRING,
    error_number INT64,
    error_argument STRING,
    created_at TIMESTAMP,
    message STRING
);