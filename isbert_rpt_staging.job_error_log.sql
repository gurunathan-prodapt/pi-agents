-- BigQuery DDL for the job_error_log table
-- Used for logging errors, replacing shell-based error messages for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh
CREATE TABLE IF NOT EXISTS `isbert_rpt_staging.job_error_log`
(
    job_kennung     STRING,
    eintragsnr      STRING,
    error_timestamp TIMESTAMP,
    error_message   STRING,
    sql_state       STRING,
    stack_trace     STRING
);