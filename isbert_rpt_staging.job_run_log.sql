-- BigQuery DDL for the job_run_log table
-- Used for logging job runs, replacing shell-based logging for job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_acc_ref.ksh
CREATE TABLE IF NOT EXISTS `isbert_rpt_staging.job_run_log`
(
    job_kennung         STRING,
    eintragsnr          STRING,
    start_timestamp     TIMESTAMP,
    end_timestamp       TIMESTAMP,
    status              STRING, -- e.g., 'SUCCESS', 'FAILED', 'RUNNING'
    processed_records   INT64,
    log_message         STRING
);