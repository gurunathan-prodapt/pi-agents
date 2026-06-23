-- BigQuery DDL for job error log table
-- Original job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_v_ta_barrier_zusgf.ksh
CREATE TABLE IF NOT EXISTS `your_project_id.your_dataset_id.job_error_log` (
    log_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    job_kennung STRING,
    eintrags_nr STRING,
    error_code INT64,
    error_message STRING,
    severity STRING -- e.g., 'ERROR', 'WARNING'
);