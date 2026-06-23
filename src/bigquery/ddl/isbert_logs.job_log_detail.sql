-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh

CREATE SCHEMA IF NOT EXISTS isbert_logs;

CREATE TABLE IF NOT EXISTS isbert_logs.job_log_detail (
    job_kennung STRING NOT NULL,
    entry_number INT64 NOT NULL,
    log_timestamp TIMESTAMP,
    log_level STRING, -- e.g., 'INFO', 'WARN', 'ERROR'
    message STRING,
    run_id STRING
);