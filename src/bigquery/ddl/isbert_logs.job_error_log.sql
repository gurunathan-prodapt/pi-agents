-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh

CREATE SCHEMA IF NOT EXISTS isbert_logs;

CREATE TABLE IF NOT EXISTS isbert_logs.job_error_log (
    job_kennung STRING NOT NULL,
    entry_number INT64 NOT NULL,
    error_timestamp TIMESTAMP,
    error_code STRING,
    error_message STRING,
    program_name STRING,
    line_number INT64,
    stack_trace STRING,
    run_id STRING
);