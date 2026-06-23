-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_acc_ref.ksh

CREATE SCHEMA IF NOT EXISTS isbert_logs;

CREATE TABLE IF NOT EXISTS isbert_logs.job_log (
    job_kennung STRING NOT NULL,
    entry_number INT64 NOT NULL,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    status STRING, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED'
    message STRING,
    stichtag_info STRING,
    job_name STRING,
    program_version STRING,
    user_name STRING,
    host_name STRING,
    log_file_path STRING, -- In BigQuery, this might just be a reference or become redundant
    run_id STRING
);