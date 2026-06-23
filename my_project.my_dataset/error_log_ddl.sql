-- DDL for error_log table
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh

CREATE TABLE IF NOT EXISTS my_project.my_dataset.error_log (
    job_name STRING NOT NULL,
    error_time TIMESTAMP NOT NULL,
    error_message STRING NOT NULL,
    error_code STRING,
    stack_trace STRING,
    run_id STRING
);