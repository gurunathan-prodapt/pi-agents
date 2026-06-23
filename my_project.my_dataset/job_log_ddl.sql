-- DDL for job_log table
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/k_ausd_bp_ta_rn_vertrag.ksh

CREATE TABLE IF NOT EXISTS my_project.my_dataset.job_log (
    job_name STRING NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    status STRING NOT NULL,
    records_processed INT64,
    message STRING,
    run_id STRING
);