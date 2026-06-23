-- BigQuery DDL for the job_audit_log table
-- Used for logging job execution and status
-- Legacy Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_da_vda_tk.ksh

CREATE TABLE IF NOT EXISTS my_project.my_dataset.job_audit_log
(
    job_run_id STRING NOT NULL,
    job_name STRING NOT NULL,
    start_timestamp TIMESTAMP,
    end_timestamp TIMESTAMP,
    status STRING, -- e.g., 'RUNNING', 'SUCCESS', 'FAILED'
    message STRING,
    parameter_stichtag STRING,
    parameter_wiederanlaufwert STRING,
    creation_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);