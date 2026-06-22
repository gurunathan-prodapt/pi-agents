-- DDL for job_audit table
-- Replaces logging to file in vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_apn.ksh
CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_audit` (
    job_entry_number INT64 NOT NULL OPTIONS(description="Unique identifier for each job run"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job being executed"),
    script_name STRING OPTIONS(description="Original script name or stored procedure name"),
    start_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING OPTIONS(description="Status of the job (e.g., 'RUNNING', 'SUCCESS', 'FAILED')"),
    message STRING OPTIONS(description="Detailed message or error information"),
    stichtag DATE OPTIONS(description="Reference date parameter (DDMMYYYY)"),
    wiederanlaufwert INT64 OPTIONS(description="Restart value parameter"),
    sysdate_at_run DATE OPTIONS(description="System date at the time of job execution"),
    log_file_name STRING OPTIONS(description="Original log file name for context (not directly used in BQ)"),
    PRIMARY KEY (job_entry_number) NOT ENFORCED
);