-- DDL for job_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_instance.ksh

CREATE TABLE IF NOT EXISTS project.dataset.job_log (
    job_run_id STRING NOT NULL OPTIONS(description="Unique identifier for a specific job execution"),
    job_name STRING NOT NULL OPTIONS(description="Name of the BigQuery stored procedure or job"),
    program_name STRING OPTIONS(description="Name of the program/script (e.g., 'r_ausd_bp_ta_bpr_instance')"),
    program_version STRING OPTIONS(description="Version of the program/script"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'STARTED', 'SUCCEEDED', 'FAILED')"),
    start_time TIMESTAMP OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    stichtag DATE OPTIONS(description="Processing date for the job"),
    wiederanlaufwert INT64 OPTIONS(description="Restart value parameter"),
    log_message STRING OPTIONS(description="General log message or status update"),
    error_details STRING OPTIONS(description="Detailed error message if the job failed"),
    parameters_json JSON OPTIONS(description="JSON representation of all input parameters")
);