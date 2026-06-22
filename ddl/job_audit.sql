-- Target BigQuery DDL for job_audit table
-- Legacy Source: Audit logging from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bcp_iccid.ksh

CREATE TABLE IF NOT EXISTS `my_project.my_dataset.job_audit` (
    job_id STRING OPTIONS(description="Legacy job identifier, e.g., r_ausd_bp_ta_bcp_iccid"),
    run_id STRING OPTIONS(description="Unique identifier for a specific job execution"),
    start_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job execution started"),
    end_timestamp TIMESTAMP OPTIONS(description="Timestamp when the job execution ended"),
    status STRING OPTIONS(description="Overall status of the job execution (e.g., 'RUNNING', 'OK', 'ERROR')"),
    stichtag DATE OPTIONS(description="Cutoff date for the data snapshot"),
    wiederanlauf_wert INT64 OPTIONS(description="Restart value for the job"),
    message STRING OPTIONS(description="Summary message for the job execution status")
);