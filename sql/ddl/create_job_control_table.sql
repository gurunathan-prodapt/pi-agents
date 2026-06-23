-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_rn_einzeln.ksh
-- Description: DDL for job control table, tracking job executions and status.

CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for each job run, typically a UUID."),
    job_name STRING NOT NULL OPTIONS(description="Name of the job being executed, e.g., 'ausd_bp_ta_rn_einzeln'"),
    start_time TIMESTAMP OPTIONS(description="Timestamp when the job started."),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended."),
    status STRING OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'SUCCESS', 'FAILED')."),
    p_stichtag STRING OPTIONS(description="Input parameter for the Stichtag (cutoff date) as DDMMYYYY string."),
    p_wiederanlaufwert INT64 OPTIONS(description="Input parameter for the restart value."),
    effective_stichtag STRING OPTIONS(description="The resolved Stichtag used by the job after defaulting, as DDMMYYYY string."),
    effective_wiederanlaufwert INT64 OPTIONS(description="The resolved Wiederanlaufwert used by the job after defaulting."),
    parameters_json JSON OPTIONS(description="JSON representation of all input parameters and resolved values."),
    error_message STRING OPTIONS(description="Detailed error message if the job failed."),
    stack_trace STRING OPTIONS(description="Stack trace or detailed error context if the job failed.")
)
OPTIONS(
    description="Table to control and track the execution of BigQuery stored procedures."
);