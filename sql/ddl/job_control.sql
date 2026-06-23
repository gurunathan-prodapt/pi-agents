-- DDL for job_control table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
CREATE OR REPLACE TABLE project.dataset.job_control (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for a job run, references job_log.job_id"),
    parameter_name STRING NOT NULL OPTIONS(description="Name of the control parameter (e.g., 'Stichtag')"),
    parameter_value STRING OPTIONS(description="Value of the control parameter"),
    description STRING OPTIONS(description="Description of the parameter"),
    valid_from DATE OPTIONS(description="Date from which this parameter is valid"),
    valid_to DATE OPTIONS(description="Date until which this parameter is valid")
)
OPTIONS(
    description="Table to store job-specific control parameters"
);