-- DDL for job_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
CREATE OR REPLACE TABLE project.dataset.job_log (
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for a job run"),
    job_name STRING NOT NULL OPTIONS(description="Name of the job"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job ended"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'OK', 'ERROR')"),
    version STRING OPTIONS(description="Version of the job or script"),
    parameters_json JSON OPTIONS(description="Input parameters for the job in JSON format")
)
OPTIONS(
    description="Main log table for job entry records, status, and timestamps"
);