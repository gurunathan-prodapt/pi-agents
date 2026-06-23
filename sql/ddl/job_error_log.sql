-- DDL for job_error_log table
-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_v_ta_action_assoc.ksh
CREATE OR REPLACE TABLE project.dataset.job_error_log (
    error_id INT64 NOT NULL OPTIONS(description="Auto-incrementing unique identifier for each error entry"),
    job_id STRING NOT NULL OPTIONS(description="Unique identifier for a job run, references job_log.job_id"),
    timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the error occurred"),
    error_code STRING OPTIONS(description="Specific error code if available"),
    error_message STRING NOT NULL OPTIONS(description="Detailed error message"),
    error_stack_trace STRING OPTIONS(description="Stack trace or additional error context"),
    severity STRING OPTIONS(description="Severity of the error (e.g., 'HIGH', 'MEDIUM', 'LOW')"),
    source_procedure STRING OPTIONS(description="The procedure or function where the error originated")
)
OPTIONS(
    description="Table to record detailed error information for job runs"
);