-- Legacy Source: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
-- Job: vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit_log` (
    job_name STRING NOT NULL OPTIONS(description="Name of the job/stored procedure being executed."),
    run_id STRING NOT NULL OPTIONS(description="Unique identifier for a specific execution run of the job."),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job execution started."),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job execution ended."),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., 'RUNNING', 'SUCCEEDED', 'FAILED')."),
    message STRING OPTIONS(description="Detailed message or error description."),
    parameters_json JSON OPTIONS(description="JSON representation of input parameters for the job run.")
)
OPTIONS(
    description="Audit log table for tracking job executions in BigQuery."
);