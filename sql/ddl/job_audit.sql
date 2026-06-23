-- DDL for job_audit table
-- Replaces logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_opt_text.ksh
-- and associated DWMSG functions.

CREATE TABLE IF NOT EXISTS `project.dataset.job_audit` (
    job_name STRING NOT NULL OPTIONS(description="Name of the job or stored procedure executed"),
    start_time TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job started"),
    end_time TIMESTAMP OPTIONS(description="Timestamp when the job finished"),
    status STRING NOT NULL OPTIONS(description="Overall status of the job (e.g., 'SUCCESS', 'FAILED', 'RUNNING')"),
    message STRING OPTIONS(description="Detailed message about job status or errors"),
    stichtag DATE OPTIONS(description="The cutoff date parameter used for the job"),
    wiederanlaufwert INT64 OPTIONS(description="The restart value parameter used for the job")
)
OPTIONS(
    description="Audit table for tracking job executions within the BigQuery environment."
);