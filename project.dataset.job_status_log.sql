--
-- BigQuery DDL for job_status_log table
-- Replaces job status tracking functionality from legacy job vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_iccid_einzeln.ksh
--
CREATE TABLE IF NOT EXISTS project.dataset.job_status_log (
    run_id STRING NOT NULL OPTIONS(description="Foreign key to job_run_log, linking to the specific job execution."),
    status_timestamp TIMESTAMP NOT NULL OPTIONS(description="Timestamp when this status update occurred."),
    status_message STRING NOT NULL OPTIONS(description="Brief message describing the job's status at this point (e.g., 'Job started', 'Parameters parsed', 'Kernel script called')."),
    detail STRING OPTIONS(description="Additional detail for the status message.")
);