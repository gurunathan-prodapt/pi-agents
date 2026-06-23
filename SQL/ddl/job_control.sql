-- DDL for job_control table
-- Replaces job tracking logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_control` (
    job_nr INT64 NOT NULL OPTIONS(description="Unique job entry number"),
    job_kennung STRING NOT NULL OPTIONS(description="Unique identifier for a job run"),
    script_name STRING NOT NULL OPTIONS(description="Name of the script/procedure being executed"),
    log_file STRING OPTIONS(description="Reference to the log file (or log ID in BigQuery)"),
    stichtag_info STRING OPTIONS(description="Cutoff date information for the job"),
    status STRING NOT NULL OPTIONS(description="Current status of the job (e.g., RUNNING, OK, FAILED)"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the job record was created"),
    finished_at TIMESTAMP OPTIONS(description="Timestamp when the job completed or failed")
);