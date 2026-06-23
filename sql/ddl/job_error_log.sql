--
-- BigQuery DDL for job_error_log table
-- Replaces error logging functionality from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_cntrct_dist.ksh
--
CREATE TABLE IF NOT EXISTS `project.dataset.job_error_log` (
    error_id STRING NOT NULL OPTIONS(description="Unique identifier for each error entry (UUID)."),
    job_kennung STRING OPTIONS(description="Identifier for the job/stored procedure."),
    err_nr INT64 OPTIONS(description="Error number or code."),
    err_arg STRING OPTIONS(description="Additional argument or context for the error."),
    created_at TIMESTAMP OPTIONS(description="Timestamp when the error was logged."),
    message STRING OPTIONS(description="Detailed error message.")
);