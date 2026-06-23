-- DDL for job_log table
-- Replaces logging logic from vobs/dw_source/isrpt/isbert/SQL/aktuell/aufbereitung/bin/r_ausd_bp_ta_bpr_optionen.ksh
CREATE TABLE IF NOT EXISTS `project.dataset.job_log` (
    job_nr INT64 NOT NULL OPTIONS(description="Job entry number, linking to job_control"),
    job_kennung STRING NOT NULL OPTIONS(description="Job identifier, linking to job_control"),
    log_level STRING NOT NULL OPTIONS(description="Level of the log message (e.g., INFO, WARN, ERROR)"),
    message STRING NOT NULL OPTIONS(description="Log message content"),
    created_at TIMESTAMP NOT NULL OPTIONS(description="Timestamp when the log message was created")
);